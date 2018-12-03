#!/usr/bin/env bash

## file: create-variant-odes.sh
## desc: Small helper shell script to create ode_gene_ids for variants. The SQL
##       queries and workflow for this are based off the loadvariants.py 
##       script written by Erich. This should be a little faster since we do 
##       scary things like dropping indexes and removing constraints. This
##       script loads variant annotations into a temporary staging table, 
##       then uses several joins to determine ode_gene_ids for genes and 
##       variants, and stores the results in the homology table.
## vers: 0.1.12
## auth: TR
#

## Load the config file
source './config.sh'

usage() {

    echo ""
    echo "usage: $0 [options] <db-name> <db-user> <genome-build> <variants>"
    echo "usage: $0 [options] -c <genome-build> <variants> "
    echo ""
    echo "Loads variant metadata into the GeneWeaver database."
    echo "Variant annotations are processed, inserted into a temporary staging table,"
    echo "joined with the GW IDs, and finally added to the homology table."
    echo ""
    echo "Config options:"
    echo "  -c, --config  get <db-name> and <db-user> from the GW config file"
    echo ""
    echo "Misc. options:"
    echo "  -h, --help    print this help message and exit"
    echo ""
    exit
}

while :; do
    case $1 in

        -c | --config)
            config=1
            ;;

        -h | -\? | --help)
            usage
            exit
            ;;

        --)
            shift
            break
            ;;

        -?*)
            echo "WARN: unknown option (ignored): $1" >&2
            ;;

        *)
            break
    esac

    shift
done

## Check to see if we can grab database, user, and password info from the
## config file used for the GW python library
if [[ -n "$config"  && -f "gwlib.cfg" ]]; then

    dbname=$(sed -n -r -e 's/database\s*=\s*(.+)/\1/p' gwlib.cfg)
    dbuser=$(sed -n -r -e 's/user\s*=\s*(.+)/\1/p' gwlib.cfg)
    password=$(sed -n -r -e 's/password\s*=\s*(.+)/\1/p' gwlib.cfg)

    set -- "$dbname" "$dbuser" "$1" "$2"
fi

## Print usage iff 
##  1) the user has provided less than four arguments (DB, user, genome build, and
##     variant filepath) and hasn't used the --config option.
##  2) the genome build and annotation filepath arguments are missing.
if [[ ($# -lt 4 && -z "$config") || -n "$help" || (-z "$3" || -z "$4") ]]; then

    usage
    exit 1
fi

dbname="$1"
dbuser="$2"
build="$3"
variants="$4"

if [ -z "$password" ]; then
    read -s -p "DB password: " password
fi

connect="host=localhost dbname=$dbname user=$dbuser password=$password"

## SQL queries:

## Query to get the sp_id for the genome build we're updating
q_spid="SELECT sp_id FROM odestatic.genome_build WHERE gb_ref_id = '$build';"

## Query to get the gdb_id for the Variant gene type
q_vartype="SELECT gdb_id FROM odestatic.genedb WHERE gdb_name = 'Variant';"

## Query to create the staging table for loading annotations
read -r -d '' q_create_stage <<- EOF
    CREATE TEMP TABLE variant_staging (

        rsid        BIGINT,
        -- allele      VARCHAR,
        effect      VARCHAR,
        current     BOOL,
        observed    VARCHAR,
        ma          VARCHAR,
        maf         FLOAT,
        clinsig     VARCHAR,
        chromosome  VARCHAR,
        position    INT,
        build       VARCHAR
    );
EOF

## Query to insert genomic coordinates for a variant into the variant_info table
read -r -d '' q_insert_variant_info <<- EOF

    INSERT INTO extsrc.variant_info (vri_chromosome, vri_position, gb_id)
    SELECT      chromosome, 
                position, 
                (
                    SELECT  gb_id 
                    FROM    odestatic.genome_build 
                    WHERE   gb_ref_id = build
                )
    FROM        variant_staging;
EOF

## Query to insert variants and their metadata into the variant table
read -r -d '' q_insert_variants <<- EOF
    
    INSERT INTO extsrc.variant (

        var_ref_id,
        vt_id,
        var_ref_cur,
        var_obs_alleles,
        var_ma,
        var_maf,
        var_clinsig,
        vri_id

    ) SELECT   vs.rsid, 
               --
               ---- A variant can be associated with multiple effects. This splits the
               ---- effects list (from the staging table, separated by ',') into a table
               ---- then joins the table onto variant_effect to get an array of variant
               ---- effect IDs.
               --
               (
                    SELECT      COALESCE(array_agg(distinct vt_id), '{1}') AS effects
                    FROM        odestatic.variant_type AS vt 
                    INNER JOIN  regexp_split_to_table(vs.effect, ',') AS effect 
                    ON          vt.vt_effect = effect
               ),
               true,
               vs.observed,
               vs.ma,
               vs.maf,
               vs.clinsig,
               vi.vri_id
    FROM       variant_staging vs
    INNER JOIN extsrc.variant_info vi
    ON         vs.chromosome = vi.vri_chromosome AND 
               vs.position = vi.vri_position
    WHERE      vi.gb_id = (
                    SELECT  gb_id 
                    FROM    odestatic.genome_build 
                    WHERE   gb_ref_id = vs.build
               );
EOF

## Query and remove whitespace
spid=$(psql "$connect" -t -c "$spid" | sed -e 's/\s//g')

## If no sp_id was returned, the genome build doesn't exist
if [[ -z "$spid" || $? -ne 0 ]]; then
    echo "There was an error retrieving the species for the build you provided"
    echo "That genome build probably doesn't exist"
    echo "Exiting..."

    exit 1
fi

## Query and remove whitespace
vartype=$(psql "$connect" -t -c "$vartype" | sed -e 's/\s//g')

## Can't do anything if the variant type is missing
if [[ -z "$vartype" || $? -ne 0 ]]; then
    echo "The variant gene type is missing from genedb"
    echo "Exiting..."

    exit 1
fi

## Performs most of the steps to process the variant file into a format that
## can be immediately ingested into the database.
read -r -d '' process_variant_file <<-'EOF'

    ## First we add missing columns: 'current' which marks the status of the
    ## rsID as current and not deprecated, 'clinvar' which reports clinical
    ## significance, 'position' which indicates bp position of the variant,
    ## and 'build' which is the variant genome build
    $current = true;
    $clinsig = "unknown";
    $position = $start;
    $build = "!BUILD";
    $ma = gsub($a1, ",", "/");
    $chromosome = $chr;

    ## Next we merge variant and reference allele columns into a single
    ## observed field
    $observed = gsub($a1 . "," . $a2, ",", "/");

    ## Remove the 'rs' prefix from the rsID since we store these as integers
    $rsid = substr($rsid, 2, -1);

    ## Last, reformat the effects column to only include effect terms
    effects = splitnvx($effects, ";");
    map elist = {};

    ## Loop through each effect
    for (k, v in effects) {

        ## Match on the effect
        v =~ "effect=([_a-z]+),";

        ## Extract the effect
        effect = "\1";

        ## Replace null effects with 'intergenic'
        if (effect == "0") { effect = "intergenic"; }

        elist[effect] = effect;
    }

    $effects = joink(elist, ",");
EOF

log "Splitting variants for parallel processing"

## Substutite the '!BUILD' string for the actual genome build
process_variant_file="${process_variant_file/!BUILD/$build}"

## Absolute path to the variant file in a place the postgres server can access 
tmp_variants=$(mktemp)
## Temp path to use for final variant processing prior to insertion
tmp_splits=$(mktemp -u)

## Split file to format in parallel, remove the header which will be added
## back later
tail -n +2 "$variants" | split -C 10GB -a 3 -d - "$tmp_splits"

log "Processing and formatting variants"

for vs in "$tmp_splits"???
do
    out="$vs.formatted"
    (
        ## Preformatting
        mlr --implicit-csv-header --tsvlite label 'rsid,chr,start,end,strand,a1,a2,maf,effects' "$vs" |
        ## Filter out things without an rsID
        mlr --tsvlite filter '$rsid =~ "^rs"' |
        ## Make data changes
        mlr --tsvlite put "$process_variant_file" |
        ## Organize columns into their final positions
        mlr --tsvlite cut -o -f 'rsid,effects,current,observed,ma,maf,clinsig,chromosome,position,build' > "$out"

        ## Delete the split file
        rm "$vs"
    ) &
done

wait

log "Merging formatted variants"

mlr --tsvlite cat "$tmp_splits"*formatted > "$tmp_variants"

## Remove processed files
rm "$tmp_splits"*

## Make sure the postgres server has permission to access this file
chmod 777 "$tmp_variants"

## Query to load the variants into the staging table
q_copy_variants="COPY variant_staging FROM '$tmp_variants' WITH CSV HEADER NULL 'NULL' DELIMITER E'\t';"

## Query to insert the variant metadata into the proper tables
read -r -d '' q_create_variants <<EOF
    BEGIN TRANSACTION;

    ALTER TABLE extsrc.variant         DISABLE TRIGGER ALL;
    ALTER TABLE extsrc.variant_info    DISABLE TRIGGER ALL;
    ALTER TABLE odestatic.variant_type DISABLE TRIGGER ALL;

    DROP INDEX IF EXISTS extsrc.variant_var_id_uindex;
    DROP INDEX IF EXISTS extsrc.variant_var_ref_id_index;
    DROP INDEX IF EXISTS extsrc.variant_vri_id_index;
    DROP INDEX IF EXISTS extsrc.variant_info_vri_id_uindex;
    DROP INDEX IF EXISTS extsrc.variant_info_vri_chromosome_vri_position_index;

    $q_create_stage

    $q_copy_variants

    $q_insert_variant_info

    CREATE INDEX stage_chromosome_position_index 
    ON           variant_staging (chromosome, position);

    CREATE INDEX variant_info_vri_chromosome_vri_position_index 
    ON           extsrc.variant_info (vri_chromosome, vri_position);

    ANALYZE variant_staging;
    ANALYZE extsrc.variant_info;

    $q_insert_variants

    ALTER TABLE extsrc.variant         ENABLE TRIGGER ALL;
    ALTER TABLE extsrc.variant_info    ENABLE TRIGGER ALL;
    ALTER TABLE odestatic.variant_type ENABLE TRIGGER ALL;

    END TRANSACTION;
EOF

log "Inserting variants into the database"

result=$(psql "$connect" -c "$create_variants")

## Check the psql exit code for an error
if [[ $? -ne 0 ]]; then
    echo "ERROR: psql returned a non-zero exit code. Looks like there were errors"
    echo "       creating the inserting the variants"
    rm "$tmp_variants"
    exit 1
fi

log "Cleaning up workspace"

rm "$tmp_variants"

log "Recreating indexes"

## Recreate the indexes
(psql "$connect" -c "CREATE UNIQUE INDEX variant_info_vri_id_uindex ON extsrc.variant_info (vri_id);") &
(psql "$connect" -c "CREATE INDEX variant_info_vri_index ON extsrc.variant_info (vri_id);") &
(psql "$connect" -c "CREATE UNIQUE INDEX variant_var_id_uindex ON extsrc.variant (var_id);") &
(psql "$connect" -c "CREATE INDEX variant_var_ref_id_index ON extsrc.variant (var_ref_id);") &
(psql "$connect" -c "CREATE INDEX variant_vri_id_index ON extsrc.variant (vri_id);") &
(psql "$connect" -c "CREATE INDEX variant_info_vri_chromosome_vri_position_index ON extsrc.variant_info (vri_chromosome, vri_position);") &

wait

log "Vacuuming and analyzing the variant tables"

(psql "$connect" -c "VACUUM ANALYZE extsrc.variant;") &
(psql "$connect" -c "VACUUM ANALYZE extsrc.variant_info;") &
(psql "$connect" -c "VACUUM ANALYZE odestatic.variant_type;") &

wait

