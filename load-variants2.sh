#!/usr/bin/env bash

## file: load-variants.sh
## desc: Loads variant metadata into the GeneWeaver DB. Designed to load things
##       as quickly as possible--this script will remove indexes and table constraints
##       prior to loading.
## auth: TR

## Load the configuration file
source './config.sh'

## Load the secrets file which contains credentials
source './secrets.sh'

usage() {

    echo ""
	echo "usage: $0 [options] <genome-build> <variants> "
    echo ""
    echo "Load variant metadata into GeneWeaver." 
    echo ""
    echo "Processing options:"
    echo "  -s, --size  size of each subfile (default = 15GB)"
    echo ""
    echo "Misc. options:"
    echo "  -h, --help  print this help message and exit"
    echo ""
}

## Default options if the user doesn't provide any
split_size='10GB'

while :; do
    case $1 in

        -s | --size)
            if [ "$2"]; then
                split_size="$2"
                shift
            else
                echo "ERROR: --size requires an argument"
                echo "       e.g. '2GB', '1024MB', etc."
                exit 1
            fi
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

## Check if the secrets were loaded
if [[ -z "$db_name" || -z "$db_user" || -z "$db_pass" ]];

    echo "ERROR: There was a problem loading the secrets file."
    echo "       DB credentials are missing."
    exit 1
fi

## If the <genome-build> or <variants> arguments are missing, then print the usage 
## and exit
if [[ $# -lt 2 ]]; then

    echo "ERROR: You need to provide the <genome-build> and <variants> arguments"
    echo "       to the script."
    echo ""
    usage
	exit 1
fi

connect="host=$db_host dbname=$db_name user=$db_user password=$db_pass"

build="$1"
variants="$2"

## Checks to see if the genome build we're using exists
spid="SELECT sp_id FROM odestatic.genome_build WHERE gb_ref_id = '$build';"
## Query and remove whitespace
spid=$(psql "$connect" -t -c "$spid" | sed -e 's/\s//g')

## If no sp_id was returned, the genome build doesn't exist
if [[ -z "$spid" || $? -ne 0 ]]; then

    echo "ERROR: There was a problem retrieving the species for the build you provided."
    echo "       That genome build probably doesn't exist."
    exit 1
fi

## Query to get the gdb_id for the Variant gene type
vartype="SELECT gdb_id FROM odestatic.genedb WHERE gdb_name = 'Variant';"
## Query and remove whitespace
vartype=$(psql "$connect" -t -c "$vartype" | sed -e 's/\s//g')

## If the variant gene type is missing, try to create it
if [[ -z "$vartype" || $? -ne 0 ]]; then

    echo "ERROR: The variant gene type is missing from genedb."
    echo "       This script will now attempt to create the variant gene type."

    read -r -d '' varinsert <<-EOF
        INSERT INTO odestatic.genedb 
                    (gdb_name, sp_id, gdb_shortname, gdb_date)
        VALUES      ('Variant', 0, 'variant', NOW())
        RETURNING   gdb_id;
	EOF

    ## Query and remove whitespace
    vartype=$(psql "$connect" -t -c "$varinsert" | sed -e 's/\s//g')

    if [[ -z "$vartype" || $? -ne 0 ]]; then

        echo "ERROR: Couldn't create the variant type."
        exit 1
    fi
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

    ## Remove the 'rs' prefix from the rsID since we store these as 64bit integers
    $rsid = substr($rsid, 2, -1);

    ## Last, reformat the effects column to only include effect terms
    effects = splitnvx($effects, ";");
    map elist = {};

    # Loop through each effect
    for (k, v in effects) {

        # Match on the effect
        v =~ "effect=([_a-z]+),";

        # Extract the effect
        effect = "\1";

        if (effect == "0") { effect = "intergenic"; }

        elist[effect] = effect;
    }

    $effects = joink(elist, ",");
EOF

log "Splitting variants for parallel processing"

## Substutite the '!BUILD' string for the actual genome build
process_variant_file="${process_variant_file/!BUILD/$build}"

## Variant file locations that are accessible by the DB
tmp_variants=$(mktemp)
tmp_splits=$(mktemp -u)

## Remove the header and split into evenly sized files for parallel processing
mlr --tsv --headerless-csv-output cat "$variants" | 
split -C "$split_size" -a 3 -d - "$tmp_splits"

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
    ) &
done

wait

log "Merging formatted variants"

mlr --tsvlite cat "$tmp_splits"*formatted > "$tmp_variants"

rm "$tmp_splits"*
chmod 777 "$tmp_variants"

## Query to load the variants into the staging table
copy_variants="COPY variant_staging FROM '$tmp_variants' WITH CSV HEADER NULL 'NULL' DELIMITER E'\t';"

## Query to create the staging table for loading annotations
read -r -d '' create_stage <<- EOF
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

read -r -d '' insert_variant_info <<- EOF

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

read -r -d '' insert_variants <<- EOF
    
    INSERT INTO extsrc.variant (

        var_ref_id,
        vt_id,
        var_ref_cur,
        var_obs_alleles,
        var_ma,
        var_maf,
        var_clinsig,
        vri_id

    ) SELECT   rsid, 
               (
                    SELECT      COALESCE(array_agg(distinct vt_id), '{1}') AS effects
                    FROM        variant_type AS vt 
                    INNER JOIN  regexp_split_to_table(vs.effect, ',') AS effect 
                    ON          vt.vt_effect = effect
               ),
               true,
               observed,
               ma,
               maf,
               clinsig,
               vi.vri_id
    FROM       variant_staging vs
               -- (
               --      SELECT      array_agg(distinct vt_id) AS effects
               --      FROM        variant_type AS vt 
               --      INNER JOIN  regexp_split_to_table(vs.effect, ',') AS effect 
               --      ON          vt.vt_effect = effect
               -- ) vt_ids
                    
    INNER JOIN extsrc.variant_info vi
    ON         vs.chromosome = vi.vri_chromosome AND 
               vs.position = vi.vri_position
    -- INNER JOIN (
    --                 SELECT      array_agg(distinct vt_id) AS effects
    --                 FROM        variant_type AS vt 
    --                 INNER JOIN  regexp_split_to_table(vs.effect, ',') AS effect 
    --                 ON          vt.vt_effect = effect
    --            ) vt_ids
    -- ON         
    WHERE      vi.gb_id = (
                    SELECT  gb_id 
                    FROM    odestatic.genome_build 
                    WHERE   gb_ref_id = vs.build
               );
EOF

read -r -d '' create_variants <<EOF
    BEGIN TRANSACTION;

    ALTER TABLE extsrc.variant DISABLE TRIGGER ALL;
    ALTER TABLE extsrc.variant_info DISABLE TRIGGER ALL;
    ALTER TABLE odestatic.variant_type DISABLE TRIGGER ALL;

    DROP INDEX IF EXISTS extsrc.variant_var_id_uindex;
    DROP INDEX IF EXISTS extsrc.variant_var_ref_id_index;
    DROP INDEX IF EXISTS extsrc.variant_vri_id_index;
    DROP INDEX IF EXISTS extsrc.variant_info_vri_id_uindex;
    DROP INDEX IF EXISTS extsrc.variant_info_vri_chromosome_vri_position_index;

    $create_stage

    $copy_variants

    $insert_variant_info

    CREATE INDEX stage_chromosome_position_index 
    ON           variant_staging (chromosome, position);

    CREATE INDEX variant_info_vri_chromosome_vri_position_index 
    ON           extsrc.variant_info (vri_chromosome, vri_position);

    ANALYZE variant_staging;
    ANALYZE extsrc.variant_info;

    $insert_variants

    ALTER TABLE extsrc.variant ENABLE TRIGGER ALL;
    ALTER TABLE extsrc.variant_info ENABLE TRIGGER ALL;
    ALTER TABLE odestatic.variant_type ENABLE TRIGGER ALL;

    END TRANSACTION;
EOF

log "Inserting variants into the database"

result=$(psql "$connect" -c "$create_variants")

## Clean up our mess
rm "$tmp_variants"

if [[ $? -ne 0 ]]; then
    echo "There was an error while populating the DB with variants"
    exit 1
fi

log "Recreating indexes and vacuuming"

## Recreate the indexes
(psql "$connect" -c "CREATE UNIQUE INDEX variant_info_vri_id_uindex ON extsrc.variant_info (vri_id);") &
(psql "$connect" -c "CREATE INDEX variant_info_vri_index ON extsrc.variant_info (vri_id);") &
(psql "$connect" -c "CREATE UNIQUE INDEX variant_var_id_uindex ON extsrc.variant (var_id);") &
(psql "$connect" -c "CREATE INDEX variant_var_ref_id_index ON extsrc.variant (var_ref_id);") &
(psql "$connect" -c "CREATE INDEX variant_vri_id_index ON extsrc.variant (vri_id);") &
(psql "$connect" -c "CREATE INDEX variant_info_vri_chromosome_vri_position_index ON extsrc.variant_info (vri_chromosome, vri_position);") &

wait

(psql "$connect" -c "VACUUM ANALYZE extsrc.variant;") &
(psql "$connect" -c "VACUUM ANALYZE extsrc.variant_info;") &
(psql "$connect" -c "VACUUM ANALYZE odestatic.variant_type;") &

wait
