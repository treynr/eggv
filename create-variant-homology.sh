#!/usr/bin/env bash

## file: create-variant-odes.sh
## desc: Small helper shell script to create ode_gene_ids for variants. The SQL
##       queries and workflow for this are based off the loadvariants.py 
##       script written by Erich. This should be a little faster since we do 
##       scary things like dropping indexes and removing constraints. This
##       script loads variant annotations into a temporary staging table, 
##       then uses several joins to determine ode_gene_ids for genes and 
##       variants, and finally stores the results in the homology table.
## auth: TR
#

source './config.sh'

usage() {

    echo ""
	echo "usage: $0 [options] <db-name> <db-user> <genome-build> <annotations>"
	echo "usage: $0 [options] -c <genome-build> <data-file> "
    echo "" 
    echo "Generates and inserts variant-geen associations into the GeneWeaver database."
    echo "Variant annotations are processed, inserted into a temporary staging table,"
    echo "joined with the GW IDs, and finally added to the homology table."
    echo ""
    echo "Config options:"
    echo "  -c, --config  get <db-name> and <db-user> from the GW config file"
    echo ""
    echo "Misc. options:"
    echo "  -s, --source  specify a hom_source_name for the new mappings (default = Variant)"
    echo "  -h, --help    print this help message and exit"
    echo ""
	exit
}

hom_source_name="Variant"

## cmd line processing
while :; do
    case $1 in
        -c | --config)
            config=1
            ;;

        -s | --source)
            if [ "$2" ]; then
                hom_source_name="$2"
                shift
            else
                echo "ERROR: --source requires an argument"
                echo ""
                usage
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

## Check to see if we can grab database, user, and password info from the
## config file used for the GW python library
if [[ -n "$config"  && -f "gwlib.cfg" ]]; then

    dbname=$(sed -n -r -e 's/database\s*=\s*(.+)/\1/p' gwlib.cfg)
    dbuser=$(sed -n -r -e 's/user\s*=\s*(.+)/\1/p' gwlib.cfg)
    password=$(sed -n -r -e 's/password\s*=\s*(.+)/\1/p' gwlib.cfg)

    set -- "$dbname" "$dbuser" "$1" "$2"
fi

## Print usage iff 1) the user has provided less than four arguments (DB, user, genome
## build, and annotation filepath) and hasn't used the --config option, 2) the genome
## build and annotation filepath arguments are missing.
if [[ ($# -lt 4 && -z "$config") || (-z "$3" || -z "$4") ]]; then

    usage
	exit 1
fi

dbname="$1"
dbuser="$2"
build="$3"
annotations="$4"

if [ -z "$password" ]; then
    read -s -p "DB password: " password
fi

## Connection string for psql
connect="host=localhost dbname=$dbname user=$dbuser password=$password"

## SQL queries:

## Query to get the sp_id for the genome build we're updating
q_spid="SELECT sp_id FROM odestatic.genome_build WHERE gb_ref_id = '$build';"

## Query to get the gdb_id for the Variant gene type
q_vartype="SELECT gdb_id FROM odestatic.genedb WHERE gdb_name = 'Variant';"

## Query to generate an incremental sequence for the homology ID
q_create_sequence="CREATE TEMP SEQUENCE hom_id_sequence INCREMENT BY 1;"

## Query to set the homology ID sequence to the maximum homology ID
q_set_sequence="SELECT setval('hom_id_sequence', (SELECT MAX(hom_id) FROM homology));"

## Query to create the staging table for loading annotations
read -r -d '' q_create_stage <<- EOF
    CREATE TEMP TABLE annotation_staging (
        rsid BIGINT,
        ensembl VARCHAR,
        effect VARCHAR
    );
EOF

## Query the species for the given genome build, remove whitespace from the result
spid=$(psql "$connect" -t -c "$q_spid" | sed -e 's/\s//g')

## If no species ID was returned or psql encountered an error then exit
if [[ -z "$spid" || $? -ne 0 ]]; then

    log "There was an error retrieving the species for the build you provided."
    log "That genome build probably doesn't exist."
    log "Exiting..."

    exit 1
fi

## Query the variant gdb_id from the gene table, remove whitespace from the result
vartype=$(psql "$connect" -t -c "$q_vartype" | sed -e 's/\s//g')

## If no variant type ID was returned or psql encountered an error then exit
if [[ -z "$vartype" || $? -ne 0 ]]; then

    log "There was an error retrieving the variant gene type."
    log "The variant gene type is probably missing from genedb."
    log "Exiting..."

    exit 1
fi

## Absolute path to the annotations in a place the postgres server can access
tmp_annos=$(mktemp)
## Temp path to use for final annotation processing prior to insertion
tmp_splits=$(mktemp -u)

log "Splitting annotations to process concurrently"

## Remove the header which will be added back later, split the annotations into
## separate 2GB files that can be processed in parallel
tail -n +2 "$annotations" | split -C 2GB -a 2 -d - "$tmp_splits"

log "Processing and formatting annotations"

for vs in "$tmp_splits"??
do
    out="$vs.formatted"
    (
        ## Add the header back
        mlr --implicit-csv-header --tsvlite label 'rsid,ensembl,biotype' "$vs" |
        ## Filter out things without an rsID
        mlr --tsvlite filter '$rsid =~ "^rs"' |
        ## Remove the rs prefix since we store these IDs as integers
        mlr --tsvlite put '$rsid = substr($rsid, 2, -1)' > "$out"

        ## Delete the split file
        rm "$vs"
    ) &
done

wait

log "Finalizing formatted annotations"

mlr --tsvlite cat "$tmp_splits"*formatted > "$tmp_annos"

## Remove processed files
rm "$tmp_splits"*

## Make sure the postgres server has permission to access this file
chmod 777 "$tmp_annos"

## Query to load the annotations into the staging table
q_copy_annotations="COPY annotation_staging FROM '$tmp_annos' WITH CSV HEADER DELIMITER E'\t';"

## Query to create the gene-variant associations
read -r -d '' q_insert_homology <<- EOF

    INSERT INTO extsrc.homology (

        hom_id, hom_source_id, hom_source_name, ode_gene_id, sp_id, hom_date
        
    -- The distinct on ensures gene-variant associations only appear once
    --
    ) SELECT   DISTINCT ON (gene_ode, var_id) 
               nextval('hom_id_sequence'),
               gv.ode_gene_id AS variant_ode,
               '$hom_source_name',
               g.ode_gene_id AS gene_ode, 
               $spid,
               NOW()
    FROM       annotation_staging st 
    --
    ---- Join onto variant to get GW var_ids for each rsID in the staging table
    --
    INNER JOIN extsrc.variant v 
    ON         st.rsid = v.var_ref_id 
    --
    ---- Join onto the gene table to get ode_gene_ids for the gene the
    ---- variant is found in.
    --
    INNER JOIN exstrc.gene g
    ON         st.ensembl = g.ode_ref_id
    --
    ---- Finally we join onto the gene table once more to retrieve ode_gene_ids
    ---- for the rsIDs in the staging table using the var_ids we retrieved in the
    ---- first join. We convert var_id to varchar instead of converting
    ---- ode_ref_id to a bigint to take advantage of the gene table's index.
    --
    INNER JOIN  extsrc.gene gv 
    ON          v.var_id :: VARCHAR = gv.ode_ref_id 
    WHERE       gv.sp_id = $spid AND 
                gv.gdb_id = $vartype AND
                g.gdb_id = (
                    SELECT gdb_id 
                    FROM   odestatic.genedb 
                    WHERE  gdb_name = 'Ensembl Gene'
                ) AND
                g.sp_id = $spid;
EOF

## Query to generate the variant-gene associations
read -r -d '' q_create_variant_mapping <<EOF
    BEGIN TRANSACTION;
    DROP INDEX IF EXISTS extsrc.homology_ode_gene_id_idx;
    DROP INDEX IF EXISTS extsrc.homology_hom_id_idx;
    DROP INDEX IF EXISTS extsrc.homology_hom_source_id_idx;
    DROP INDEX IF EXISTS extsrc.homology_hom_source_name_idx;
    $q_create_sequence
    $q_set_sequence
    $q_create_stage
    $q_copy_annotations
    CREATE INDEX ensembl_stage_index ON annotation_staging (ensembl);
    ANALYZE annotation_staging;
    $q_insert_homology
    END TRANSACTION;
EOF

log "Inserting variant-gene annotations into the database"

result=$(psql "$connect" -c "$q_create_variant_mapping")

## Check the psql exit code for an error
if [[ $? -ne 0 ]]; then
    echo "ERROR: psql returned a non-zero exit code. Looks like there were errors"
    echo "       creating the variant-gene homology relationships."
    rm "$tmp_annos"
    exit 1
fi

log "Cleaning up workspace"

rm "$tmp_annos"

log "Recreating indexes"

## Recreate indexes
(psql "$connect" -c "CREATE INDEX homology_ode_gene_id_idx ON extsrc.homology (ode_gene_id);") &
(psql "$connect" -c "CREATE INDEX homology_hom_id_idx ON extsrc.homology (hom_id);") &
(psql "$connect" -c "CREATE INDEX homology_hom_source_id_idx ON extsrc.homology (hom_source_id);") &
(psql "$connect" -c "CREATE INDEX homology_hom_source_name_idx ON extsrc.homology (hom_source_name);") &

wait

log "Vacuuming and analyzing the homology table"

psql "$connect" -c "VACUUM ANALYZE extsrc.homology;"

