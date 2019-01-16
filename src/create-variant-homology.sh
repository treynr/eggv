#!/usr/bin/env bash

## file: create-variant-odes.sh
## desc: Generate variant-gene assocations and store those associations as part
##       of GeneWeaver's homology model.
## auth: TR

## Load the config
[[ -f './config.sh' ]] && source './config.sh' || source '../config.sh'

## Load the secrets
[[ -f './secrets.sh' ]] && source './secrets.sh' || source '../secrets.sh'

usage() {

    echo ""
	echo "usage: $0 [options] <genome-build> <annotations> "
    echo "" 
    echo "Process variant-gene associations and store them as part of GeneWeaver's"
    echo "homology model."
    echo ""
    echo "Processing options:"
    echo "  -s, --source  specify a hom_source_name for the new mappings (default = Variant)"
    echo "  --size        size of each subfile used for parallel processing (default = 2GB)"
    echo ""
    echo "Misc. options:"
    echo "  -h, --help    print this help message and exit"
    echo ""
	exit
}

## Default options if the user doesn't provide any
split_size='2GB'
hom_source_name="Variant"

## cmd line processing
while :; do
    case $1 in

		 --size)
			 if [ "$2"]; then
				 split_size="$2"
				 shift
			 else
				 echo "ERROR: --size requires an argument"
				 echo "       e.g. '2GB', '1024MB', etc."
				 exit 1
			 fi
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

## Check if the secrets were loaded
if [[ -z "$db_name" || -z "$db_user" || -z "$db_pass" ]];

    echo "ERROR: There was a problem loading the secrets file."
    echo "       DB credentials are missing."
    exit 1
fi

if [[ $# -lt 2 ]]; then

    echo "ERROR: You need to provide <genome-build> and <annotations> arguments"
    echo ""
    usage
    exit 1
fi

connect="host=$db_host dbname=$db_name user=$db_user password=$db_pass"

build="$1"
annotations="$2"

## Get the sp_id for the genome build we're updating
q_spid="SELECT sp_id FROM odestatic.genome_build WHERE gb_ref_id = '$build';"
## Query the species and remove whitespace
spid=$(psql "$connect" -t -c "$q_spid" | sed -e 's/\s//g')

## If no species ID was returned or psql encountered an error then exit
if [[ -z "$spid" || $? -ne 0 ]]; then

    echo "ERROR: There was an error retrieving the species for the build you provided."
    echo "       That genome build probably doesn't exist."
    exit 1
fi

## Get the gdb_id for the Variant gene type
q_vartype="SELECT gdb_id FROM odestatic.genedb WHERE gdb_name = 'Variant';"
## Query the variant gdb_id and remove whitespace
vartype=$(psql "$connect" -t -c "$q_vartype" | sed -e 's/\s//g')

## If no variant type ID was returned or psql encountered an error then exit
if [[ -z "$vartype" || $? -ne 0 ]]; then

    echo "ERROR: There was an error retrieving the variant gene type."
    echo "       The variant gene type is probably missing from genedb."
    exit 1
fi

## Absolute path to the annotations in a place the postgres server can access
tmp_annos=$(mktemp)
## Temp path to use for final annotation processing prior to insertion
tmp_splits=$(mktemp -u)

## Prefix to use for split files and the path where they are stored
split_pre="cvh-split"
split_path="$DATA_DIR/$split_pre"

log "Splitting annotations to process concurrently"

## Remove the header which will be added back later, split the annotations into
## separate 2GB files that can be processed in parallel
mlr --tsv --headerless-csv-output cat "$annotations" |
split -C "$split_size" -a 2 -d - "$tmp_splits"

log "Processing and formatting annotations"

## Format annotations so they can be ingested using postgres COPY
for vs in "$split_path"??
do
    out="$vs.processed"
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

mlr --tsvlite cat "$split_path"*processed > "$tmp_annos"

## Remove processed files
rm "$split_path"*

## Make sure the postgres server has permission to access this file
chmod 777 "$tmp_annos"

## Query to load the annotations into the staging table
read -r -d '' q_copy_annotations <<-EOF
    COPY annotation_staging 
    FROM '$tmp_annos' 
    WITH CSV HEADER DELIMITER E'\t';"
EOF

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

