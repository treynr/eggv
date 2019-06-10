#!/usr/bin/env bash

## file: create-variant-odes.sh
## desc: Generates internal GeneWeaver IDs (ode_gene_id) for variants. This enables 
##       collections of variants to be stored as sets and used by the rest of the GW 
##       application (e.g. analysis tools).
## auth: TR
#

## Load the configuration file
[[ -f './config.sh' ]] && source './config.sh' || source '../config.sh'

## Load the secrets file which contains credentials
[[ -f './secrets.sh' ]] && source './secrets.sh' || source '../secrets.sh'

usage() {

    echo ""
	echo "usage: $0 [options] <genome-build> <data-file> "
    echo ""
    echo "Generate internal GeneWeaver IDs for all variants." 
    echo ""
    echo "Misc. options:"
    echo "  -h, --help  print this help message and exit"
	exit
}

while :; do
    case $1 in

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

if [[ $# -lt 1 ]]; then

    echo "ERROR: You need to provide a <genome-build> argument"
    echo ""
    usage
    exit 1
fi

connect="host=$db_host dbname=$db_name user=$db_user password=$db_pass"

## This query is used to check if ode_gene_ids exist for variants belonging to
## a particular genome build. 
read -r -d '' ode_exists <<- EOF
    SELECT       count(*) 
    FROM         extsrc.gene AS g
    INNER JOIN   odestatic.genedb AS gdb
    USING        (gdb_id)
    INNER JOIN   odestatic.genome_build AS gb
    ON           gdb.sp_id = gb.sp_id
    WHERE        gdb.gdb_name = 'Variant' AND
                 gb.gb_ref_id = '$build';
EOF

exists=$(psql "$connect" -t -c "$ode_exists" | sed -e 's/\s//g')

## If gene_ids exist for the variants, we don't do anything
if [ "$exists" -ne 0 ]; then

    echo "ERROR: ode_gene_ids already exist for this genome build. If you wish to"
    echo "       regenerate them, you must drop all ode_gene_ids for this build first."
    exit 1
fi

## Disable triggers and delete indexes for faster loading
psql "$connect" -c "ALTER TABLE extsrc.gene DISABLE TRIGGER ALL;"
psql "$connect" -c "DROP INDEX IF EXISTS extsrc.gene_lower_gdb_id_sp_id_expr_idx;"
psql "$connect" -c "DROP INDEX IF EXISTS extsrc.gene_ode_gene_id_gdb_id_idx;"
psql "$connect" -c "DROP INDEX IF EXISTS extsrc.gene_ode_gene_id_idx;"
psql "$connect" -c "DROP INDEX IF EXISTS extsrc.gene_ode_ref_id_idx;"

## This is the query to generate ode_gene_ids for all variants belonging to a
## particular genome build.
read -r -d '' create_genes <<- EOF

    INSERT INTO extsrc.gene (
        ode_ref_id, gdb_id, sp_id, ode_pref, ode_date, old_ode_gene_ids
    ) SELECT  
        v.var_id,
        (SELECT gdb_id FROM odestatic.genedb WHERE gdb_name = 'Variant'),
        gb.sp_id, 
        't', 
        NOW(),
        NULL 
    FROM        extsrc.variant_info AS vi 
    INNER JOIN  variant AS v 
    USING       (vri_id) 
    INNER JOIN  genome_build AS gb 
    USING       (gb_id) 
    WHERE       gb.gb_ref_id = '$build';
EOF

psql "$connect" -c "$create_genes"

## Recreate indexes and activate triggers, these indexes are copied from the
## schema dump on bitbucket
(psql "$connect" -c "CREATE INDEX gene_lower_gdb_id_sp_id_expr_idx ON extsrc.gene USING btree (lower((ode_ref_id)::text), gdb_id, sp_id, ((((gdb_id)::text || '*'::text) || lower((ode_ref_id)::text))));") &
(psql "$connect" -c "CREATE INDEX gene_ode_gene_id_gdb_id_idx ON extsrc.gene USING btree (ode_gene_id, gdb_id);") &
(psql "$connect" -c "CREATE INDEX gene_ode_gene_id_idx ON extsrc.gene USING btree (ode_gene_id);") &
(psql "$connect" -c "CREATE INDEX gene_ode_ref_id_idx ON extsrc.gene USING btree (ode_ref_id);") &

wait

psql "$connect" -c "ALTER TABLE extsrc.gene ENABLE TRIGGER ALL;"
psql "$connect" -c "VACUUM ANALYZE extsrc.gene;"

