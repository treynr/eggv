#!/usr/bin/env bash

## file: delete-variants.sh
## desc: Deletes all variant metadata and variant-gene associations from the database.
##       Use with caution.
## auth: TR

## Load the config
source './config.sh'

## Load the secrets
source './secrets.sh'

## Check if the secrets were loaded
if [[ -z "$db_name" || -z "$db_user" || -z "$db_pass" ]]; then

    echo "ERROR: There was a problem loading the secrets file."
    echo "       DB credentials are missing."
    exit 1
fi

while true; do
    read -p "Are you sure you want to delete the variant metadata? " yn

    case $yn in
        [Yy]*) break;;
        [Nn]*) exit;;
    esac
done

connect="host=$db_host dbname=$db_name user=$db_user password=$db_pass"

## Variant table constraints
(psql "$connect" -c "ALTER TABLE extsrc.variant DISABLE TRIGGER ALL;") &
(psql "$connect" -c "ALTER TABLE extsrc.variant_info DISABLE TRIGGER ALL;") &
## Gene table constraints
(psql "$connect" -c "ALTER TABLE extsrc.gene DISABLE TRIGGER ALL;") &

## Indexes for variant tables
(psql "$connect" -c "DROP INDEX IF EXISTS extsrc.variant_var_id_uindex;") &
(psql "$connect" -c "DROP INDEX IF EXISTS extsrc.variant_var_ref_id_index;") &
(psql "$connect" -c "DROP INDEX IF EXISTS extsrc.variant_vri_id_index;") &
(psql "$connect" -c "DROP INDEX IF EXISTS extsrc.variant_info_vri_id_uindex;") &
(psql "$connect" -c "DROP INDEX IF EXISTS extsrc.variant_info_vri_chromosome_vri_position_index;") &

## Gene table indexes. Actually I don't think it makes sense to delete and reindex these,
## especially since our delete from statement can take advantage of the gdb_id index
#(psql "$connect" -c "DROP INDEX IF EXISTS extsrc.gene_lower_gdb_id_sp_id_expr_idx;") &
#(psql "$connect" -c "DROP INDEX IF EXISTS extsrc.gene_ode_gene_id_gdb_id_idx;") &
#(psql "$connect" -c "DROP INDEX IF EXISTS extsrc.gene_ode_gene_id_idx;") &
#(psql "$connect" -c "DROP INDEX IF EXISTS extsrc.gene_ode_ref_id_idx;") &

wait

## Delete all data from the variant tables
(psql "$connect" -c "DELETE FROM extsrc.variant;") &
(psql "$connect" -c "DELETE FROM extsrc.variant_info;") &
## Delete variant -> ode_gene_id assocations
(psql "$connect" -c "DELETE FROM extsrc.gene WHERE gdb_id = (SELECT gdb_id FROM odestatic.genedb WHERE gdb_name = 'Variant');") &
## Delete variant annotations
## WARNING: This assumes we've supplied a hom_source_name of 'Variant' for annotations
(psql "$connect" -c "DELETE FROM extsrc.homology WHERE hom_source_name LIKE 'Variant%';") &

wait

## Resets the auto increment ID counters
psql "$connect" -c "ALTER SEQUENCE extsrc.variant_var_id_seq RESTART WITH 1;"
psql "$connect" -c "ALTER SEQUENCE extsrc.variant_info_vri_id_seq RESTART WITH 1;"

## Re-enable variant table constraints
psql "$connect" -c "ALTER TABLE extsrc.variant ENABLE TRIGGER ALL;"
psql "$connect" -c "ALTER TABLE extsrc.variant_info ENABLE TRIGGER ALL;"

## Re-enable gene table constraints
psql "$connect" -c "ALTER TABLE extsrc.gene ENABLE TRIGGER ALL;"

(psql "$connect" -c "CREATE UNIQUE INDEX variant_var_id_uindex ON extsrc.variant (var_id);") &
(psql "$connect" -c "CREATE INDEX variant_var_ref_id_index ON extsrc.variant (var_ref_id);") &
(psql "$connect" -c "CREATE INDEX variant_vri_id_index ON extsrc.variant (vri_id);") &
(psql "$connect" -c "CREATE UNIQUE INDEX variant_info_vri_id_uindex ON extsrc.variant_info (vri_id);") &
(psql "$connect" -c "CREATE INDEX variant_info_vri_chromosome_vri_position_index ON extsrc.variant_info (vri_chromosome, vri_position);") &

wait

(psql "$connect" -c "VACUUM ANALYZE extsrc.variant;") &
(psql "$connect" -c "VACUUM ANALYZE extsrc.variant_info;") &
(psql "$connect" -c "VACUUM ANALYZE extsrc.gene;") &
(psql "$connect" -c "VACUUM ANALYZE extsrc.homology;") &

wait

