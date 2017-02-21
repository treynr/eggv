#!/bin/bash

## file: dbload
## desc: Small helper shell script to load the variant COPY sql file into
##       the database in the most efficient way possible.
## vers: 0.1.0
## auth: TR
#

if [ $# -lt 3 ]; then
	echo "usage: $0 <sql-file> <db-name> <db-user>"
	exit
fi

## COPY command requires an absolute path
sql_path=$(realpath $1)

trans_begin="BEGIN;"
trans_end="COMMIT;"

analyze="ANALYZE;"

drop_idx0="DROP INDEX extsrc.variant_var_id_idx;"
drop_idx1="DROP INDEX extsrc.variant_var_ref_id_idx;"
drop_idx2="DROP INDEX extsrc.variant_var_ref_id_gb_id_idx;"
drop_idx3="DROP INDEX extsrc.variant_var_ref_id_var_allele_idx;"
drop_idx4="DROP INDEX extsrc.variant_var_chromosome_var_position_idx;"

make_idx0="CREATE INDEX variant_var_id_idx ON extsrc.variant (var_id);"
make_idx1="CREATE INDEX variant_var_ref_id_idx ON extsrc.variant (var_ref_id);"
make_idx2="CREATE INDEX variant_var_ref_id_gb_id_idx ON extsrc.variant (var_ref_id, gb_id);"
make_idx3="CREATE INDEX variant_var_ref_id_var_allele_idx ON extsrc.variant (var_ref_id, var_allele);"
make_idx4="CREATE INDEX variant_var_chromosome_var_position_idx ON extsrc.variant (var_chromosome, var_position);"

copy="COPY extsrc.variant FROM '$sql_path';"

read -s -p "DB password: " password

connect_string="host=localhost dbname=$2 user=$3 password=$password"

## Drop indexes for faster insertion
(psql "$connect_string" -c "$drop_idx0") &
(psql "$connect_string" -c "$drop_idx1") &
(psql "$connect_string" -c "$drop_idx2") &
(psql "$connect_string" -c "$drop_idx3") &
(psql "$connect_string" -c "$drop_idx4") &

wait

## Insert the data
echo "$trans_begin $copy $trans_end" | psql "$connect_string" # -c "$trans_begin $copy $trans_end"

## Build indexes in parallel
(psql "$connect_string" -c "$make_idx0") &
(psql "$connect_string" -c "$make_idx1") &
(psql "$connect_string" -c "$make_idx2") &
(psql "$connect_string" -c "$make_idx3") &
(psql "$connect_string" -c "$make_idx4") &

wait

## Re-analyze since we made some major changes
psql "$connect_string" -c "$analyze"

