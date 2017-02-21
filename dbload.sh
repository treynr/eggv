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

copy="COPY extsrc.variant FROM $1;"

read -r -d '' psql_command << EOM
$drop_idx0
$drop_idx1
$drop_idx2
$drop_idx3
$drop_idx4
$trans_begin
$copy
$trans_end
$make_idx0
$make_idx1
$make_idx2
$make_idx3
$make_idx4
$analyze
EOM

echo "$psql_command" | psql -d $2 -U $3

