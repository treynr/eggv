#!/bin/bash

## file: delete-variants.sh
## desc: Small helper shell script to delete variant data from the DB to
##       provide a clean slate for loading.
## vers: 0.1.12
## auth: TR
#

## Check to see if we can grab database, user, and password info from the
## config file used for the GW python library
if [[ $# -lt 2 && -f "gwlib.cfg" ]]; then

    dbname=$(sed -n -r -e 's/database\s*=\s*(.+)/\1/p' gwlib.cfg)
    dbuser=$(sed -n -r -e 's/user\s*=\s*(.+)/\1/p' gwlib.cfg)
    password=$(sed -n -r -e 's/password\s*=\s*(.+)/\1/p' gwlib.cfg)

    set -- "$dbname" "$dbuser"
fi

if [ $# -lt 2 ]; then
	echo "usage: $0 <db-name> <db-user>"
	exit
fi

if [ -z "$password" ]; then
    read -s -p "DB password: " password
fi

connect="host=localhost dbname=$1 user=$2 password=$password"

psql "$connect" -c "ALTER TABLE extsrc.variant DISABLE TRIGGER ALL;"
psql "$connect" -c "ALTER TABLE extsrc.variant_info DISABLE TRIGGER ALL;"
psql "$connect" -c "ALTER TABLE odestatic.variant_type DISABLE TRIGGER ALL;"

psql "$connect" -c "DROP INDEX extsrc.variant_var_id_uindex;"
psql "$connect" -c "DROP INDEX extsrc.variant_var_ref_id_index;"
psql "$connect" -c "DROP INDEX extsrc.variant_info_vri_id_index;"

(psql "$connect" -c "DELETE FROM extsrc.variant;") &
(psql "$connect" -c "DELETE FROM extsrc.variant_info;") &
(psql "$connect" -c "DELETE FROM odestatic.variant_type;") &

wait

## Resets the auto increment ID counters
psql "$connect" -c "ALTER SEQUENCE extsrc.variant_var_id_seq RESTART WITH 1;"
psql "$connect" -c "ALTER SEQUENCE extsrc.variant_info_vri_id_seq RESTART WITH 1;"
psql "$connect" -c "ALTER SEQUENCE odestatic.variant_type_vt_id_seq RESTART WITH 1;"

psql "$connect" -c "ALTER TABLE extsrc.variant ENABLE TRIGGER ALL;"
psql "$connect" -c "ALTER TABLE extsrc.variant_info ENABLE TRIGGER ALL;"
psql "$connect" -c "ALTER TABLE odestatic.variant_type ENABLE TRIGGER ALL;"

psql "$connect" -c "CREATE UNIQUE INDEX variant_var_id_uindex ON extsrc.variant (var_id);"
psql "$connect" -c "CREATE INDEX variant_var_ref_id_index ON extsrc.variant (var_ref_id);"
psql "$connect" -c "CREATE INDEX variant_info_vri_id_index ON extsrc.variant_info (vri_id);"

psql "$connect" -c "VACUUM ANALYZE extsrc.variant;"
psql "$connect" -c "VACUUM ANALYZE extsrc.variant_info;"
psql "$connect" -c "VACUUM ANALYZE odestatic.variant_type;"

