#!/usr/bin/env bash

## file: load-variants.sh
## desc: Small helper shell script to load variant data into the DB.
## vers: 0.1.12
## auth: TR
#

while getopts ":chs" opt; do
    case $opt in
        c)
            config=1
            ;;
        s)
            slow=1
            ;;
        h)
            help=1
            ;;
        \?)
            help=1
            ;;
    esac
done

shift $((OPTIND-1))

## Check to see if we can grab database, user, and password info from the
## config file used for the GW python library
if [[ -n "$config"  && -f "gwlib.cfg" ]]; then

    dbname=$(sed -n -r -e 's/database\s*=\s*(.+)/\1/p' gwlib.cfg)
    dbuser=$(sed -n -r -e 's/user\s*=\s*(.+)/\1/p' gwlib.cfg)
    password=$(sed -n -r -e 's/password\s*=\s*(.+)/\1/p' gwlib.cfg)

    set -- "$dbname" "$dbuser" "$1" "$2"
fi

if [[ ($# -lt 4 && -z "$config") || -n "$help" ]]; then

    echo ""
	echo "usage: $0 [options] <db-name> <db-user> <genome-build> <data-file> "
	echo "usage: $0 [options] -c <genome-build> <data-file> "
    echo "" 
    echo "    -c, get <db-name> and <db-user> from the GW config file"
    echo "    -s, use slow but disk space friendly variant loading method"
    echo "    -h, print this help message and exit"
	exit
fi

dbname="$1"
dbuser="$2"
build="$3"
variants="$4"

if [ -z "$password" ]; then
    read -s -p "DB password: " password
fi

connect="host=localhost dbname=$dbname user=$dbuser password=$password"

## Disable triggers for faster loading
psql "$connect" -c "ALTER TABLE extsrc.variant DISABLE TRIGGER ALL;"
psql "$connect" -c "ALTER TABLE extsrc.variant_info DISABLE TRIGGER ALL;"
psql "$connect" -c "ALTER TABLE odestatic.variant_type DISABLE TRIGGER ALL;"

## Delete indexes for faster loading
psql "$connect" -c "DROP INDEX extsrc.variant_var_id_uindex;"
psql "$connect" -c "DROP INDEX extsrc.variant_var_ref_id_index;"
psql "$connect" -c "DROP INDEX extsrc.variant_vri_id_index;"
psql "$connect" -c "DROP INDEX extsrc.variant_info_vri_id_index;"
psql "$connect" -c "DROP INDEX extsrc.variant_info_vri_chromosome_vri_position_index;"


## Use slower method
if [ -n "$slow" ]; then

    python load_variants.py "$build" "$variants"
else

    ## Writes variant data to a file in a TSV format for postgres to load
    python load_variants.py --write "$build" "$variants"

    ## The file name created by load_variants looks something like 
    ## e.g. "hg38-variant-copy.tsv".
    ## The COPY command requires an absolute path
    #sql_path=$(realpath "${build}-variant-copy.tsv")
    copy_file="${build}-variant-copy.tsv"

    ## Set permissions to rw/rw/rw and move to tmp folder so the postgres
    ## server process can access the file
    chmod 666 "$copy_file"
    mv "$copy_file" "/tmp"

    columns="var_ref_id, var_allelel, vt_id, var_ref_cur, var_obs_alleles, var_ma, var_maf, var_clinsig, vri_id"
    copy="COPY extsrc.variant ($columns) FROM '/tmp/$copy_file' WITH NULL AS 'NULL';"

    psql "$connect" -c "$copy"

    ## The copy input is no longer needed
    rm "/tmp/$copy_file"
fi

psql "$connect" -c "ALTER TABLE extsrc.variant ENABLE TRIGGER ALL;"
psql "$connect" -c "ALTER TABLE extsrc.variant_info ENABLE TRIGGER ALL;"
psql "$connect" -c "ALTER TABLE odestatic.variant_type ENABLE TRIGGER ALL;"

(psql "$connect" -c "CREATE UNIQUE INDEX variant_var_id_uindex ON extsrc.variant (var_id);") &
(psql "$connect" -c "CREATE INDEX variant_var_ref_id_index ON extsrc.variant (var_ref_id);") &
(psql "$connect" -c "CREATE INDEX variant_vri_id_index ON extsrc.variant (variant_vri_id_index);") &
(psql "$connect" -c "CREATE INDEX variant_info_vri_id_index ON extsrc.variant_info (vri_id);") &
(psql "$connect" -c "CREATE INDEX variant_info_vri_chromosome_vri_position_index ON extsrc.variant_info_vri_chromosome_vri_position_index (vri_id);") &

wait

psql "$connect" -c "VACUUM ANALYZE extsrc.variant;"
psql "$connect" -c "VACUUM ANALYZE extsrc.variant_info;"
psql "$connect" -c "VACUUM ANALYZE odestatic.variant_type;"

