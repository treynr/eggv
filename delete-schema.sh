#!/usr/bin/env bash

## file: delete-schema.sh
## desc: Helper script to completely delete the variant schema and data from
##       the GeneWeaver database.
## vers: 0.1.12
## auth: TR
#

while getopts ":chs" opt; do
    case $opt in
        c)
            config=1
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
sdir="./schema"

psql "$connect" -f "$sdir/delete-variant-schema.sql"

