#!/bin/bash

## file: merge-variants.sh
## desc: Removes variants with unknown clinical significance and merges
##       separate chromosome variant files into a single file.
## auth: TR
#


while getopts ":fh" opt; do
    case $opt in
        f)
            filter=1
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

if [[ $# -lt 1 || -n "$help" ]]; then

    echo "usage: $0 [options] <genome build> "
    echo "    -f, filter out variants with unknown clinical significance"
    echo "    -h, print this help message and exit"
    exit 0
fi

if [ -n "$filter" ]; then

    cat *"-$1-variants.tsv" | sed '/unknown/d' > "$1-variants-filtered.tsv"
else

    cat *"-$1-variants.tsv" > "$1-variants.tsv"
fi

