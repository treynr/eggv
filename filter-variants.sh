#!/bin/bash

## file: filter-merge-variants.sh
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

if [ -n "$help" ]; then

    echo "usage: $0 [options]"
    echo "    -f, filter out variants with unknown clinical significance"
    echo "    -h, print this help message and exit"
    exit 0
fi

if [ -n "$filter" ]; then

    cat *"-hg38-variants.tsv" | sed '/unknown/d' > "hg38-variants-filtered.tsv"
else

    cat *"-hg38-variants.tsv" > "hg38-variants.tsv"
fi

#cat hg38-variants.tsv.bak | sed '/unknown/d' > filtered-hg38-variants.tsv
#cut -f3 mp-nervous-gene-annotations.tsv | sed s/\|/\\n/g > mp-nervous-gene-annotations-og.txt
