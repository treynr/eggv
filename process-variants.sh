#!/usr/bin/env bash

## file: process-variants.sh
## desc: Process GVF formatted variants into an intermediate flat file format
##       that can be used for analysis or integration into GeneWeaver.
##       A GVF reference can be found here:
##       https://github.com/The-Sequence-Ontology/Specifications/blob/master/gvf.md
## auth: TR

## Load the config file
source "./config.sh"

usage() {

    echo "usage: $0 [options] <gvf> <output>"
    echo ""
    echo "Process GVF formatted variants into an intermediate flat file format"
    echo "that can be used for analysis or integration into GeneWeaver."
    echo ""
    echo "IO options:"
    echo "  -d, --delete  delete uncompressed input GVF file after processing"
    echo ""
    echo "Processing options:"
    echo "  -s, --size    size of each subfile (default = 15GB)"
    echo ""
    echo "Misc options:"
    echo "  -h, --help    print this help message and exit"
}

## cmd line options
delete=""
size='15GB'

## cmd line processing
while :; do
    case $1 in

        -d | --delete)
            delete=1
            ;;

        -s | --size)
            if [ "$2" ]; then
                size="$2"
                shift
            else
                echo "ERROR: --size requires an argument"
                echo "       e.g. '2GB', '1024MB', etc."
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

if [ $# -lt 2 ]; then
    echo "ERROR: You need to provide an input GVF file"
    echo "       You need to provide an output filepath"
    echo ""
    usage
    exit 1
fi

input="$1"
output="$2"

## Check if the file is compressed, if it is decompress it
if [[ "${input##*.}" == ".gz" ]]; then

    log "Decompressing GVF input"

    gunzip -C -d "$input" > "${input%.gz}"

    input="${input%.gz}"
fi

log "Splitting GVF data into subfiles for concurrent processing"

## Prefix to use for split files and the path where they are stored
split_pre="pv-split"
split_path="$DATA_DIR/$split_pre"

split -C "$size" -a 2 -d "$input" "$split_path"

## Process and format each of the split vairant files in parallel
for vs in "$split_path"*
do
    (
        ## This script does the bulk of the processing work, including cleaning,
        ## formatting, filtering out unusable data, etc.
        ./process-variant-file.sh "$vs" > "${vs}.processed"

        ## Delete the split file since it's no longer needed
        rm "$vs"
    ) &
done

wait

log "Merging processed variant files"

## Concatenate everything together, use miller so only a single header line is output
mlr --tsv cat "$split_path"*.processed > "$output"

log "Cleaning up workspace"

## Delete split files
rm "${split_path}"*

## Delete input file if the -d/--delete option is set. A good idea in most cases b/c
## uncompressed GVF files are usualy huge.
[[ -n "$delete" ]] && rm "$input"

