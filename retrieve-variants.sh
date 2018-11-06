#!/usr/bin/env bash

## file: retrieve-variants.sh
## desc: Retrieves and pre-processes variant data from Ensembl.
##       Currently retrieves data from Ensembl release v. 91, which maps onto
##       NCBI dbSNP v. 150.
## auth: TR

## Load the config file
source "./config.sh"

usage() {

    echo "usage: $0 [options]"
    echo ""
    echo "Retrieve and preprocess variant data from Ensembl."
    echo "Current configuration settings are set to retrieve variant data from "
    echo "Ensembl v. $ENSEMBL"
    echo ""
    echo "Retrieval options:"
    echo "  --human     retrieve human ($HUMAN_BUILD) variants"
    echo "  --mouse     retrieve mouse ($MOUSE_BUILD) variants"
    echo "  -s, --split file size used when splitting files for processing (default = 15GB)"
    echo "  -f, --force force data retrieval if the variant files already exist"
    echo ""
    echo "Misc options:"
    echo "  -h, --help  print this help message and exit"
}

## Unzipper to use
if hash pigz 2>/dev/null; then
    unzip="pigz"
else
    unzip="gzip"
fi

## Options set by cmd line arguments
build=""
variant_url=""
force=""
nsplit='15GB'
## Arguments to this script so we know how the data file was generated
filetag="$0 $@"

## cmd line processing
while :; do
    case $1 in

        --human)
            build="$HUMAN_BUILD"
            variant_url="$HUMAN_VARIANT_URL"
            ;;

        --mouse)
            build="$MOUSE_BUILD"
            variant_url="$MOUSE_VARIANT_URL"
            ;;

        -f | --force)
            force=1
            ;;

        -s | --split)
            if [ "$2" ]; then
                nsplit="$2"
                shift
            else
                echo "ERROR: --split requires an argument"
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

if [[ -z "$build" ]]; then

    echo 'ERROR: you must specify a species'
    usage
    exit 1
fi

## We'll save the Genome Variant Format (GVF) file from Ensembl to this filepath
gvf="${build}-$ENSEMBL.gvf"
## Filepath to the processed output
output="${gvf%.gvf}-preprocessed.tsv"

if [[ -n "$force" || ! -f "${gvf}.gz" ]]; then

    log "Downloading $build data"

    curl -s -S -o "${gvf}.gz" "$variant_url"
fi

#if [[ -n "$force" || ! -f "$gvf" ]]; then
#
#    log "Decompressing $build data"
#
#    $unzip -C -d "${gvf}.gz" > "$gvf"
#fi
#
#log "Splitting $build data for concurrent processing"
#
### Split into ten evenly sized files
#split -C "$nsplit" -a 2 -d "$gvf" "variant-split"
#
#log "Formatting and processing $build data"
#
### Process and format each of the split variant files in parallel
#for vs in "variant-split"*
#do
#    ## Isolate number for split file
#    n="${vs#variant-split}"
#
#    (
#        ## This script does the bulk of the processing work, including cleaning, 
#        ## formatting, filtering out unusable data, etc.
#        ./preprocess-variant-file.sh "$vs" > "${output}.$n"
#
#        ## Delete the split file since it's no longer needede
#        rm "$vs"
#    ) &
#done
#
#wait
#
#log "Merging preprocessed files"
#
### Concatenate everything together, use miller so only a single header line 
### is output
#mlr --tsvlite cat "$output".* > "$output"
#
#log "Cleaning up workspace"
#
### Delete split files and uncompressed GVF file which is probably huge
#rm "variant-split"*
#rm "${output}."??
#rm "$gvf"

