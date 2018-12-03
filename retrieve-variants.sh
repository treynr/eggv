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
    echo "Retrieve and variant data from Ensembl."
    echo "Current configuration settings are set to retrieve variant data from "
    echo "Ensembl v. $ENSEMBL"
    echo ""
    echo "Retrieval options:"
    echo "  --human     retrieve human ($HUMAN_BUILD) variants"
    echo "  --mouse     retrieve mouse ($MOUSE_BUILD) variants"
    echo "  -f, --force force data retrieval if the variant files already exist"
    echo ""
    echo "Misc options:"
    echo "  -h, --help  print this help message and exit"
}

## Options set by cmd line arguments
build=""
variant_url=""
force=""
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

    wget --quiet -O "${gvf}.gz" "$variant_url"
fi

