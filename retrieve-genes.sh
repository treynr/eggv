#!/usr/bin/env bash

## file: retrieve-genes.sh
## desc: Retrieves gene entity metadatadata from Ensembl. Used to annotate variants to
##       genes. Currently retrieves data from Ensembl release v. 91.
## auth: TR

## Load the config
source "./config.sh"

usage() {

    echo "usage: $0 [options]"
    echo ""
    echo "Retrieve gene biotype metadata from Ensembl."
    echo "Current configuration settings are set to retrieve variant data from "
    echo "Ensembl v. $ENSEMBL"
    echo ""
	echo "Build options:"
	echo "  --hg38      retrieve human ($HG38_BUILD) genes"
	echo "  --mm10      retrieve mouse ($MM10_BUILD) genes"
    echo ""
	echo "Retrieval options:"
	echo "  -f, --force force data retrieval if the gene files already exist"
	echo ""
	echo "Misc options:"
	echo "  -h, --help  print this help message and exit"
}


## Options set by command line arguments
build=""
gene_url=""
force=""

## cmd line processing
while :; do
    case $1 in

        --hg38)
            build="$HG38_BUILD"
            gene_url="$HG38_GENE_URL"
            ;;

        --mm10)
            build="$MM10_BUILD"
            gene_url="$MM10_GENE_URL"
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

    echo 'ERROR: you must specify a species build to use'
    usage
    exit 1
fi

## GTF output
output="$DATA_DIR/${build}-$ENSEMBL.gtf"

if [[ -n "$force" || ! -f "${output}.gz" ]]; then

    log "Downloading $build data"

    wget --quiet -O "${output}.gz" "$gene_url"
fi



## Filepath to the Genome Variant Format (GVF) file from Ensembl
gtf_fp="./${build}-$release.gtf"
## Filepath to the processed output
output="${gtf_fp%.gtf}-genes.tsv"

if [[ -n "$force" || (! -f "$gtf_fp" && ! -f "${gtf_fp}.gz") ]]; then

    log "Downloading $build gene data"

    curl -s -S -o "${gtf_fp}.gz" "$url"

    log "Decompressing $build gene data"

    $unzip -d "${gtf_fp}.gz"
fi

log "Formatting and processing $build data"

./process-gtf-file.sh "$gtf_fp" > "$output"

