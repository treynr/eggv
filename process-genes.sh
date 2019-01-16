#!/usr/bin/env bash

## file: process-genes.sh
## desc: Process GTF formatted genes from Ensembl into a simple flat file.
## auth: TR

## Load the config
source "./config.sh"

usage() {

    echo "usage: $0 [options] <gtf> <output>"
    echo ""
    echo "Process GTF formatted genes from Ensembl into a simple flat file."
    echo ""
    echo "IO options:"
    echo "  -d, --delete  delete uncompressed input GVF file after processing"
    echo ""
    echo "Misc options:"
    echo "  -h, --help    print this help message and exit"
}

## cmd line processing
while :; do
    case $1 in

        -d | --delete)
            delete=1
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

    echo "ERROR: You need to provide an input GTF file"
    echo "       You need to provide an output filepath"
    echo ""
    usage
    exit 1
fi

input="$1"
output="$2"

## Check if the file is compressed, if it is decompress it
if [[ "${input##*.}" == "gz" ]]; then

    log "Decompressing GTF input"

    gunzip -c -d "$input" > "${input%.gz}"

    input="${input%.gz}"
fi

log "Processing GTF file"

## Remove trailing ; characters at the end of lines so they don't show up later
## when the field is parsed
sed -e 's/;$//g' "$input" | 
## Start streaming the file, assign indexes to columns and skip comments
mlr --implicit-csv-header --skip-comments --tsvlite cat |
## Only keep gene and transcript features, output as a set of key-value pairs
mlr --itsvlite --odkvp filter '$3 == "gene" || $3 == "transcript"' |
## Cut out the attribute field (index 9) which contains the data we want
mlr cut -f 9 |
## Separate each key-value pair in the attribute field
mlr nest --explode --pairs --across-fields -f 9 --nested-fs '; ' --nested-ps ' ' |
## Only keep records that contain an Ensembl transcript ID
mlr filter 'is_present($transcript_id)' |
## Cut out relevant fields and output without headers
mlr --headerless-csv-output cut -o -f 'gene_id,transcript_id,gene_biotype' |
## Rename headers
mlr --otsv label 'gene,transcript,biotype' |
## Remove quotes from strings
sed -e 's/"//g' > "$output"

## Delete input file if the -d/--delete option is set. 
[[ -n "$delete" ]] && rm "$input"

log "Finished GTF file processing"

