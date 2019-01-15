#!/usr/bin/env bash

## file: process-gvf-file.sh
## desc: Performs processing steps on a single Genome Variant Format (GVF) file.
##       Removes irrelevant fields (for our work) and formats the file for later use.
##       A GVF reference can be found here:
##       https://github.com/The-Sequence-Ontology/Specifications/blob/master/gvf.md
## auth: TR

## Load the config file
source "./config.sh"

## This miller DSL is used to isolate and format variant effects (and related data) 
## from data parsed out of the attributes column of the GVF file.
## First, we check to see if the variant has an NCBI rsID, if it does great, if 
## not then the Dbxref field is left alone.
## Next, check to see if the variant has an effect, if it doesn't the DSL statement
## fills in the Variant_effect field with null values, otherwise it parses out the
## effects into their own fields. 
## Last, check to see if the variant has a minor allele frequency (MAF), if it doesn't a 
## value of zero is used as the MAF. 
read -r -d '' format_effects <<-'EOF'
        
    $Dbxref =~ "dbSNP.+:(rs[0-9]+)" {$Dbxref="\1";};

    is_absent($Variant_effect) {$Variant_effect = "0 0 0 0";};

    m = splitnvx($Variant_effect, " ");

    $effect = m[1];
    $biotype = m[3];
    $ensembl = m[4];

    if (is_absent($global_minor_allele_frequency)) {

        $maf = 0;
    } else {
        
        m = splitnvx($global_minor_allele_frequency, "|");

        $maf = m[2];
    }
EOF

## Column labels for processed output
columns='Dbxref,1,4,5,7,Variant_seq,Reference_seq,maf,effect,biotype,ensembl'
newcols='rsid,chr,start,end,strand,a1,a2,maf,effect,biotype,ensembl'

usage() {

    echo "usage: $0 [options] <gvf-file>"
    echo ""
    echo "Process a single Genome Variant Format (GVF) file. Removes irrelevant fields"
    echo "and formats the file for later use. Outputs processed results to stdout."
    echo ""
    echo "IO options:"
    echo "  --head        input GVF file contains a header row"
    echo "  -o, --output  output formatted data to a file"
    echo ""
    echo "Misc options:"
    echo "  -h, --help    print this help message and exit"
}

header="--tsv --implicit-csv-header"

## cmd line processing
while :; do
    case $1 in

        --head)
            header="--tsv"
            ;;

        -o | --output)
            if [ "$2" ]; then
                exec 1> "$2"
                shift
            else
                echo "ERROR: --output requires an argument"
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

if [ $# -lt 1 ]; then
    echo "ERROR: You need to provide an input file"
    usage
    exit 1
fi

## Read in the file using cat
mlr $header --skip-comments --headerless-csv-output cat "$1" |
## Only keep variants that are associated with a reference SNP identifier
mlr --tsv --implicit-csv-header filter '$9 =~ "Dbxref=dbSNP"' |
## For every record, parse the list of attribute key value pairs (found in
## column 9 which is the attributes column) and add them to the record itself
mlr --itsv --oxtab nest --explode --pairs --across-fields -f 9 --nested-ps '=' |
## Create a record for every variant effect value we find; a single SNP may
## have multiple functional effects
mlr --xtab nest --explode --values --across-records -f 'Variant_effect' --nested-fs ',' |
## Reformat variant effects and minor allele frequency
mlr --xtab put "$format_effects" |
## Save only the necessary unnecessary columns
mlr --ixtab --otsv --headerless-csv-output cut -o -f "$columns" |
## Rename the columns
mlr --tsv --implicit-csv-header label "$newcols" 

