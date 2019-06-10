#!/usr/bin/env bash

## file: separate-intergenic-variants.sh
## desc: Reads in a file containing intergenic variants and separates them into two
##       separate files, one containing upstream variants and the other containing 
##       downstream variants. 
## auth: TR

if [[ $# -lt 1 ]]; then

    echo "ERROR: The script requires one argument: an annotated variant data file."
    echo "       This type of file is produced by the annotate-variants.sh script."
    echo ""
    exit 1
fi

if [[ "$1" =~ intergenic ]]; then

    upstream="${1/intergenic/upstream}"
    downstream="${1/intergenic/downstream}"
else

    upstream="${1%.tsv}-upstream.tsv"
    downstream="${1%.tsv}-downstream.tsv"
fi

(mlr --tsvlite filter '$snp_effect == "upstream_gene_variant"' "$1" > "$upstream") &
(mlr --tsvlite filter '$snp_effect == "downstream_gene_variant"' "$1" > "$downstream") &

(
    mlr --tsvlite filter '$snp_effect == "upstream_gene_variant"' "$1" |
    mlr --tsvlite cut -f rsid,gene,gene_biotype > "${upstream%.tsv}-lite.tsv"
) &

(
    mlr --tsvlite filter '$snp_effect == "downstream_gene_variant"' "$1" |
    mlr --tsvlite cut -f rsid,gene,gene_biotype > "${downstream%.tsv}-lite.tsv"
) &

wait

