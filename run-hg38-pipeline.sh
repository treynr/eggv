#!/usr/bin/env bash

## file: run-hg38-pipeline.sh
## desc: Executes the variant ETL pipeline for the hg38 genome build.
## auth: TR

source './config.sh'

if [[ ! -f './secrets.sh' ]]; then

    echo ""
    echo "WARNING: To use the loading features of this pipeline you will need to create"
    echo "         a secrets.sh file containing GeneWeaver database credentials."
    echo ""
fi

log "Retrieving variant and gene metadata from Ensembl..."

## Start by retrieving variant and gene data sources for the hg38 build
($SRC_DIR/retrieve-genes.sh --hg38 2> "$LOG_DIR/1a-retrieve-genes.log") & A=$!
($SRC_DIR/retrieve-variants.sh --hg38 2> "$LOG_DIR/1b-retrieve-variants.log") & B=$!

## Get return codes from subprocs
wait $A && A=$?
wait $B && B=$?

## If any subprocess encounters an error, the pipeline exits
if [[ $A -ne 0 ]]; then

	echo "ERROR: retrieve-genes.sh ran into problems, check the logs"
	exit 1

elif [[ $B -ne 0 ]]; then

	echo "ERROR retrieve-variant.sh ran into problems, check the logs"
	exit 1
fi

## Compressed gene and variant metadata from Ensembl
gene_gz="$DATA_DIR/hg38-${ENSEMBL}.gtf.gz"
variant_gz="$DATA_DIR/$HG38_BUILD-${ENSEMBL}.gvf.gz"

gene_final="$DATA_DIR/hg38-genes.tsv"
variant_pre="$DATA_DIR/hg38-variants-preprocessed.tsv"

log "Processing variant and gene metadata from Ensembl..."

## Completely process gene metadata and process the variants into a semi-usable format
($SRC_DIR/process-genes.sh "$gene_gz" "$gene_final" 2> "$LOG_DIR/2a-process-genes.log") & A=$!
($SRC_DIR/process-variants.sh -s "25GB" "$variant_gz" "$variant_pre" 2> "$LOG_DIR/2b-process-variants.log") & B=$!

## Get return codes from subprocs
wait $A && A=$?
wait $B && B=$?

if [[ $A -ne 0 ]]; then

	echo "ERROR: process-genes.sh ran into problems, check the logs"
	exit 1

elif [[ $B -ne 0 ]]; then

	echo "ERROR process-variant.sh ran into problems, check the logs"
	exit 1
fi

variant_ann_intra="$DATA_DIR/hg38-annotated-variants-intragenic.tsv"
variant_ann_inter="$DATA_DIR/hg38-annotated-variants-intergenic.tsv"
## These are stripped down versions of annotations that the annotate-variants
## script automatically creates
variant_ann_intra_lite="$DATA_DIR/hg38-annotated-variants-intragenic-lite.tsv"
variant_ann_inter_lite="$DATA_DIR/hg38-annotated-variants-intergenic-lite.tsv"

log "Annotating variants and loading variant metadata into the GW database..."

## Annotate intragenic and intergenic variants
## If disk space is an issue, run these one at a time
($SRC_DIR/annotate-variants.sh "$variant_pre" "$gene_final" "$variant_ann_intra" 2> "$LOG_DIR/3a-annotate-variants.log") & A=$!
($SRC_DIR/annotate-variants.sh -i "$variant_pre" "$gene_final" "$variant_ann_inter" 2> "$LOG_DIR/3b-annotate-variants.log") & B=$!

## and we can also simultaneously load the variants into the database
($SRC_DIR/load-variants.sh "hg38" "$variant_pre" 2> "$LOG_DIR/3c-load-variants.log") & C=$!

## Get return codes from subprocs
wait $A && A=$?
wait $B && B=$?
wait $C && C=$?

if [[ $A -ne 0 ]]; then

	echo "ERROR: annotate-variants.sh (intragenic) ran into problems, check the logs"
	exit 1

elif [[ $B -ne 0 ]]; then

	echo "ERROR process-variant.sh (intergenic) ran into problems, check the logs"
	exit 1

elif [[ $C -ne 0 ]]; then

	echo "ERROR load-variants.sh ran into problems, check the logs"
	exit 1
fi

log "Generating variant GeneWeaver IDs and separating intergenic variants..."

## Create ode_gene_ids for all the variants
($SRC_DIR/create-variant-ode-ids.sh "hg38" 2> "$LOG_DIR/4a-create-variant-ode-ids.log") & A=$!
## Separate intergenic variants into upstream/downstream
($SRC_DIR/separate-intergenic-variants.sh "$variant_ann_inter" 2> "$LOG_DIR/4b-separate-intergenic-variants.log") & B=$!

## Get return codes from subprocs
wait $A && A=$?
wait $B && B=$?

if [[ $A -ne 0 ]]; then

	echo "ERROR: create-variant-ode-ids.sh ran into problems, check the logs"
	exit 1

elif [[ $B -ne 0 ]]; then

	echo "ERROR separate-intergenic-variants.sh ran into problems, check the logs"
	exit 1
fi

log "Loading variant annotations into the GeneWeaver database..."

## These were made by the separate-intergenic-variants script
downstream_ann="$DATA_DIR/hg38-annotated-variants-downstream-lite.tsv"
upstream_ann="$DATA_DIR/hg38-annotated-variants-upstream-lite.tsv"

$SRC_DIR/create-variant-homology.sh -s "Variant" "hg38" "$variant_ann_intra_lite"

if [[ $? -ne 0 ]]; then

	echo "ERROR: create-variant-homology.sh (intragenic) ran into problems, check the logs"
	exit 1
fi

$SRC_DIR/create-variant-homology.sh -s "Variant (Downstream)" "hg38" "$downstream_ann"

if [[ $? -ne 0 ]]; then

	echo "ERROR: create-variant-homology.sh (downstream) ran into problems, check the logs"
	exit 1
fi

$SRC_DIR/create-variant-homology.sh -s "Variant (Upstream)" "hg38" "$upstream_ann"

if [[ $? -ne 0 ]]; then

	echo "ERROR: create-variant-homology.sh (upstream) ran into problems, check the logs"
	exit 1
fi

