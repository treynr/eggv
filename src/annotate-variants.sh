#!/usr/bin/env bash

## file: annotate-variants.sh
## desc: Annotates genetic variants to Ensembl gene identifiers using upstream,
##       downstream, or intragenic annotations.
## auth: TR

## Load the configuration file
[[ -f './config.sh' ]] && source './config.sh' || source '../config.sh'

usage() {

    echo "usage: $0 [options] <variants> <genes> <output>"
    echo ""
    echo "Annotate variants to Ensembl genes. Requires the variant and gene"
    echo "data files to be in TSV output formats produced by the process-variants.sh"
    echo "and process-gtf-file.sh scripts respectively."
    echo "By default, this script will remove upstream/downstream intergenic variant"
    echo "annotations. To generate a file only containing these annotations, use the"
    echo "-i/--intergenic option."
    echo ""
    echo "If memory is an issue, disable the duplicate SNP/gene pair removal"
    echo "using the --keep-duplicates option."
    echo ""
    echo "Processing options:"
    echo "  --keep-duplicates  keep duplicate SNP/gene pairs to reduce memory usage"
    echo "  -i, --intergenic   only annotate intergenic variants"
    echo "  -s, --size         size of each subfile (default = 15GB)"
    echo ""
    echo "Misc. options:"
    echo "  -h, --help         print this help message and exit"
}

## This miller DSL will remove duplicate SNP/gene pairs from the input stream.
read -r -d '' remove_duplicates <<-'EOF'

    if (!is_present(@seen[$rsid])) { @seen = {}; }
        
    @seen[$rsid][$gene] += 1;
    @seen[$rsid][$gene] == 1;
EOF

## This DSL query will parse out variant effects, and if the effect is intergenic
## (meaning an upstream or downstream annotation) it is ignored. If the variant occurs in
## a gene, then the affected gene and transcripts are parsed out and stored.
read -r -d '' effect_filter <<-'EOF'
        
    effects = splitnvx($effects, ";");
    elist = "";
    tlist = "";

    for (k, v in effects) {

        if (v =~ "effect=(up|down)stream") {

            continue;
        } else {

            e = splitkvx(v, "=", ",");

            elist = is_empty(elist) ? e["effect"] : elist . "," . e["effect"];
            tlist = is_empty(tlist) ? e["transcript"] : tlist . "," . e["transcript"];
        }
    }

    $effects = elist;
    $transcripts = tlist;

    ## Only keep intra-genic variants
    !is_empty(elist);
EOF

## Default options if the user doesn't provide any
duplicates=""
intergenic=""
split_size="15GB"

## cmd line processing
while :; do
    case $1 in

        --keep-duplicates)
            duplicates=1
            ;;

        -i | --intergenic)
            intergenic=1
            ;;

        -s | --size)
            if [ "$2" ]; then
                split_size="$2"
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

if [[ $# -lt 3 ]]; then
    echo "ERROR: The script requires three arguments:"
    echo "         a processed variant input file"
    echo "         a processed gene input file"
    echo "         an output filepath"
    echo ""
    usage
    exit 1
fi

variants="$1"
genes="$2"
output="$3"

## If the user wishes to keep duplicates, we replace the remove duplicates DSL with a
## statement that always returns true
if [[ -n "$duplicates" ]]; then
    remove_duplicates="true"
fi

log "Splitting variant data for parallelization"

## Prefix to use for split files and the path where they are stored
split_path="$(mktemp -u -p $DATA_DIR)"

## Split into evenly sized files, we remove the header row which is recreated later
mlr --tsv --headerless-csv-output cat "$variants" | 
split -C "$split_size" -a 3 -d - "$split_path"

log "Annotating variants"

## Process and format each of the split variant files in parallel
for vs in "$split_path"*
do
    (
        ## Miller DSL to filter out intergenic variants
        filt='$effect != "upstream_gene_variant" && $effect != "downstream_gene_variant"'

        ## The user wishes to keep intergenic variants instead
        if [[ -n "$intergenic" ]]; then
            filt='$effect == "upstream_gene_variant" || $effect == "downstream_gene_variant"'
        fi

        ## Add header for each file
        mlr --implicit-csv-header --tsvlite label 'rsid,chr,start,end,strand,a1,a2,maf,effect,biotype,transcript' "$vs" |
        ## Filter intergenic variants
        mlr --tsvlite filter "$filt" |
        ## Annotate variants based on the Ensembl transcript ID
        mlr --tsvlite join -j transcript --lp l_  -f "$genes" |
        ## Only save necessary columns
        mlr --tsvlite --headerless-csv-output cut -o -f 'rsid,l_gene,l_biotype,transcript,effect' |
        ## Rename columns
        mlr --tsvlite label 'rsid,gene,gene_biotype,transcript,snp_effect' |
        ## Remove duplicates
        mlr --tsvlite filter "$remove_duplicates" > "${vs}.processed"

        ## Delete the split variant file
        rm "$vs"
    ) &
done

wait

log "Merging preprocessed files"

## Merge processed files together, use miller so only a single header is output
mlr --tsvlite cat "${split_path}"*.processed > "$output"

## Clean up our mess
rm "${split_path}"*.processed

log "Generating a lite version of the annotations"

## Compact version of the variants with only SNPs and genes
mlr --tsvlite cut -f rsid,gene,gene_biotype "$output" > "${output%.*}-lite.tsv"

