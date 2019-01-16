#!/usr/bin/env bash

## file: config.sh
## desc: Contains configuration variables, commonly used functions, and data
##       sources for the variant ETL pipeline.
## auth: TR

log() { printf "[%s] %s\n" "$(date '+%Y.%m.%d %H:%M:%S')" "$*" >&2; }

## Ensembl Variation data release version. You should make sure the Ensembl release uses
## the same NCBI dbSNP version specified below.
ENSEMBL=91
## NCBI dbSNP version.
DBSNP=150

## Human genome build identifier
HG38_BUILD="hg38"
## Mouse genome build identifier
MM10_BUILD="mm10"

## Ensembl FTP URLs for human and mouse variants. Warning, these are pretty big (~120GB)
## when unzipped.
HG38_VARIANT_URL="ftp://ftp.ensembl.org/pub/release-$ENSEMBL/variation/gvf/homo_sapiens/homo_sapiens_incl_consequences.gvf.gz"
MM10_VARIANT_URL="ftp://ftp.ensembl.org/pub/release-$ENSEMBL/variation/gvf/mus_musculus/mus_musculus_incl_consequences.gvf.gz"

## Ensembl FTP URLs for human and mouse genes.
HG38_GENE_URL="ftp://ftp.ensembl.org/pub/release-$ENSEMBL/gtf/homo_sapiens/Homo_sapiens.GRCh38.${ENSEMBL}.gtf.gz"
MM10_GENE_URL="ftp://ftp.ensembl.org/pub/release-$ENSEMBL/gtf/mus_musculus/Mus_musculus.GRCm38.${ENSEMBL}.gtf.gz"

## Location to save any executables
EXE_DIR="$HOME/.local/bin"
## Returns the directory that contains config.sh regardless of where/how the script is
## executed
SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
## Script directory
SRC_DIR="$SELF_DIR/src"
## Data directory where all raw and processed files live
DATA_DIR="$SELF_DIR/data"
## Log directory used *-pipeline.sh scripts
LOG_DIR="$SELF_DIR/logs"

## Add executable directory to the path
export PATH="$PATH:$EXE_DIR"

## Make directories if they don't exist
mkdir -p "$DATA_DIR"
mkdir -p "$LOG_DIR"

