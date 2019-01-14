#!/usr/bin/env bash

## file: config.sh
## desc: Contains configuration variables, commonly used functions, and URLs for the
##       shell scripts in this directory.
## vers: 0.1.0
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

## Location to save any executables
EXE_DIR="$HOME/.local/bin"
## Returns the directory that contains config.sh regardless of where/how the script is
## executed
SELF_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
## Data directory where all raw and processed files live
DATA_DIR="$SELF_DIR/data"

## Add executable directory to the path
export PATH="$PATH:$EXE_DIR"

## Make the data directory if it doesn't exist
mkdir -p "$DATA_DIR"

