#!/usr/bin/env bash

## file: config.sh
## desc: Contains configuration variables, commonly used functions, and URLs for the
##       shell scripts in this directory.
## vers: 0.1.0
## auth: TR

log() { printf "[%s] %s\n" "$(date '+%Y.%m.%d %H:%M:%S')" "$*" >&2; }

## Ensembl data release version. You should make sure the Ensembl release uses the 
## same NCBI dbSNP version specified below.
ENSEMBL=91

## Human genome build identifier
HUMAN_BUILD="hg38"
## Mouse genome build identifier
MOUSE_BUILD="mm10"

## Ensembl FTP URLs for human and mouse variants. Warning, these are pretty big (~120GB)
## when unzipped.
HUMAN_VARIANT_URL="ftp://ftp.ensembl.org/pub/release-$ENSEMBL/variation/gvf/homo_sapiens/homo_sapiens_incl_consequences.gvf.gz"
MOUSE_VARIANT_URL="ftp://ftp.ensembl.org/pub/release-$ENSEMBL/variation/gvf/mus_musculus/mus_musculus_incl_consequences.gvf.gz"

## Location to save any executables
EXE_DIR="$HOME/.local/bin"

## Add executable directory to the path
export PATH="$PATH:$EXE_DIR"
