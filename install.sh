#!/usr/bin/env bash

## file: install.sh
## desc: Retrieves additional repos and python libraries associated required by
##       this loader.
## auth: TR

source './config.sh'

## Check to see if any prereqs are missing
if ! hash wget &>/dev/null; then

    log 'ERROR: wget is missing, please install it prior to using this pipeline'
    exit 1
fi

if ! hash psql 2>/dev/null; then

    log 'ERROR: psql is missing. If you plan on loading variant metadata directly'
    log '       into a database, postgres and psql are required.'
fi

if ! hash mlr 2>/dev/null; then

    log 'Retrieving and installing miller v. 5.4.0'

    wget --quiet -O "$EXE_DIR/mlr" 'https://github.com/johnkerl/miller/releases/download/5.4.0/mlr.linux.x86_64'

    chmod +x "$EXE_DIR/mlr"
fi

