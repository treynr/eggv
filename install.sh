#!/usr/bin/env bash

## file: install.sh
## desc: Retrieves additional repos and python libraries associated required by
##       this loader.
## auth: TR

git clone@s1k.pw:gwlib

pip install --user psycopg2

