#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: globe.py
## desc: Contains global variables, mostly output directories and files used by
##       the pipeline scripts.

from pathlib import Path
import logging

logging.getLogger(__name__).addHandler(logging.NullHandler())


## Variables ##

_var_human_chromosomes = [str(c) for c in range(1, 23)] + ['X', 'Y']
_var_mouse_chromosomes = [str(c) for c in range(1, 20)] + ['X', 'Y']


## URLs ##

## Ensembl variation build v. 95
## For humans, Ensembl stores variants separately per chromosome, so this URL must
## be substituted w/ the chromosome
_url_hg38_variation = 'http://ftp.ensembl.org/pub/release-95/variation/gvf/homo_sapiens/homo_sapiens_incl_consequences-chr{}.gvf.gz'
_url_mm10_variation = 'http://ftp.ensembl.org/pub/release-95/variation/gvf/mus_musculus/mus_musculus_incl_consequences.gvf.gz'

## Ensembl gene build v. 95
_url_hg38_gene = 'http://ftp.ensembl.org/pub/release-95/gtf/homo_sapiens/Homo_sapiens.GRCh38.95.gtf.gz'
_url_mm10_gene = 'http://ftp.ensembl.org/pub/release-95/gtf/mus_musculus/Mus_musculus.GRCm38.95.gtf.gz'


## Output directories ##

## Data directory
_dir_data = 'data/'

## Variant datasets
_dir_hg38_variant_raw = Path(_dir_data, 'variant/hg38/raw').as_posix()
_dir_hg38_variant_proc = Path(_dir_data, 'variant/hg38/processed').as_posix()
_dir_mm10_variant_raw = Path(_dir_data, 'variant/mm10/raw').as_posix()
_dir_mm10_variant_proc = Path(_dir_data, 'variant/mm10/processed').as_posix()

## Gene datasets
_dir_hg38_gene_raw = Path(_dir_data, 'gene/hg38/raw').as_posix()
_dir_hg38_gene_proc = Path(_dir_data, 'gene/hg38/processed').as_posix()
_dir_mm10_gene_raw = Path(_dir_data, 'gene/mm10/raw').as_posix()
_dir_mm10_gene_proc = Path(_dir_data, 'gene/mm10/processed').as_posix()


## Output files ##

_fp_hg38_gene_compressed = Path(_dir_hg38_gene_raw, 'hg38-gene-build.gtf.gz')
_fp_hg38_gene = Path(_dir_hg38_gene_raw, 'hg38-gene-build.gtf')

_fp_mm10_gene_compressed = Path(_dir_mm10_gene_raw, 'mm10-gene-build.gtf.gz')
_fp_mm10_gene = Path(_dir_mm10_gene_raw, 'mm10-gene-build.gtf')

_fp_mm10_variant_compressed = Path(_dir_mm10_variant_raw, 'mm10-variant-build.gvf.gz')
_fp_mm10_variant_raw = Path(_dir_mm10_variant_raw, 'mm10-variant-build.gvf')
_fp_mm10_variant_processed = Path(_dir_mm10_variant_proc, 'mm10-variant-build.tsv')


## In case these don't exist
try:
    Path(_dir_hg38_variant_raw).mkdir(parents=True, exist_ok=True)
    Path(_dir_hg38_variant_proc).mkdir(parents=True, exist_ok=True)
    Path(_dir_mm10_variant_raw).mkdir(parents=True, exist_ok=True)
    Path(_dir_mm10_variant_proc).mkdir(parents=True, exist_ok=True)
    Path(_dir_hg38_gene_raw).mkdir(parents=True, exist_ok=True)
    Path(_dir_hg38_gene_proc).mkdir(parents=True, exist_ok=True)
    Path(_dir_mm10_gene_raw).mkdir(parents=True, exist_ok=True)
    Path(_dir_mm10_gene_proc).mkdir(parents=True, exist_ok=True)

except OSError as e:
    logging.getLogger(__name__).error('Could not make data directories: %s', e)

