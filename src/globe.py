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
_dir_hg38_variant_meta = Path(_dir_data, 'variant/hg38/meta').as_posix()
_dir_hg38_variant_neo = Path(_dir_data, 'variant/hg38/neo4j').as_posix()
_dir_mm10_variant_raw = Path(_dir_data, 'variant/mm10/raw').as_posix()
_dir_mm10_variant_proc = Path(_dir_data, 'variant/mm10/processed').as_posix()
_dir_mm10_variant_meta = Path(_dir_data, 'variant/mm10/meta').as_posix()
_dir_mm10_variant_neo = Path(_dir_data, 'variant/mm10/neo4j').as_posix()

## Gene datasets
_dir_hg38_gene_raw = Path(_dir_data, 'gene/hg38/raw').as_posix()
_dir_hg38_gene_proc = Path(_dir_data, 'gene/hg38/processed').as_posix()
_dir_hg38_gene_neo = Path(_dir_data, 'gene/hg38/neo4j').as_posix()
_dir_mm10_gene_raw = Path(_dir_data, 'gene/mm10/raw').as_posix()
_dir_mm10_gene_proc = Path(_dir_data, 'gene/mm10/processed').as_posix()
_dir_mm10_gene_neo = Path(_dir_data, 'gene/mm10/neo4j').as_posix()

## Annotated variant datasets
_dir_hg38_annotated = Path(_dir_data, 'annotated/hg38').as_posix()
_dir_mm10_annotated = Path(_dir_data, 'annotated/mm10').as_posix()


## Output files ##

_fp_hg38_gene_compressed = Path(_dir_hg38_gene_raw, 'hg38-gene-build.gtf.gz')
_fp_hg38_gene_raw = Path(_dir_hg38_gene_raw, 'hg38-gene-build.gtf')
_fp_hg38_gene_processed = Path(_dir_hg38_gene_proc, 'hg38-gene-build.gtf')
_fp_hg38_gene_neo = Path(_dir_hg38_gene_neo, 'hg38-gene-build.tsv')

_fp_mm10_gene_compressed = Path(_dir_mm10_gene_raw, 'mm10-gene-build.gtf.gz')
_fp_mm10_gene_raw = Path(_dir_mm10_gene_raw, 'mm10-gene-build.gtf')
_fp_mm10_gene_processed = Path(_dir_mm10_gene_proc, 'mm10-gene-build.gtf')
_fp_mm10_gene_neo = Path(_dir_mm10_gene_neo, 'mm10-gene-build.tsv')

_fp_mm10_variant_compressed = Path(_dir_mm10_variant_raw, 'mm10-variant-build.gvf.gz')
_fp_mm10_variant_raw = Path(_dir_mm10_variant_raw, 'mm10-variant-build.gvf')
_fp_mm10_variant_processed = Path(_dir_mm10_variant_proc, 'mm10-variant-build.tsv')
_fp_mm10_variant_meta = Path(_dir_mm10_variant_meta, 'mm10-variant-build-metadata.tsv')
_fp_mm10_variant_neo = Path(_dir_mm10_variant_neo, 'mm10-variant-build-metadata.tsv')

#_fp_hg38_annotated = Path(_dir_hg38_annotated, 'hg38-annotated-variants.tsv')
#_fp_hg38_intergenic = Path(_dir_hg38_annotated, 'hg38-intergenic-variants.tsv')

_fp_mm10_annotated = Path(_dir_mm10_annotated, 'mm10-annotated-variants.tsv')
_fp_mm10_intergenic = Path(_dir_mm10_annotated, 'mm10-intergenic-variants.tsv')

_fp_hg38_annotation_stats = Path(_dir_data, 'hg38-annotation-stats.tsv')
_fp_mm10_annotation_stats = Path(_dir_data, 'mm10-annotation-stats.tsv')

## In case these don't exist
try:
    Path(_dir_hg38_variant_raw).mkdir(parents=True, exist_ok=True)
    Path(_dir_hg38_variant_proc).mkdir(parents=True, exist_ok=True)
    Path(_dir_mm10_variant_raw).mkdir(parents=True, exist_ok=True)
    Path(_dir_mm10_variant_proc).mkdir(parents=True, exist_ok=True)
    Path(_dir_hg38_variant_meta).mkdir(parents=True, exist_ok=True)
    Path(_dir_mm10_variant_meta).mkdir(parents=True, exist_ok=True)
    Path(_dir_hg38_variant_neo).mkdir(parents=True, exist_ok=True)
    Path(_dir_mm10_variant_neo).mkdir(parents=True, exist_ok=True)
    Path(_dir_hg38_gene_raw).mkdir(parents=True, exist_ok=True)
    Path(_dir_hg38_gene_proc).mkdir(parents=True, exist_ok=True)
    Path(_dir_hg38_gene_neo).mkdir(parents=True, exist_ok=True)
    Path(_dir_mm10_gene_raw).mkdir(parents=True, exist_ok=True)
    Path(_dir_mm10_gene_proc).mkdir(parents=True, exist_ok=True)
    Path(_dir_mm10_gene_neo).mkdir(parents=True, exist_ok=True)
    Path(_dir_hg38_annotated).mkdir(parents=True, exist_ok=True)
    Path(_dir_mm10_annotated).mkdir(parents=True, exist_ok=True)

except OSError as e:
    logging.getLogger(__name__).error('Could not make data directories: %s', e)

