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

## Data directories
_dir_data = 'data/'
_dir_stats = Path(_dir_data, 'stats').as_posix()

## Variant datasets
_dir_variant = Path(_dir_data, 'variants').as_posix()

_dir_hg38_variant = Path(_dir_variant, 'hg38').as_posix()
_dir_hg38_variant_raw = Path(_dir_hg38_variant, 'raw').as_posix()
_dir_hg38_variant_effect = Path(_dir_hg38_variant, 'effects').as_posix()
_dir_hg38_variant_meta = Path(_dir_hg38_variant, 'meta').as_posix()

_dir_mm10_variant = Path(_dir_variant, 'mm10').as_posix()
_dir_mm10_variant_raw = Path(_dir_mm10_variant, 'raw').as_posix()
_dir_mm10_variant_effect = Path(_dir_mm10_variant, 'effects').as_posix()
_dir_mm10_variant_meta = Path(_dir_mm10_variant, 'meta').as_posix()

## Gene datasets
_dir_gene = Path(_dir_data, 'genes').as_posix()

_dir_hg38_gene = Path(_dir_gene, 'hg38').as_posix()
_dir_hg38_gene_raw = Path(_dir_hg38_gene, 'raw').as_posix()
_dir_hg38_gene_meta = Path(_dir_hg38_gene, 'meta').as_posix()

_dir_mm10_gene = Path(_dir_gene, 'mm10').as_posix()
_dir_mm10_gene_raw = Path(_dir_mm10_gene, 'raw').as_posix()
_dir_mm10_gene_meta = Path(_dir_mm10_gene, 'meta').as_posix()

## Annotated variant datasets
_dir_hg38_annotated = Path(_dir_hg38_variant, 'annotated').as_posix()
_dir_hg38_annotated_inter = Path(_dir_hg38_annotated, 'intergenic').as_posix()
_dir_hg38_annotated_intra = Path(_dir_hg38_annotated, 'intragenic').as_posix()

_dir_mm10_annotated = Path(_dir_mm10_variant, 'annotated').as_posix()

## Neo4j formatted datasets
_dir_hg38_neo_variant = Path(_dir_hg38_variant, 'neo4j').as_posix()
_dir_hg38_neo_gene = Path(_dir_hg38_gene, 'neo4j').as_posix()
_dir_hg38_neo_variant_meta = Path(_dir_hg38_neo_variant, 'meta').as_posix()
_dir_hg38_neo_variant_rel = Path(_dir_hg38_neo_variant, 'relations').as_posix()

_dir_mm10_neo_variant = Path(_dir_mm10_variant, 'neo4j').as_posix()
_dir_mm10_neo_gene = Path(_dir_mm10_gene, 'neo4j').as_posix()
_dir_mm10_neo_variant_meta = Path(_dir_mm10_neo_variant, 'meta').as_posix()
_dir_mm10_neo_variant_rel = Path(_dir_mm10_neo_variant, 'relations').as_posix()


## Output files ##

_fp_hg38_gene_compressed = Path(_dir_hg38_gene_raw, 'hg38-gene-build.gtf.gz').as_posix()
_fp_hg38_gene_raw = Path(_dir_hg38_gene_raw, 'hg38-gene-build.gtf').as_posix()
_fp_hg38_gene_meta = Path(_dir_hg38_gene_meta, 'hg38-gene-build.gtf').as_posix()

_fp_mm10_gene_compressed = Path(_dir_mm10_gene_raw, 'mm10-gene-build.gtf.gz').as_posix()
_fp_mm10_gene_raw = Path(_dir_mm10_gene_raw, 'mm10-gene-build.gtf').as_posix()
_fp_mm10_gene_meta = Path(_dir_mm10_gene_meta, 'mm10-gene-build.gtf').as_posix()

_fp_mm10_variant_compressed = Path(
    _dir_mm10_variant_raw, 'mm10-variant-build.gvf.gz'
).as_posix()
_fp_mm10_variant_raw = Path(_dir_mm10_variant_raw, 'mm10-variant-build.gvf').as_posix()
_fp_mm10_variant_effect = Path(
    _dir_mm10_variant_effect, 'mm10-variant-build.tsv'
).as_posix()
_fp_mm10_variant_meta = Path(
    _dir_mm10_variant_meta, 'mm10-variant-build-metadata.tsv'
).as_posix()

_fp_mm10_intergenic = Path(_dir_mm10_annotated, 'mm10-intergenic-variants.tsv').as_posix()
_fp_mm10_intragenic = Path(_dir_mm10_annotated, 'mm10-intragenic-variants.tsv').as_posix()

_fp_hg38_annotation_stats = Path(
    _dir_stats, 'hg38-variant-annotation-stats.tsv'
).as_posix()
_fp_mm10_annotation_stats = Path(
    _dir_stats, 'mm10-variant-annotation-stats.tsv'
).as_posix()

## Neo4j outputs
_fp_hg38_neo_gene = Path(_dir_hg38_neo_gene, 'hg38-gene-build.tsv').as_posix()
_fp_hg38_neo_gene_head = Path(_dir_hg38_neo_gene, 'hg38-gene-head.tsv').as_posix()
_fp_hg38_neo_variant_meta_head = Path(
    _dir_hg38_neo_variant_meta, 'hg38-variant-head.tsv'
).as_posix()
_fp_hg38_neo_variant_rel_head = Path(
    _dir_hg38_neo_variant_rel, 'hg38-variant-relations-head.tsv'
).as_posix()

_fp_mm10_neo_gene = Path(_dir_mm10_neo_gene, 'mm10-gene-build.tsv').as_posix()
_fp_mm10_neo_gene_head = Path(_dir_mm10_neo_gene, 'mm10-gene-head.tsv').as_posix()
_fp_mm10_neo_variant_meta = Path(
    _dir_mm10_neo_variant_meta, 'mm10-variant-build-metadata.tsv'
).as_posix()
_fp_mm10_neo_variant_meta_head = Path(
    _dir_mm10_neo_variant_meta, 'mm10-variant-head.tsv'
).as_posix()
_fp_mm10_neo_variant_rel = Path(
    _dir_mm10_neo_variant_rel, 'mm10-variant-relations.tsv'
).as_posix()
_fp_mm10_neo_variant_rel_head = Path(
    _dir_mm10_neo_variant_rel, 'mm10-variant-relations-head.tsv'
).as_posix()

## In case these don't exist
try:
    Path(_dir_hg38_variant_raw).mkdir(parents=True, exist_ok=True)
    Path(_dir_hg38_variant_effect).mkdir(parents=True, exist_ok=True)
    Path(_dir_mm10_variant_raw).mkdir(parents=True, exist_ok=True)
    Path(_dir_mm10_variant_effect).mkdir(parents=True, exist_ok=True)
    Path(_dir_hg38_variant_meta).mkdir(parents=True, exist_ok=True)
    Path(_dir_mm10_variant_meta).mkdir(parents=True, exist_ok=True)
    Path(_dir_hg38_gene_raw).mkdir(parents=True, exist_ok=True)
    Path(_dir_hg38_gene_meta).mkdir(parents=True, exist_ok=True)
    Path(_dir_mm10_gene_raw).mkdir(parents=True, exist_ok=True)
    Path(_dir_mm10_gene_meta).mkdir(parents=True, exist_ok=True)
    Path(_dir_hg38_annotated).mkdir(parents=True, exist_ok=True)
    Path(_dir_hg38_annotated_inter).mkdir(parents=True, exist_ok=True)
    Path(_dir_hg38_annotated_intra).mkdir(parents=True, exist_ok=True)
    Path(_dir_mm10_annotated).mkdir(parents=True, exist_ok=True)

    Path(_dir_hg38_neo_gene).mkdir(parents=True, exist_ok=True)
    Path(_dir_hg38_neo_variant_meta).mkdir(parents=True, exist_ok=True)
    Path(_dir_hg38_neo_variant_rel).mkdir(parents=True, exist_ok=True)
    Path(_dir_mm10_neo_gene).mkdir(parents=True, exist_ok=True)
    Path(_dir_mm10_neo_variant_meta).mkdir(parents=True, exist_ok=True)
    Path(_dir_mm10_neo_variant_rel).mkdir(parents=True, exist_ok=True)

except OSError as e:
    logging.getLogger(__name__).error('Could not make data directories: %s', e)

