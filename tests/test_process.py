#!/usr/bin/env python
# -*- coding: utf8 -*-

## file: test_process.py
## desc: Test functions in process.py.

from dask.distributed import Client
from dask.distributed import LocalCluster
from pathlib import Path
import pytest
import tempfile as tf

from src import process

@pytest.fixture(scope='module')
def root_dir():
    return Path(__file__).resolve().parent.as_posix()

@pytest.fixture(scope='module')
def sample_gvf_file(root_dir):
    return Path(root_dir, 'data/sample-hg38-chromosome-10.gvf').as_posix()

@pytest.fixture(scope='module')
def sample_gtf_file(root_dir):
    return Path(root_dir, 'data/sample-hg38-gene-build.gtf').as_posix()

@pytest.fixture(scope='module')
def client():
    return Client(LocalCluster(
        n_workers=1,
        processes=True,
        local_directory=tf.gettempdir()
    ))

@pytest.fixture(scope='module', autouse=True)
def cleanup(request, client):
    def close_client():
        client.close()

    request.addfinalizer(close_client)


def test_read_gvf_file(sample_gvf_file):
    df = process._read_gvf_file(sample_gvf_file)

    assert 'seqid' in df.columns
    assert 'source' in df.columns
    assert 'type' in df.columns
    assert 'start' in df.columns
    assert 'end' in df.columns
    assert 'score' in df.columns
    assert 'strand' in df.columns
    assert 'phase' in df.columns
    assert 'attr' in df.columns

    assert len(df.index) == 2


def test_process_gvf(sample_gvf_file):
    df = process._read_gvf_file(sample_gvf_file)
    df = process._process_gvf(df)
    df = df.compute()

    assert (df.chromosome == 'chr10').all()
    assert (df.rsid == 1308274876).any()
    assert (df.rsid == 1486292194).any()
    assert df[df.rsid == 1308274876].observed.iloc[0] == 'C,G'
    assert df[df.rsid == 1486292194].observed.iloc[0] == 'G,T'
    assert (df.maf == 0.0).all()
    assert df[df.rsid == 1308274876].effect.iloc[0] == 'intergenic'
    assert df[df.rsid == 1486292194].effect.isin(['intron_variant', 'missense_variant']).all()
    assert df[df.rsid == 1486292194].transcript.isin(['ENST00000381496', 'ENST00000634311', 'ENST00000280886', 'ENST00000434695']).all()
    assert len(df.index) == 5


def test_process_variants(client, sample_gvf_file):
    df = process._process_variants(sample_gvf_file)
    df = df.compute()

    assert (df.chromosome == 'chr10').all()
    assert (df.rsid == 1308274876).any()
    assert (df.rsid == 1486292194).any()
    assert df[df.rsid == 1308274876].observed.iloc[0] == 'C,G'
    assert df[df.rsid == 1486292194].observed.iloc[0] == 'G,T'
    assert (df.maf == 0.0).all()
    assert df[df.rsid == 1308274876].effect.iloc[0] == 'intergenic'
    assert df[df.rsid == 1486292194].effect.isin(['intron_variant', 'missense_variant']).all()
    assert df[df.rsid == 1486292194].transcript.isin(['ENST00000381496', 'ENST00000634311', 'ENST00000280886', 'ENST00000434695']).all()
    assert len(df.index) == 5


def test_isolate_variant_effects(client, sample_gvf_file):
    df = process._process_variants(sample_gvf_file)
    df = process.isolate_variant_effects(df, npartitions=1)
    df = df.compute()

    assert (df.rsid == 1308274876).any()
    assert (df.rsid == 1486292194).any()
    assert df[df.rsid == 1308274876].effect.iloc[0] == 'intergenic'
    assert df[df.rsid == 1486292194].effect.isin(['intron_variant', 'missense_variant']).all()
    assert df[df.rsid == 1308274876].transcript.iloc[0] == ''
    assert df[df.rsid == 1486292194].transcript.isin(['ENST00000381496', 'ENST00000634311', 'ENST00000280886', 'ENST00000434695']).all()
    assert len(df.index) == 5


def test_isolate_variant_metadata(client, sample_gvf_file):
    df = process._process_variants(sample_gvf_file)
    df = process.isolate_variant_metadata(df, npartitions=1)
    df = df.compute()

    assert (df.chromosome == 'chr10').all()
    assert (df.rsid == 1308274876).any()
    assert (df.rsid == 1486292194).any()
    assert df[df.rsid == 1308274876].observed.iloc[0] == 'C,G'
    assert df[df.rsid == 1486292194].observed.iloc[0] == 'G,T'
    assert (df.maf == 0.0).all()
    assert len(df.index) == 2


def test_run_variant_processing_pipeline(client, sample_gvf_file):
    d = process.run_variant_processing_pipeline(sample_gvf_file)
    effects = d['effects'].compute()
    meta = d['metadata'].compute()

    assert (effects.rsid == 1308274876).any()
    assert (effects.rsid == 1486292194).any()
    assert effects[effects.rsid == 1308274876].effect.iloc[0] == 'intergenic'
    assert effects[effects.rsid == 1486292194].effect.isin(['intron_variant', 'missense_variant']).all()
    assert effects[effects.rsid == 1308274876].transcript.iloc[0] == ''
    assert effects[effects.rsid == 1486292194].transcript.isin(['ENST00000381496', 'ENST00000634311', 'ENST00000280886', 'ENST00000434695']).all()
    assert len(effects.index) == 5

    assert (meta.chromosome == 'chr10').all()
    assert (meta.rsid == 1308274876).any()
    assert (meta.rsid == 1486292194).any()
    assert meta[meta.rsid == 1308274876].observed.iloc[0] == 'C,G'
    assert meta[meta.rsid == 1486292194].observed.iloc[0] == 'G,T'
    assert (meta.maf == 0.0).all()
    assert len(meta.index) == 2


def test_read_gtf_file(sample_gtf_file):
    df = process._read_gtf_file(sample_gtf_file)

    assert 'seqname' in df.columns
    assert 'source' in df.columns
    assert 'feature' in df.columns
    assert 'start' in df.columns
    assert 'end' in df.columns
    assert 'score' in df.columns
    assert 'strand' in df.columns
    assert 'frame' in df.columns
    assert 'attr' in df.columns

    assert len(df.index) == 269


def test_process_gtf(sample_gtf_file):
    df = process._read_gtf_file(sample_gtf_file)
    df = process._process_gtf(df)
    df = df.compute()

    assert (df.chromosome == 'chr10').all()
    assert df.gene_id.isin(['ENSG00000151240', 'ENSG00000201861']).all()
    assert df.transcript_id.isin(['ENST00000381496', 'ENST00000280886', 'ENST00000634311', 'ENST00000434695', 'ENST00000421992', 'ENST00000364991']).all()
    assert (df[df.gene_id == 'ENSG00000151240'].biotype == 'protein_coding').all()
    assert (df[df.gene_id == 'ENSG00000201861'].biotype == 'rRNA_pseudogene').all()
    assert (df[df.gene_id == 'ENSG00000151240'].gene_name == 'DIP2C').all()
    assert (df[df.gene_id == 'ENSG00000201861'].gene_name == 'RNA5SP298').all()

    assert len(df.index) == 6


def test_process_genes(client, sample_gtf_file):
    df = process._process_genes(sample_gtf_file)
    df = df.compute()

    assert (df.chromosome == 'chr10').all()
    assert df.gene_id.isin(['ENSG00000151240', 'ENSG00000201861']).all()
    assert df.transcript_id.isin(['ENST00000381496', 'ENST00000280886', 'ENST00000634311', 'ENST00000434695', 'ENST00000421992', 'ENST00000364991']).all()
    assert (df[df.gene_id == 'ENSG00000151240'].biotype == 'protein_coding').all()
    assert (df[df.gene_id == 'ENSG00000201861'].biotype == 'rRNA_pseudogene').all()
    assert (df[df.gene_id == 'ENSG00000151240'].gene_name == 'DIP2C').all()
    assert (df[df.gene_id == 'ENSG00000201861'].gene_name == 'RNA5SP298').all()

    assert len(df.index) == 6


def test_run_gene_processing_pipeline(client, sample_gtf_file):
    df = process.run_gene_processing_pipeline(sample_gtf_file)
    df = df.compute()

    assert (df.chromosome == 'chr10').all()
    assert df.gene_id.isin(['ENSG00000151240', 'ENSG00000201861']).all()
    assert df.transcript_id.isin(['ENST00000381496', 'ENST00000280886', 'ENST00000634311', 'ENST00000434695', 'ENST00000421992', 'ENST00000364991']).all()
    assert (df[df.gene_id == 'ENSG00000151240'].biotype == 'protein_coding').all()
    assert (df[df.gene_id == 'ENSG00000201861'].biotype == 'rRNA_pseudogene').all()
    assert (df[df.gene_id == 'ENSG00000151240'].gene_name == 'DIP2C').all()
    assert (df[df.gene_id == 'ENSG00000201861'].gene_name == 'RNA5SP298').all()

    assert len(df.index) == 6

