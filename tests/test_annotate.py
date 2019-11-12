#!/usr/bin/env python
# -*- coding: utf8 -*-

## file: test_annotate.py
## desc: Test functions in annotate.py.

from dask.distributed import Client
from dask.distributed import LocalCluster
from pathlib import Path
import pytest
import tempfile as tf

from src import annotate

@pytest.fixture(scope='module')
def root_dir():
    return Path(__file__).resolve().parent.as_posix()

@pytest.fixture(scope='module')
def sample_variant_file(root_dir):
    return Path(root_dir, 'data/sample-hg38-processed-variant-effects.tsv').as_posix()

@pytest.fixture(scope='module')
def sample_gene_file(root_dir):
    return Path(root_dir, 'data/sample-hg38-processed-genes.tsv').as_posix()

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


def test_read_processed_variants(sample_variant_file):
    df = annotate._read_processed_variants(sample_variant_file)
    df = df.compute()

    assert 'rsid' in df.columns
    assert 'effect' in df.columns
    assert 'transcript' in df.columns

    assert len(df.index) == 5


def test_read_processed_genes(sample_gene_file):
    df = annotate._read_processed_genes(sample_gene_file)
    df = df.compute()

    assert 'chromosome' in df.columns
    assert 'start' in df.columns
    assert 'end' in df.columns
    assert 'transcript_id' in df.columns
    assert 'gene_id' in df.columns
    assert 'gene_name' in df.columns
    assert 'biotype' in df.columns

    assert len(df.index) == 6


def test_annotate_variants(sample_variant_file, sample_gene_file):
    vdf = annotate._read_processed_variants(sample_variant_file)
    gdf = annotate._read_processed_genes(sample_gene_file)
    df = annotate.annotate_variants(vdf, gdf)
    df = df.compute()

    assert df[df.rsid == 1308274876].variant_effect.iloc[0] == 'intergenic'
    assert df[df.rsid == 1486292194].variant_effect.isin(['intron_variant', 'missense_variant']).all()
    assert df[df.rsid == 1486292194].transcript.isin(['ENST00000381496', 'ENST00000634311', 'ENST00000280886', 'ENST00000434695']).all()
    assert (df[df.rsid == 1486292194].gene_id == 'ENSG00000151240').all()
    assert (df[df.rsid == 1486292194].gene_name == 'DIP2C').all()
    assert (df[df.rsid == 1486292194].gene_biotype == 'protein_coding').all()
    assert len(df.index) == 5


def test_isolate_intergenic_variants(sample_variant_file, sample_gene_file):
    vdf = annotate._read_processed_variants(sample_variant_file)
    gdf = annotate._read_processed_genes(sample_gene_file)
    df = annotate.annotate_variants(vdf, gdf)
    df = annotate.isolate_intergenic_variants(df)
    df = df.compute()

    assert (df.rsid == 1308274876).all()
    assert (df.variant_effect == 'intergenic').all()
    assert len(df.index) == 1


def test_isolate_intragenic_variants(sample_variant_file, sample_gene_file):
    vdf = annotate._read_processed_variants(sample_variant_file)
    gdf = annotate._read_processed_genes(sample_gene_file)
    df = annotate.annotate_variants(vdf, gdf)
    df = annotate.isolate_intragenic_variants(df)
    df = df.compute()

    assert (df.rsid == 1486292194).all()
    assert df.variant_effect.isin(['intron_variant', 'missense_variant']).all()
    assert len(df.index) == 2


def test_run_annotation_pipeline(client, sample_variant_file, sample_gene_file):
    vdf = annotate._read_processed_variants(sample_variant_file)
    gdf = annotate._read_processed_genes(sample_gene_file)
    d = annotate.run_annotation_pipeline(vdf, gdf)
    inter = d['intergenic'].compute()
    intra = d['intragenic'].compute()

    assert (inter.rsid == 1308274876).all()
    assert (inter.variant_effect == 'intergenic').all()
    assert len(inter.index) == 1

    assert (intra.rsid == 1486292194).all()
    assert intra.variant_effect.isin(['intron_variant', 'missense_variant']).all()
    assert len(intra.index) == 2

