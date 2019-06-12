#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: neoformat.py
## desc: Functions for formatting processing datasets for external resources e.g.,
##       loading into Postgres and/or Neo4j databases.

from dask.distributed import Client
from dask.distributed import Future
from dask.distributed import LocalCluster
from dask.distributed import get_client
from dask.distributed import secede
from dask_jobqueue import PBSCluster
from functools import partial
from pathlib import Path
from typing import List
import dask.dataframe as ddf
import logging
import numpy as np
import pandas as pd

from . import dfio
from . import globe
from . import log

logging.getLogger(__name__).addHandler(logging.NullHandler())


def write_neo4j_annotated_header(input: str, output: str) -> None:
    """

    :param input:
    :param output:
    :return:
    """

    df = pd.read_csv(input, sep='\t', nrows=0)

    ## Get rid of columns we don't really need
    df = df[['rsid', 'gene_id']]

    ## Add the type field
    df['type'] = ''

    ## Rename columns
    df = df.rename(columns={
        'rsid': ':START_ID',
        'gene_id': ':END_ID',
        'type': ':TYPE'
    })

    ## Save
    df.to_csv(output, sep='\t', index=False)


def format_neo4j_annotations(input: str, output: str) -> None:
    """
    Read a processed variant annotation file, format for batch loading in Neo4j, and
    store the results to disk.

    arguments
        input:  input filepath
        output: output filepath
    """

    ## Just assume we have enough memory to read the entire thing into memory (we
    ## should since this is usually run on an HPC cluster)
    df = pd.read_csv(input, sep='\t')

    ## Get rid of columns we don't really need
    df = df[['rsid', 'gene_id']]

    ## Add the intragenic role type
    df['type'] = 'INTRAGENIC'

    ## Rename columns
    #df = df.rename(columns={
    #    'rsid': ':START_ID',
    #    'gene_id': ':END_ID',
    #    'type': ':TYPE'
    #})

    ## Save
    df.to_csv(output, sep='\t', index=False, header=False)


def write_neo4j_gene_header(input: str, output: str) -> None:
    """

    :param input:
    :param output:
    :return:
    """

    df = pd.read_csv(input, sep='\t', nrows=0)

    ## Add build column
    df['build'] = 'build'

    ## Get rid of columns we don't really need
    df = df.drop(columns=['start', 'end', 'transcript_id'])

    ## Rename columns for neo4j batch loading
    df = df.rename(columns={'gene_id': 'gene_id:ID'})

    df.to_csv(output, sep='\t', index=False)


def format_neo4j_genes(input: str, output: str, build: str) -> None:
    """
    Read a processed gene file, format for batch loading in Neo4j, and store the
    results to disk.

    arguments
        input:  input filepath
        output: output filepath
        bulid:  genome build identifier
    """

    ## Just assume we have enough memory to read the entire thing into memory, these
    ## files aren't that big ~1GB so we should be fine.
    df = pd.read_csv(input, sep='\t')

    ## Get rid of columns we don't really need
    df = df.drop(columns=['start', 'end', 'transcript_id'])

    ## Remove duplicates which neo4j won't like
    df = df.drop_duplicates(subset=['gene_id'])

    ## Add the build
    df['build'] = build

    ## Save
    df.to_csv(output, sep='\t', index=False, header=False)


def write_neo4j_variant_header(input: str, output: str) -> None:
    """

    :param input:
    :param output:
    :return:
    """

    df = pd.read_csv(input, sep='\t', nrows=0)

    ## Add build column
    df['build'] = 'build'

    ## Rename columns for neo4j batch loading
    df = df.rename(columns={
        'rsid': 'rsid:ID',
        'start': 'start:int',
        'end': 'end:int',
        'maf': 'maf:float'
    })

    df.to_csv(output, sep='\t', index=False)


def format_neo4j_variants(input: str, output: str, build: str) -> None:
    """
    Read a processed variant file, format for batch loading in Neo4j, and store the
    results to disk.

    arguments
        input:  input filepath
        output: output filepath
        bulid:  genome build identifier
    """

    ## Just assume we have enough memory to read the entire thing into memory (we
    ## should since this is usually run on an HPC cluster)
    df = pd.read_csv(input, sep='\t')

    ## Add the build
    df['build'] = build

    ## Save
    df.to_csv(output, sep='\t', index=False, header=False)


def run_hg38_variant_formatting(
    client: Client,
    indir: str = globe._dir_hg38_variant_meta,
    outdir: str = globe._dir_hg38_neo_variant_meta,
    header: str = globe._fp_hg38_neo_variant_meta_head,
    **kwargs
) -> Future:
    """
    28 min.
    """

    futures = []
    done_header = False

    for fl in Path(indir).iterdir():

        output = Path(outdir, fl.name)

        ## Write the neo4j header only once
        if not done_header:
            futures.append(client.submit(write_neo4j_variant_header, fl, header))

            done_header = True

        futures.append(client.submit(format_neo4j_variants, fl, output, 'hg38'))

    return futures


def run_mm10_variant_formatting(
    client: Client,
    input: str = globe._fp_mm10_variant_meta,
    output: str = globe._fp_mm10_neo_variant_meta,
    header: str = globe._fp_mm10_neo_variant_meta_head,
    **kwargs
) -> Future:
    """
    28 min.
    """

    return [
        client.submit(write_neo4j_variant_header, input, header),
        client.submit(format_neo4j_variants, input, output, 'mm10')
    ]


def run_hg38_gene_formatting(
    client: Client,
    input: str = globe._fp_hg38_gene_meta,
    output: str = globe._fp_hg38_neo_gene,
    header: str = globe._fp_hg38_neo_gene_head,
    **kwargs
) -> Future:
    """
    28 min.
    """

    return [
        client.submit(write_neo4j_gene_header, input, header),
        client.submit(format_neo4j_genes, input, output, 'hg38')
    ]


def run_mm10_gene_formatting(
    client: Client,
    input: str = globe._fp_mm10_gene_meta,
    output: str = globe._fp_mm10_neo_gene,
    header: str = globe._fp_mm10_neo_gene_head,
    **kwargs
) -> Future:
    """
    28 min.
    """

    return [
        client.submit(write_neo4j_gene_header, input, header),
        client.submit(format_neo4j_genes, input, output, 'mm10')
    ]


def run_hg38_annotation_formatting(
    client: Client,
    indir: str = globe._dir_hg38_annotated,
    outdir: str = globe._dir_hg38_neo_variant_rel,
    header: str = globe._fp_hg38_neo_variant_rel_head,
    **kwargs
) -> Future:
    """
    28 min.
    """

    futures = []
    done_header = False


    for fl in Path(indir).iterdir():

        ## Skip intergenic annotations
        if 'intergenic' in fl.as_posix():
            continue

        output = Path(outdir, fl.name)

        ## Write the neo4j header only once
        if not done_header:
            futures.append(client.submit(write_neo4j_annotated_header, fl, header))

            done_header = True

        futures.append(client.submit(format_neo4j_annotations, fl, output))

    return futures


def run_mm10_annotation_formatting(
    client: Client,
    input: str = globe._fp_mm10_annotated,
    output: str = globe._fp_mm10_neo_variant_rel,
    header: str = globe._fp_mm10_neo_variant_rel_head,
    **kwargs
) -> Future:
    """
    28 min.
    """

    return [
        client.submit(write_neo4j_annotated_header, input, header),
        client.submit(format_neo4j_annotations, input, output)
    ]


if __name__ == '__main__':

    log._initialize_logging(verbose=True)

    #client = Client(LocalCluster(
    #    n_workers=18,
    #    processes=True
    #))
    cluster = PBSCluster(
        name='variant-etl',
        queue='batch',
        interface='ib0',
        cores=1,
        processes=1,
        memory='50GB',
        walltime='03:00:00',
        job_extra=['-e logs', '-o logs'],
        env_extra=['cd $PBS_O_WORKDIR']
    )

    cluster.adapt(minimum=10, maximum=45)

    client = Client(cluster)

    init_logging_partial = partial(log._initialize_logging, verbose=True)

    ## Init logging on each worker
    client.register_worker_callbacks(setup=init_logging_partial)

    hg38_variants = run_hg38_variant_formatting(client)
    mm10_variants = run_mm10_variant_formatting(client)
    hg38_genes = run_hg38_gene_formatting(client)
    mm10_genes = run_mm10_gene_formatting(client)
    hg38_annotations = run_hg38_annotation_formatting(client)
    mm10_annotations = run_mm10_annotation_formatting(client)

    client.gather(hg38_variants)
    client.gather(mm10_variants)
    client.gather(hg38_genes)
    client.gather(mm10_genes)
    client.gather(hg38_annotations)
    client.gather(mm10_annotations)

    log._logger.info('Done')

    client.close()

