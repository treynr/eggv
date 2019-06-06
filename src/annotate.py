#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: annotate.py
## desc: Functions for annotating gene variants based on the variant effects provided
##       by Ensembl.

from dask.distributed import Client
from dask.distributed import Future
from dask.distributed import get_client
from dask.distributed import secede
from dask.distributed import LocalCluster
from dask_jobqueue import PBSCluster
from functools import partial
from pathlib import Path
from typing import Dict
import dask.dataframe as ddf
import logging
import pandas as pd
import shutil
import tempfile as tf

from . import globe
from . import log

logging.getLogger(__name__).addHandler(logging.NullHandler())


def read_processed_variants(fp: str) -> ddf.DataFrame:
    """
    Read and parse a pre-processed file containing Ensembl variation build data. Should
    be in the format created by the process.py module.

    :param fp:
    :return:
    """

    #header = [
    #    'chromosome', 'rsid', 'start', 'end', 'observed', 'maf', 'effect', 'transcript'
    #]

    return ddf.read_csv(fp, sep='\t', comment='#')


def read_processed_genes(fp: str) -> ddf.DataFrame:
    """
    Read and parse a pre-processed file containing Ensembl gene build data. Should
    be in the format created by the process.py module.

    :param fp:
    :return:
    """

    #header = [
    #    'chromosome', 'rsid', 'start', 'end', 'observed', 'maf', 'effect', 'transcript'
    #]

    return ddf.read_csv(fp, sep='\t', comment='#')


def annotate_variants(vdf, gdf) -> ddf.DataFrame:
    """
    Annotate variants to genes based on variant effects provided by Ensembl.

    :param vdf:
    :param gdf:
    :return:
    """

    ## Merge variant and gene frames based on the Ensembl transcript ID. Normally we
    ## use an inner merge but use a left instead to collect mapping stats.
    df = vdf.merge(gdf, how='left', left_on='transcript', right_on='transcript_id')

    ## Rename some columns
    df = df.rename(columns={'effect': 'variant_effect', 'biotype': 'gene_biotype'})

    return df[[
        'chromosome', 'rsid', 'gene_id', 'gene_name', 'variant_effect', 'gene_biotype'
    ]]


def isolate_intergenic_variants(df) -> ddf.DataFrame:
    """
    Return a dataframe containing only intergenic variants.

    :param vdf:
    :param gdf:
    :return:
    """

    return df[df.effect == 'intergenic']


def collect_annotation_stats(df) -> ddf.DataFrame:
    """

    :param df:
    :return:
    """

    ## Conditions
    is_intergenic = df.variant_effect != 'intergenic'
    is_mapped = df.gene_id.notnull()

    ## Intragenic variants successfully mapped to genes
    intra_mapped = df[is_intergenic & is_mapped].groupby('chromosome').count()

    print(intra_mapped)


def save_distributed_variants(df: ddf.DataFrame) -> str:
    """
    Write variants to a file. Assumes these variants have been distributed using dask
    dataframes. Writes each dataframe partition to a temp folder.
    """

    temp = tf.mkdtemp(dir=globe._dir_data)

    df.to_csv(temp, sep='\t', index=False, na_rep='NA')

    return temp


def write_dataframe(df, output, append=False, **kwargs):

    if append:
        df.to_csv(output, sep='\t', index=False, header=False, mode='a')
    else:
        df.compute().to_csv(output, sep='\t', index=False)

    return True


def consolidate_saved_variants(input: str, output: str):
    """
    Read variant files separated due to dask dataframe partitions and concatenate them
    into a single file.

    :param input:
    :param output:
    :return:
    """

    log._logger.info(f'Finalizing {output}')

    first = True

    with open(output, 'w') as ofl:
        for ifp in Path(input).iterdir():
            with open(ifp, 'r') as ifl:

                ## If this is the first file being read then we include the header
                if first:
                    ofl.write(ifl.read())

                    first = False

                ## Otherwise skip the header so it isn't repeated throughout
                else:
                    next(ifl)

                    ofl.write(ifl.read())

    ## Assume the input directory is a temp one and remove it since it's no longer needed
    shutil.rmtree(input)

    return True


def run_hg38_annotation(
    client: Client,
    indir: str = globe._dir_hg38_variant_proc
):
    """

    :param client:
    :return:
    """

    variant_fp = Path(indir, 'chromosome-22.tsv')

    df = read_processed_variants(variant_fp)
    annotated_df = ann


if __name__ == '__main__':

    log._initialize_logging(verbose=True)

    client = Client(LocalCluster(
        n_workers=18,
        processes=True
    ))
    #cluster = PBSCluster(
    #    name='variant-etl',
    #    queue='batch',
    #    interface='ib0',
    #    #cores=2,
    #    #processes=2,
    #    #memory='80GB',
    #    cores=1,
    #    processes=1,
    #    memory='45GB',
    #    walltime='02:00:00',
    #    job_extra=['-e logs', '-o logs'],
    #    env_extra=['cd $PBS_O_WORKDIR']
    #)

    cluster.adapt(minimum=10, maximum=38)

    client = Client(cluster)
    """
    """

    init_logging_partial = partial(log._initialize_logging, verbose=True)

    client.register_worker_callbacks(setup=init_logging_partial)

    ## Init logging on each worker
    #client.run(log._initialize_logging, verbose=True)

    hg38_futures = run_hg38_variant_processing2(client)
    #hg38_futures = run_hg38_variant_processing3(client)
    #df = read_gvf_file('data/variant/hg38/raw/chromosome-21.vcf')
    #df = process_gvf(df)

    #log._logger.info('Processing and saving variants')

    #tempdir = save_variants(df)

    #log._logger.info('Consolidating variants')

    #consolidate_saved_variants(tempdir, 'data/variant/hg38/processed/chromosome-21.vcf')
    client.gather(hg38_futures)

    log._logger.info('Done')
    #human_futures = run_human_feature_processing(client)
    #mouse_futures = run_mouse_feature_processing(client)

    #client.gather([human_futures, mouse_futures])

    client.close()

