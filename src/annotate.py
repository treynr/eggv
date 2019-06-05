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

    ## Merge variant and gene frames based on the Ensembl transcript ID
    df = vdf.merge(gdf, how='inner', left_on='transcript', right_on='transcript_id')

    ## Rename some columns
    df = df.rename(columns={'effect': 'variant_effect', 'biotype': 'gene_biotype'})

    return df[['rsid', 'gene_id', 'gene_name', 'variant_effect', 'gene_biotype']]


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


def save_distributed_variants2(client: Client, df: ddf.DataFrame, output: str) -> str:
    """
    """

    ## Convert the distributed dataframe into delayed objects, per partition
    delayed_df = df.to_delayed()

    ## Convert to a list of futures
    df_futures = client.compute(delayed_df)
    last_future = None

    for fut in df_futures:
        if last_future:
            last_future = client.submit(
                write_dataframe, fut, output, append=True, depends=last_future
            )
        else:
            last_future = client.submit(write_dataframe, fut, output)

    return last_future


def save_distributed_variants3(dfs) -> str:
    """
    """

    client = get_client()
    ## Convert the distributed dataframe into delayed objects, per partition
    #delayed_df = dfs.to_delayed()

    ## Convert to a list of futures
    #df_futures = client.compute(delayed_df)
    #df_futures = client.compute(dfs)
    temp = tf.mkdtemp(dir=globe._dir_data)
    futures = []

    #for i, fut in enumerate(df_futures):
    for i, fut in enumerate(dfs):
        #def write_dataframe(df, output, append=False, **kwargs):
        #print('getting result')
        #print(fut.result())
        output = Path(temp, f'{i}.tsv')
        futures.append(client.submit(write_dataframe, fut, output))

    secede()

    client.gather(futures)

    return temp
    #return last_future


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


def run_hg38_variant_processing(client: Client) -> Future:
    """
    """

    futures = []

    ## Dask dataframes can use globs to read all the given files at once
    #variant_fp = Path(globe._dir_hg38_variant_raw, '*.vcf')
    variant_fp = Path(globe._dir_hg38_variant_raw, 'chromosome-2.vcf')

    raw_df = read_gvf_file(variant_fp)
    processed_df = process_gvf(raw_df)

    ## Persist the processed data form on the workers
    processed_df = client.persist(processed_df)

    ## Now separate and store processed variant by chromosome
    for chrom in globe._var_human_chromosomes:
        #for chrom in [2]:

        ## Final output filepath
        output = Path(globe._dir_hg38_variant_proc, f'chromosome-{chrom}.tsv')

        #filtered_df = client.submit(isolate_chromosome_variants, processed_df, chrom)

        filtered_df = isolate_chromosome_variants(processed_df, chrom)

        #filtered_df = isolate_chromosome_variants(processed_df, chrom)
        filtered_df = client.scatter(filtered_df, broadcast=True)

        #processed_df = client.scatter(processed_df, broadcast=True)
        #filtered_df = client.submit(isolate_chromosome_variants, processed_df, chrom)

        #print(filtered_df.result().head())
        #print(filtered_df.head())
        saved_fp = client.submit(save_distributed_variants, filtered_df)
        consolidated = client.submit(consolidate_saved_variants, saved_fp, output)
        """
        """

        futures.append(consolidated)
        #break

    return futures


def run_hg38_variant_processing2(client: Client) -> Future:
    """
    28 min.
    """

    futures = []


    ## Now separate and store processed variant by chromosome
    for chrom in globe._var_human_chromosomes:
        #for chrom in ['1']:

        ## Dask dataframes can use globs to read all the given files at once
        variant_fp = Path(globe._dir_hg38_variant_raw, f'chromosome-{chrom}.vcf')
        #variant_fp = Path(globe._dir_hg38_variant_raw, 'chromosome-1.vcf')

        raw_df = read_gvf_file(variant_fp)
        processed_df = process_gvf(raw_df)

        ## Persist the processed data form on the workers
        processed_df = client.persist(processed_df)

        ## Final output filepath
        output = Path(globe._dir_hg38_variant_proc, f'chromosome-{chrom}.tsv')

        #filtered_df = client.submit(isolate_chromosome_variants, processed_df, chrom)

        log._logger.info(f'Starting work on chromosome {chrom}')
        #filtered_df = isolate_chromosome_variants(processed_df, chrom)

        ## 1
        #filtered_df = client.scatter(filtered_df, broadcast=True)
        filtered_df = client.scatter(processed_df, broadcast=True)
        saved_fp = client.submit(save_distributed_variants, filtered_df)
        consolidated = client.submit(consolidate_saved_variants, saved_fp, output)

        ## 2
        #saved_future = save_distributed_variants2(client, filtered_df, output)
        #futures.append(saved_future)

        ## 3
        #filtered_df = client.scatter(filtered_df, broadcast=True)
        #saved_fp = client.submit(save_distributed_variants3, filtered_df)
        #saved_fp = client.submit(
        #    save_distributed_variants3, client.scatter(filtered_df.to_delayed())
        #)
        #consolidated = client.submit(consolidate_saved_variants, saved_fp, output)

        futures.append(consolidated)

        #saved_df = save_distributed_variants2(client, filtered_df, output)
        #futures.append(saved_df)
        #futures.append(filtered_df)

        """
        #filtered_df = isolate_chromosome_variants(processed_df, chrom)
        #filtered_df = client.scatter(filtered_df, broadcast=True)

        #processed_df = client.scatter(processed_df, broadcast=True)
        #filtered_df = client.submit(isolate_chromosome_variants, processed_df, chrom)

        #print(filtered_df.result().head())
        #print(filtered_df.head())
        saved_fp = client.submit(save_distributed_variants, filtered_df)
        """

        #break

    return futures


def run_hg38_variant_processing3(client: Client) -> Future:
    """
    """

    futures = []

    ## Dask dataframes can use globs to read all the given files at once
    #variant_fp = Path(globe._dir_hg38_variant_raw, '*.vcf')
    variant_fp = Path(globe._dir_hg38_variant_raw, 'chromosome-2.vcf')

    raw_df = read_gvf_file(variant_fp)
    processed_df = process_gvf(raw_df)

    ## Persist the processed data form on the workers
    processed_df = client.persist(processed_df)

    ## Now separate and store processed variant by chromosome
    #for chrom in globe._var_human_chromosomes:
    for chrom in [2]:

        ## Final output filepath
        output = Path(globe._dir_hg38_variant_proc, f'chromosome-{chrom}.tsv')

        #filtered_df = client.submit(isolate_chromosome_variants, processed_df, chrom)

        filtered_df = isolate_chromosome_variants(processed_df, chrom)

        df_delayed = filtered_df.to_delayed()
        df_futures = client.compute(df_delayed)
        saved_df = client.submit(save_distributed_variants3, df_futures)
        #saved_df = save_distributed_variants3(client, filtered_df, output)

        futures.append(saved_df)
        #break

    return futures


if __name__ == '__main__':

    log._initialize_logging(verbose=True)

    #client = Client(LocalCluster(
    #    n_workers=12,
    #    processes=True
    #))
    cluster = PBSCluster(
        name='variant-etl',
        queue='batch',
        interface='ib0',
        #cores=2,
        #processes=2,
        #memory='80GB',
        cores=1,
        processes=1,
        memory='45GB',
        walltime='02:00:00',
        job_extra=['-e logs', '-o logs'],
        env_extra=['cd $PBS_O_WORKDIR']
    )

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

