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
from typing import List
import dask.dataframe as ddf
import logging
import numpy as np
import pandas as pd
import shutil
import tempfile as tf

from . import dfio
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

    return ddf.read_csv(fp, sep='\t', comment='#', dtype={'transcript': 'object'})


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
    df = vdf.merge(
        gdf,
        how='left',
        left_on='transcript',
        right_on='transcript_id',
        suffixes=('_l', '_r')
    )

    ## Rename some columns
    df = df.rename(columns={
        'effect': 'variant_effect',
        'biotype': 'gene_biotype',
        'chromosome_l': 'chromosome'
    })

    ## Eliminate possible duplicates (bug in dask, this doesn't seem to work)
    #df = df.drop_duplicates(subset=['rsid', 'variant_effect', 'gene_id'], keep='first')

    return df[[
        'chromosome',
        'rsid',
        'variant_effect',
        'transcript',
        'gene_id',
        'gene_name',
        'gene_biotype'
    ]]


def isolate_intergenic_variants(df) -> ddf.DataFrame:
    """
    Return a dataframe containing only intergenic variants.

    :param vdf:
    :param gdf:
    :return:
    """

    keep = ['chromosome', 'rsid', 'variant_effect']

    return df[df.variant_effect == 'intergenic'].loc[:, keep]


def isolate_annotated_variants(df) -> ddf.DataFrame:
    """
    Return a dataframe containing only intergenic variants.

    :param vdf:
    :param gdf:
    :return:
    """

    keep = [
        'chromosome', 'rsid', 'variant_effect', 'gene_id', 'gene_name', 'gene_biotype'
    ]

    return (
        df[(df.variant_effect != 'intergenic') & (df.gene_id.notnull())]
            .loc[:, keep]
            .drop_duplicates(subset=['rsid', 'variant_effect', 'gene_id'])
    )


def combine_stats(dfs: List[Future]) -> pd.DataFrame:
    """
    """

    client = get_client()

    dfs = client.gather(dfs)

    return pd.concat(dfs, axis=0, sort=True)


def write_intergenic_variants(df) -> str:
    """
    :param df:
    :return:
    """

    client = get_client()
    #df = isolate_intergenic_variants(df)
    ## You have to scatter this or dask bitches and dies
    sdf = client.scatter(isolate_intergenic_variants(df), broadcast=True)

    return dfio.save_distributed_dataframe(sdf)


def write_annotated_variants(df) -> str:
    """
    :param df:
    :return:
    """

    #df = isolate_annotated_variants(df)

    #return dfio.save_distributed_dataframe(isolate_annotated_variants(df))
    client = get_client()
    #df = isolate_intergenic_variants(df)
    ## You have to scatter this or dask bitches
    sdf = client.scatter(isolate_annotated_variants(df), broadcast=True)

    return dfio.save_distributed_dataframe(sdf)


def write_annotation_stats(df, output) -> str:
    """

    """

    df.to_csv(output, sep='\t')

    return output


def collect_annotation_stats(df) -> ddf.DataFrame:
    """

    :param df:
    :return:
    """

    ## Conditions
    is_intergenic = df.variant_effect == 'intergenic'
    is_not_intergenic = df.variant_effect != 'intergenic'
    is_mapped = df.gene_id.notnull()
    is_not_mapped = df.gene_id.isnull()

    ## Intragenic variants successfully mapped to genes
    intra_mapped = (
        df[is_not_intergenic & is_mapped].loc[:, ['chromosome', 'rsid']]
            .drop_duplicates()
            .groupby('chromosome')
            .count()
            .compute()
    )

    ## Intragenic variants that failed to map to a gene (should be few or none)
    intra_failed = (
        df[is_not_intergenic & is_not_mapped].loc[:, ['chromosome', 'rsid']]
            .drop_duplicates()
            .groupby('chromosome')
            .count()
            .compute()
    )

    ## Intergenic variants
    intergenic = (
        df[is_intergenic].loc[:, ['chromosome', 'rsid']]
            .drop_duplicates()
            .groupby('chromosome')
            .count()
            .compute()
    )

    stats = pd.concat(
        [intra_mapped, intra_failed, intergenic],
        axis=1,
        sort=True
    ).fillna(0)

    ## Rename columns and the index
    stats.columns = ['intra_mapped', 'intra_failed', 'intergenic']
    stats.index.name = 'chromosome'

    ## Convert any remaining floats to ints
    stats = stats.astype(np.int64)

    return stats


def run_hg38_annotations(
    client: Client,
    variant_dir: str = globe._dir_hg38_variant_proc,
    gene_fp: str = globe._fp_hg38_gene_processed,
    annotated_dir: str = globe._dir_hg38_annotated,
    intergenic_dir: str = globe._dir_hg38_annotated,
    stats_fp: str = globe._fp_hg38_annotation_stats
):
    """

    :param client:
    :return:
    """

    ## List of Futures for annotated, intergenic, and mapping stats data
    annotated = []
    intergenic = []
    stats = []

    for chrom in globe._var_human_chromosomes:
    #for chrom in ['19', '20', '21', '22']:
    #for chrom in ['4']:
    #for chrom in ['1', '2', '3', '4']:
    #for chrom in ['10', '11', '12', '13']:

        log._logger.info(f'Starting chromosome {chrom} work')

        variant_fp = Path(variant_dir, f'chromosome-{chrom}.tsv')
        annotated_fp = Path(annotated_dir, f'annotated-chromosome-{chrom}.tsv')
        intergenic_fp = Path(intergenic_dir, f'intergenic-chromosome-{chrom}.tsv')

        vdf = read_processed_variants(variant_fp)
        gdf = read_processed_genes(gene_fp)
        adf = annotate_variants(vdf, gdf)

        ## Persist and start computation for the annotated dataset
        ndf = client.persist(isolate_annotated_variants(adf))
        idf = client.persist(isolate_intergenic_variants(adf))
        adf = client.persist(adf)

        ## Scatter the lazy frames to the workers otherwise dask bitches and dies when
        ## we use submit them to workers for processing
        sc_adf = client.scatter(adf, broadcast=True)
        sc_ndf = client.scatter(ndf, broadcast=True)
        sc_idf = client.scatter(idf, broadcast=True)

        ## Save the distributed dataframes to temp folders
        #annotated_tmp = client.submit(write_annotated_variants, sc_adf)
        #intergenic_tmp = client.submit(write_intergenic_variants, sc_adf)
        annotated_tmp = client.submit(dfio.save_distributed_dataframe, sc_ndf)
        intergenic_tmp = client.submit(dfio.save_distributed_dataframe, sc_idf)
        #annotated_tmp = client.submit(dfio.save_distributed_dataframe, ndf)
        #intergenic_tmp = client.submit(dfio.save_distributed_dataframe, idf)

        ## Consolidate distributed datasets
        annotated_fp = client.submit(
            dfio.consolidate_separate_partitions, annotated_tmp, annotated_fp
        )
        intergenic_fp = client.submit(
            dfio.consolidate_separate_partitions, intergenic_tmp, intergenic_fp
        )

        ## Get mapping stats
        annotation_stats = client.submit(collect_annotation_stats, sc_adf)

        annotated.append(annotated_fp)
        intergenic.append(intergenic_fp)
        stats.append(annotation_stats)

        #if chrom == '2':
        #    break

    ## Combine the mapping stats and save to a file
    stats = client.submit(combine_stats, stats)
    stats_fp = client.submit(write_annotation_stats, stats, stats_fp)

    return {
        'annotated': annotated,
        'intergenic': intergenic,
        #'stats': stats
        'stats': stats_fp
    }


def run_mm10_annotations(
    client: Client,
    variant_fp: str = globe._fp_mm10_variant_processed,
    gene_fp: str = globe._fp_mm10_gene_processed,
    annotated_fp: str = globe._fp_mm10_annotated,
    intergenic_fp: str = globe._fp_mm10_intergenic,
    stats_fp: str = globe._fp_mm10_annotation_stats
):
    """

    :param client:
    :return:
    """

    vdf = read_processed_variants(variant_fp)
    gdf = read_processed_genes(gene_fp)
    adf = annotate_variants(vdf, gdf)

    ## Persist and start computation for the annotated dataset
    adf = client.persist(adf)
    idf = client.persist(isolate_intergenic_variants(adf))
    ndf = client.persist(isolate_annotated_variants(adf))

    ## Scatter the lazy frames to the workers otherwise dask bitches and dies when
    ## we use submit them to workers for processing
    sc_adf = client.scatter(adf, broadcast=True)
    sc_idf = client.scatter(idf, broadcast=True)
    sc_ndf = client.scatter(ndf, broadcast=True)

    ## Save the distributed dataframes to temp folders
    #annotated_tmp = client.submit(write_annotated_variants, sc_adf)
    #intergenic_tmp = client.submit(write_intergenic_variants, sc_adf)
    annotated_tmp = client.submit(dfio.save_distributed_dataframe, sc_ndf)
    intergenic_tmp = client.submit(dfio.save_distributed_dataframe, sc_idf)

    ## Consolidate distributed datasets
    annotated_fp = client.submit(
        dfio.consolidate_separate_partitions, annotated_tmp, annotated_fp
    )
    intergenic_fp = client.submit(
        dfio.consolidate_separate_partitions, intergenic_tmp, intergenic_fp
    )

    ## Get and save mapping stats
    annotation_stats = client.submit(collect_annotation_stats, sc_adf)
    stats_fp = client.submit(write_annotation_stats, annotation_stats, stats_fp)

    return {
        'annotated': annotated_fp,
        'intergenic': intergenic_fp,
        'stats': stats_fp
    }

if __name__ == '__main__':

    log._initialize_logging(verbose=True)

    #client = Client(LocalCluster(
    #    n_workers=6,
    #    processes=True,
    #    local_dir='/var/tmp',
    #))
    ## Takes around 25min. to do all human chromosomes using 30 workers
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
        walltime='00:50:00',
        local_directory='/var/tmp',
        job_extra=['-e logs', '-o logs'],
        env_extra=['cd $PBS_O_WORKDIR']
    )

    cluster.adapt(minimum=10, maximum=45)

    client = Client(cluster)

    init_logging_partial = partial(log._initialize_logging, verbose=True)

    client.register_worker_callbacks(setup=init_logging_partial)

    #mm10_futures = run_mm10_annotations(client)
    #client.gather(mm10_futures)

    hg38_futures = run_hg38_annotations(client)
    client.gather(hg38_futures)
    ## Init logging on each worker
    #client.run(log._initialize_logging, verbose=True)

    #hg38_futures = run_hg38_variant_processing2(client)
    #hg38_futures = run_hg38_variant_processing3(client)
    #df = read_gvf_file('data/variant/hg38/raw/chromosome-21.vcf')
    #df = process_gvf(df)

    #log._logger.info('Processing and saving variants')

    #tempdir = save_variants(df)

    #log._logger.info('Consolidating variants')

    #consolidate_saved_variants(tempdir, 'data/variant/hg38/processed/chromosome-21.vcf')
    #client.gather(hg38_futures)

    log._logger.info('Done')
    #human_futures = run_human_feature_processing(client)
    #mouse_futures = run_mouse_feature_processing(client)

    #client.gather([human_futures, mouse_futures])

    client.close()

