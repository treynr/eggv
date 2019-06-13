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
from glob import glob
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


def _read_processed_variants(fp: str) -> ddf.DataFrame:
    """
    Read and parse a pre-processed file containing Ensembl variation build data. Should
    be in the format created by the process.py module.

    arguments
        fp: filepath

    returns
        a dask dataframe
    """

    return ddf.read_csv(fp, sep='\t', comment='#', dtype={'transcript': 'object'})


def _read_processed_genes(fp: str) -> ddf.DataFrame:
    """
    Read and parse a pre-processed file containing Ensembl gene build data. Should
    be in the format created by the process.py module.

    arguments
        fp: filepath

    returns
        a dask dataframe
    """

    return ddf.read_csv(fp, sep='\t', comment='#')


def annotate_variants(vdf, gdf) -> ddf.DataFrame:
    """
    Annotate variants to genes based on variant effects provided by Ensembl.

    :param vdf:
    :param gdf:
    :return:
    """

    ## Merge variant and gene frames based on the Ensembl transcript ID. Normally we
    ## use an inner merge but use a left merge instead so we can collect mapping
    ## statistics later on
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
    Return a dataframe containing only intergenic variant annotations.

    :param vdf:
    :param gdf:
    :return:
    """

    keep = ['rsid', 'variant_effect']

    ## Down/upstream annotations are present in v.95 mm10 builds but seem to have been
    ## removed in v.95 hg38 builds. Hooray for consistency.
    is_upstream = df.variant_effect == 'upstream_gene_variant'
    is_downstream = df.variant_effect == 'downstream_gene_variant'
    is_intergenic = (df.variant_effect == 'intergenic') | is_upstream | is_downstream

    return df[is_intergenic].loc[:, keep]


def isolate_intragenic_variants(df) -> ddf.DataFrame:
    """
    Return a dataframe containing only intergenic variants. Also removes duplicate rows
    based on rsid, variant effect, and gene ID triples.

    :param vdf:
    :param gdf:
    :return:
    """

    keep = ['rsid', 'variant_effect', 'gene_id', 'gene_name', 'gene_biotype']

    is_upstream = df.variant_effect == 'upstream_gene_variant'
    is_downstream = df.variant_effect == 'downstream_gene_variant'
    is_intergenic = is_upstream | is_downstream | (df.variant_effect == 'intergenic')

    return (
        df[(~is_intergenic) & (df.gene_id.notnull())]
            .loc[:, keep]
            .drop_duplicates(subset=['rsid', 'variant_effect', 'gene_id'])
    )


def run_annotation_pipeline(
    vdf: ddf.DataFrame,
    gdf: ddf.DataFrame,
    client: Client = None,
    **kwargs
) -> Dict[str, ddf.DataFrame]:
    """

    :param df:
    :param client:
    :param kwargs:
    :return:
    """

    client = get_client() if client is None else client

    annotated = annotate_variants(vdf, gdf)
    intergenic = client.persist(isolate_intergenic_variants(annotated))
    intragenic = client.persist(isolate_intragenic_variants(annotated))

    return {
        'intergenic': intergenic,
        'intragenic': intragenic,
    }


def run_complete_hg38_annotation_pipeline(
    variant_dir: str = globe._dir_hg38_variant_effect,
    gene_fp: str = globe._fp_hg38_gene_meta,
    intergenic_dir: str = globe._dir_hg38_annotated_inter,
    intragenic_dir: str = globe._dir_hg38_annotated_intra,
    client: Client = None
):
    """
    Run the complete annotation pipeline starting from processed hg38 gene
    and variant files. Save annotated datasets to files.

    :param client:
    :return:
    """

    client = get_client() if client is None else client

    ## List of Futures for annotated, intergenic, and mapping stats data
    futures = []

    ## Read/parse processed genes into a dask dataframe
    gene_df = _read_processed_genes(gene_fp)

    ## The input directory should have variant effect TSV files if no custom output
    ## filenames were used and the pipeline has used default settings to this point
    for fp in glob(f'{variant_dir}/*.tsv'):

        ## Read/parse processed variants into a dask dataframe
        variant_df = _read_processed_variants(fp)

        ## Annotate
        annotations = run_annotation_pipeline(variant_df, gene_df, client=client)

        ## Scatter annotations prior to saving
        intergenic = client.scatter(annotations['intergenic'])
        intragenic = client.scatter(annotations['intragenic'])

        ## Save to files
        inter_future = client.submit(
            dfio.save_dataset, intergenic, name=fp, outdir=intergenic_dir
        )
        intra_future = client.submit(
            dfio.save_dataset, intragenic, name=fp, outdir=intragenic_dir
        )

        futures.append({
            'intergenic': inter_future,
            'intragenic': intra_future
        })

    return futures


def run_complete_mm10_annotation_pipeline(
    variant_fp: str = globe._fp_mm10_variant_effect,
    gene_fp: str = globe._fp_mm10_gene_meta,
    intergenic_fp: str = globe._fp_mm10_intergenic,
    intragenic_fp: str = globe._fp_mm10_intragenic,
    client: Client = None
):
    """
    Run the complete annotation pipeline starting from processed mm10 gene
    and variant files. Save annotated datasets to files.

    :param client:
    :return:
    """

    client = get_client() if client is None else client

    ## List of Futures for annotated, intergenic, and mapping stats data
    futures = []

    ## Read/parse processed genes into a dask dataframe
    gene_df = _read_processed_genes(gene_fp)

    ## Read/parse processed variants into a dask dataframe
    variant_df = _read_processed_variants(variant_fp)

    ## Annotate
    annotations = run_annotation_pipeline(variant_df, gene_df, client=client)

    ## Scatter annotations prior to saving
    intergenic = client.scatter(annotations['intergenic'])
    intragenic = client.scatter(annotations['intragenic'])

    ## Save to files
    inter_future = client.submit(
        dfio.save_dataset, intergenic, name=variant_fp, output=intergenic_fp
    )
    intra_future = client.submit(
        dfio.save_dataset, intragenic, name=variant_fp, output=intragenic_fp
    )

    futures.append({
        'intergenic': inter_future,
        'intragenic': intra_future
    })

    return futures


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

    hg38_futures = run_complete_hg38_annotation_pipeline(client)
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

## this is for later
#def combine_stats(dfs: List[Future]) -> pd.DataFrame:
#    """
#    """
#
#    client = get_client()
#
#    dfs = client.gather(dfs)
#
#    return pd.concat(dfs, axis=0, sort=True)
#
#def write_annotation_stats(df, output) -> str:
#    """
#
#    """
#
#    df.to_csv(output, sep='\t')
#
#    return output
#
#
#def collect_annotation_stats(df) -> ddf.DataFrame:
#    """
#
#    :param df:
#    :return:
#    """
#
#    ## Conditions
#    is_intergenic = df.variant_effect == 'intergenic'
#    is_not_intergenic = df.variant_effect != 'intergenic'
#    is_mapped = df.gene_id.notnull()
#    is_not_mapped = df.gene_id.isnull()
#
#    ## Intragenic variants successfully mapped to genes
#    intra_mapped = (
#        df[is_not_intergenic & is_mapped].loc[:, ['chromosome', 'rsid']]
#            .drop_duplicates()
#            .groupby('chromosome')
#            .count()
#            .compute()
#    )
#
#    ## Intragenic variants that failed to map to a gene (should be few or none)
#    intra_failed = (
#        df[is_not_intergenic & is_not_mapped].loc[:, ['chromosome', 'rsid']]
#            .drop_duplicates()
#            .groupby('chromosome')
#            .count()
#            .compute()
#    )
#
#    ## Intergenic variants
#    intergenic = (
#        df[is_intergenic].loc[:, ['chromosome', 'rsid']]
#            .drop_duplicates()
#            .groupby('chromosome')
#            .count()
#            .compute()
#    )
#
#    stats = pd.concat(
#        [intra_mapped, intra_failed, intergenic],
#        axis=1,
#        sort=True
#    ).fillna(0)
#
#    ## Rename columns and the index
#    stats.columns = ['intra_mapped', 'intra_failed', 'intergenic']
#    stats.index.name = 'chromosome'
#
#    ## Convert any remaining floats to ints
#    stats = stats.astype(np.int64)
#
#    return stats

