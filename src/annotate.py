#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: annotate.py
## desc: Functions for annotating gene variants based on the variant effects provided
##       by Ensembl.

from dask.distributed import Client
from dask.distributed import get_client
from glob import glob
from pathlib import Path
from typing import Dict
import dask.dataframe as ddf
import logging

from . import dfio
from .globe import Globals
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

    return ddf.read_csv(
        fp, sep='\t', comment='#', dtype={'transcript': 'object'}, blocksize='150MB'
    )


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


def annotate_variants(vdf: ddf.DataFrame, gdf: ddf.DataFrame) -> ddf.DataFrame:
    """
    Annotate variants to genes based on variant effects provided by Ensembl.

    arguments
        vdf: dask dataframe containing processed variants
        gdf: dask dataframe containing processed genes

    returns
        a dataframe containing annotated variants
    """

    ## Merge variant and gene frames based on the Ensembl transcript ID. Normally we
    ## use an inner merge but use a left merge instead so we can collect mapping
    ## statistics later on
    df = vdf.merge(
        gdf,
        how='left',
        left_on='transcript',
        right_on='transcript_id',
        suffixes=('_l', '_r'),
        npartitions=100
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


def isolate_intergenic_variants(df: ddf.DataFrame) -> ddf.DataFrame:
    """
    Return a dataframe containing only intergenic variant annotations.
    Removes columns that are no longer necessary.

    arguments
        df: a dask dataframe containing annotated variants

    returns
        a dataframe containing only intergenic variants
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

    arguments
        df: a dask dataframe containing annotated variants

    returns
        a dataframe containing only intragenic variants
    """

    keep = ['rsid', 'variant_effect', 'gene_id', 'gene_name', 'gene_biotype']

    is_upstream = df.variant_effect == 'upstream_gene_variant'
    is_downstream = df.variant_effect == 'downstream_gene_variant'
    is_intergenic = is_upstream | is_downstream | (df.variant_effect == 'intergenic')

    return (
        df[(~is_intergenic) & (df.gene_id.notnull())]
            .loc[:, keep]
            .drop_duplicates(subset=['rsid', 'variant_effect', 'gene_id'], split_out=100)
    )


def run_annotation_pipeline(
    vdf: ddf.DataFrame,
    gdf: ddf.DataFrame,
) -> Dict[str, ddf.DataFrame]:
    """

    :param df:
    :param client:
    :param kwargs:
    :return:
    """

    annotated = annotate_variants(vdf, gdf)
    intergenic = isolate_intergenic_variants(annotated)

    intragenic = isolate_intragenic_variants(annotated)

    return {
        'intergenic': intergenic,
        'intragenic': intragenic,
    }


def run_complete_hg38_annotation_pipeline(
    variant_dir: str = None,
    gene_fp: str = None,
    intergenic_dir: str = None,
    intragenic_dir: str = None,
    client: Client = None
):
    """
    Run the complete annotation pipeline starting from processed hg38 gene
    and variant files. Save annotated datasets to files.

    :param client:
    :return:
    """

    if variant_dir is None:
        variant_dir = Globals().reinitialize(build='hg38').dir_variant_effects

    if gene_fp is None:
        gene_fp = Globals().reinitialize(build='hg38').fp_gene_meta

    if intergenic_dir is None:
        intergenic_dir = Globals().reinitialize(build='hg38').dir_annotated_inter

    if intragenic_dir is None:
        intragenic_dir = Globals().reinitialize(build='hg38').dir_annotated_intra

    client = get_client() if client is None else client

    ## List of Futures for annotated, intergenic, and mapping stats data
    futures = []

    ## Read/parse processed genes into a dask dataframe
    gene_df = _read_processed_genes(gene_fp)

    log._logger.info(f'Reading files in {variant_dir}')

    ## The input directory should have variant effect TSV files if no custom output
    ## filenames were used and the pipeline has used default settings to this point
    for fp in glob(f'{variant_dir}/*.tsv'):

        log._logger.info(f'Working on {fp}')
        ## Read/parse processed variants into a dask dataframe
        variant_df = _read_processed_variants(fp)

        ## Annotate
        annotations = run_annotation_pipeline(variant_df, gene_df)

        intergenic = annotations['intergenic']
        intragenic = annotations['intragenic']

        intergenic = client.persist(intergenic)
        intragenic = client.persist(intragenic)

        ## Save to files
        inter_future = dfio.save_dataframe_async(
            intergenic, Path(intergenic_dir, Path(fp).name).as_posix()
        )
        intra_future = dfio.save_dataframe_async(
            intragenic, Path(intragenic_dir, Path(fp).name).as_posix()
        )

        futures.append({
            'intergenic': inter_future,
            'intragenic': intra_future
        })

    return futures


def run_complete_mm10_annotation_pipeline(
    variant_fp: str = None,
    gene_fp: str = None,
    intergenic_fp: str = None,
    intragenic_fp: str = None,
    client: Client = None
):
    """
    Run the complete annotation pipeline starting from processed mm10 gene
    and variant files. Save annotated datasets to files.

    :param client:
    :return:
    """

    if variant_fp is None:
        variant_fp = Globals().reinitialize(build='mm10').fp_variant_effects

    if gene_fp is None:
        gene_fp = Globals().reinitialize(build='mm10').fp_gene_meta

    if intergenic_fp is None:
        intergenic_fp = Globals().reinitialize(build='mm10').fp_annotated_inter

    if intragenic_fp is None:
        intragenic_fp = Globals().reinitialize(build='mm10').fp_annotated_intra

    client = get_client() if client is None else client

    ## List of Futures for annotated, intergenic, and mapping stats data
    futures = []

    ## Read/parse processed genes into a dask dataframe
    gene_df = _read_processed_genes(gene_fp)

    ## Read/parse processed variants into a dask dataframe
    variant_df = _read_processed_variants(variant_fp)

    ## Annotate
    annotations = run_annotation_pipeline(variant_df, gene_df)

    ## Scatter annotations prior to saving
    # intergenic = client.scatter(annotations['intergenic'])
    # intragenic = client.scatter(annotations['intragenic'])

    ## Save to files
    inter_future = dfio.save_dataframe_async(
        annotations['intergenic'], intergenic_fp.as_posix() # type: ignore
    )
    intra_future = dfio.save_dataframe_async(
        annotations['intragenic'], intragenic_fp.as_posix() # type: ignore
    )

    futures.append({
        'intergenic': inter_future,
        'intragenic': intra_future
    })

    return futures

