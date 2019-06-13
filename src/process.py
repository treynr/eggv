#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: process.py
## desc: Functions for processing and formatting Ensembl variation and gene builds.

from dask.distributed import Client
from dask.distributed import Future
from dask.distributed import LocalCluster
from dask.distributed import get_client
from dask.distributed import fire_and_forget
from dask.distributed import secede
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

from . import dfio
from . import globe
from . import log

logging.getLogger(__name__).addHandler(logging.NullHandler())


def _read_gvf_file(fp: str) -> ddf.DataFrame:
    """
    Read a GVF file into a Dask dataframe.
    GVF format specifications can be found here:
    https://github.com/The-Sequence-Ontology/Specifications/blob/master/gvf.md

    arguments
        fp: filepath to the GVF file

    returns
        a dask dataframe
    """

    ## Header fields for the GVF file
    header = [
        'seqid',
        'source',
        'type',
        'start',
        'end',
        'score',
        'strand',
        'phase',
        'attr'
    ]

    ## Sometimes type inference fails since not all chromosomes (seqid) are numbers
    return ddf.read_csv(
        fp, sep='\t', comment='#', header=None, names=header, dtype={'seqid': 'object'}
    )


def _read_gtf_file(fp: str) -> ddf.DataFrame:
    """
    Read a GTF file into a Dask dataframe.
    GTF format specifications can be found here:
    https://useast.ensembl.org/info/website/upload/gff.html

    arguments
        fp: filepath to the GTF file

    returns
        a dask dataframe
    """

    ## Header fields for the GTF file
    header = [
        'seqname',
        'source',
        'feature',
        'start',
        'end',
        'score',
        'strand',
        'frame',
        'attr'
    ]

    return ddf.read_csv(
        fp, sep='\t', comment='#', header=None, names=header, dtype={'seqname': 'object'}
    )


def _process_gvf(df: ddf.DataFrame) -> ddf.DataFrame:
    """
    Process and format GVF fields into an intermediate data representation.
    Filters out fields that are not necessary (for our purposes), extracts variant and
    reference alleles, extracts the MAF, parses out variant effects and adds new rows
    for each effect + modified transcript.

    """

    ## Only keep fields we need
    df = df[['seqid', 'start', 'end', 'attr']]

    ## Rename to chromosome
    df = df.rename(columns={'seqid': 'chromosome'})

    ## Add the 'chr' prefix since that's usually pretty standard
    df['chromosome'] = 'chr' + df.chromosome.astype(str)

    ## Attempt to parse out a refSNP identifier from the attributes column
    df['rsid'] = df.attr.str.extract(r'Dbxref=dbSNP_\d+:(rs\d+)', expand=False)

    ## Drop anything that doesn't have a refSNP
    df = df.dropna(subset=['rsid'])

    ## Then strip the rs prefix and convert the refSNP ID to a 64-bit int
    df['rsid'] = df.rsid.str.strip('rs').astype(np.int64)

    ## Attempt to parse out reference and variant alleles
    df['var_allele'] = df.attr.str.extract(r'Variant_seq=([-,ACGT]+)', expand=False)
    df['ref_allele'] = df.attr.str.extract(r'Reference_seq=([-,ACGT]+)', expand=False)

    ## Just in case
    df['var_allele'] = df.var_allele.fillna(value='-')
    df['ref_allele'] = df.ref_allele.fillna(value='-')

    ## Combine variant and reference alleles into a single observed alleles field
    df['observed'] = df.var_allele + ',' + df.ref_allele

    ## Attempt to extract the MAF
    df['maf'] = df.attr.str.extract(
        r'global_minor_allele_frequency=\d+\|([.0-9]+)', expand=False
    )

    ## Most things don't have a MAF
    df['maf'] = df.maf.fillna(value=0.0)

    ## Attempt to extract the variant effect, of which there may be several. The variant
    ## effect attribute looks something like this:
    ## Variant_effect=non_coding_transcript_variant 0 ncRNA ENST00000566940,
    ## intron_variant 0 primary_transcript ENST00000566940,
    ## non_coding_transcript_variant 1 ncRNA ENST00000566940,
    df['veffect'] = df.attr.str.extract(r'Variant_effect=(.+?);', expand=False)

    ## Each variant can have multiple effects, so we split the variant effects into their
    ## own individual rows while still retaining the original variant metadata per new row
    df = df.map_partitions(
        lambda d: d.drop('veffect', axis=1).join(
            d.veffect.str.split(',', expand=True)
                .stack()
                .reset_index(drop=True, level=1)
                .rename('veffect')
        )
    )

    ## Fill in missing effects and split to separate effects/transcripts
    df['veffect'] = df.veffect.fillna('intergenic')
    df['veffect'] = df.veffect.str.split(' ')
    df['effect'] = df.veffect.str.get(0)
    ## Some transcripts will have NaN values, these are replaced by 'NA' later on
    ## in the to_csv function
    df['transcript'] = df.veffect.str.get(3)

    df = df.reset_index(drop=True)

    ## Keep only things we need
    return df[[
        'chromosome', 'rsid', 'start', 'end', 'observed', 'maf', 'effect', 'transcript'
    ]]


def _process_gtf(df: ddf.DataFrame) -> pd.DataFrame:
    """
    Process and format GTF fields into an intermediate data representation.
    Filters out fields that are not necessary (for our purposes), extracts variant and
    reference alleles, extracts the MAF, parses out variant effects and adds new rows
    for each effect + transcript.

    arguments
        fp: filepath to the Ensembl gene build in GTF format.

    returns
        a pandas dataframe
    """

    ## Remove anything that isn't a transcript feature, since we'll be using transcripts
    ## to annotate variants with the genes they're associated with
    df = df[df.feature == 'transcript']

    ## Attempt to extract the Ensembl gene ID from the attribute list
    df['gene_id'] = df.attr.str.extract(r'gene_id "(ENS[A-Z]*\d+)"', expand=False)

    ## Attempt to extract the Ensembl transcript ID from the attribute list
    df['transcript_id'] = df.attr.str.extract(r'transcript_id "(ENS[A-Z]*\d+)"', expand=False)

    ## Attempt to extract the gene biotype from the attributes
    df['biotype'] = df.attr.str.extract(r'biotype "(\w+)"', expand=False)
    df['biotype'] = df.biotype.fillna('NA')

    ## Attempt to extract the gene name from the attributes
    df['gene_name'] = df.attr.str.extract(r'gene_name "(\w+)"', expand=False)
    df['gene_name'] = df.gene_name.fillna('NA')

    ## Remove rows that don't have identifiers
    df = df.dropna(subset=['gene_id', 'transcript_id'])

    ## Only keep relevant rows
    df = df[[
        'seqname', 'start', 'end', 'transcript_id', 'gene_id', 'gene_name', 'biotype'
    ]]

    ## Rename some columns
    df = df.rename(columns={'seqname': 'chromosome'})

    ## These aren't floats pandas, no matter what you think
    df['start'] = df.start.astype('int')
    df['end'] = df.end.astype('int')

    ## Add 'chr' to the beginning of the chromosome number for uniformity
    df['chromosome'] = 'chr' + df.chromosome.astype('str')

    return df


def _process_genes(input: str, client: Client = None) -> ddf.DataFrame:
    """
    28 min.
    """

    client = get_client() if client is None else client

    ## Read the GTF file and annotate header fields
    raw_df = _read_gtf_file(input)

    ## Completely process the given GTF file into a format suitable for our needs
    processed_df = _process_gtf(raw_df)

    ## Persist the processed dask dataframe on the workers
    processed_df = client.persist(processed_df)

    return processed_df


def _isolate_variant_metadata(df: ddf.DataFrame) -> ddf.DataFrame:
    """
    Process the intermediate variant format into one that only keeps individual
    variant metadata.

    :param df:
    :return:
    """

    return df[['chromosome', 'rsid', 'start', 'end', 'observed', 'maf']].drop_duplicates(
        subset=['rsid']
    )


def _isolate_variant_effects(df: ddf.DataFrame) -> ddf.DataFrame:
    """
    Process the intermediate variant format into one that only keeps variant effects and
    the transcript modifications. This is used later on to annotate genes to variants.

    :param df:
    :return:
    """

    return df[['rsid', 'effect', 'transcript']]


def _process_variants(fp: str) -> ddf.DataFrame:
    """
    Read and process variants into an intermediate format suitable for our needs.

    arguments
        fp: filepath to a variant (GVF) file

    returns
        a dask dataframe
    """

    ## Read the GVF file and annotate header fields
    raw_df = _read_gvf_file(fp)

    ## Completely process the given GVF file
    return _process_gvf(raw_df)


def process_variant_effects(df: ddf.DataFrame, client: Client = None) -> ddf.DataFrame:
    """
    Process and isolate the effects associated with individual variants. The
    effects can be used to annotate intragenic variants to their respective genes later
    on.

    arguments
    :param df:
    :param client:

    returns
    """

    client = get_client() if client is None else client

    ## Isolate variant metadata  from the effects
    effect_df = _isolate_variant_effects(df)

    ## Persist the processed data form on the workers
    effect_df = client.persist(effect_df)

    return effect_df


def process_variant_metadata(df: ddf.DataFrame, client: Client = None) -> ddf.DataFrame:
    """
    Process and isolate the metadata associated with individual variants.

    arguments
    :param df:
    :param client:

    returns
    """

    client = get_client() if client is None else client

    ## Isolate variant metadata  from the effects
    meta_df = _isolate_variant_metadata(df)

    ## Persist the processed data form on the workers
    meta_df = client.persist(meta_df)

    return meta_df


def run_variant_processing_pipeline(
    input: str,
    client: Client = None
) -> Dict[str, ddf.DataFrame]:
    """
    Run the first half of the variant processing pipeline step for a single variant (GVF)
    file. This step is common to both hg38 and mm10 builds, although hg38 builds require
    this step to be run multiple times. This will process variants into an intermediate
    format and separate out variant metadata and variant effects. After this step,
    the variants are completely processed and ready to be stored to disk or processing
    can continue (e.g. annotation).

    :param input:
    :param effect_dir:
    :param meta_dir:
    :param client:
    :return:
    """

    client = get_client() if client is None else client

    ## Read and process to an intermediate format
    processed_df = _process_variants(input)

    ## Isolate variant metadata and effects
    effect_df = process_variant_effects(processed_df, client=client)
    meta_df = process_variant_metadata(processed_df, client=client)

    return {
        'effects': effect_df,
        'metadata': meta_df,
    }


## ugh these function names are so fucking long
def run_complete_hg38_variant_processing_pipeline(
    indir: str = globe._dir_hg38_variant_raw,
    effect_dir: str = globe._dir_hg38_variant_effect,
    meta_dir: str = globe._dir_hg38_variant_meta,
    client: Client = None
) -> List[Future]:
    """
    Run the variant processing pipeline step for all hg38 build chromosomes.

    :param input:
    :param effect_dir:
    :param meta_dir:
    :param client:
    :return:
    """

    client = get_client() if client is None else client

    futures = []

    ## The input directory should have a bunch of GVF files if no custom output filenames
    ## were used
    for fp in glob(f'{indir}/*.gvf'):

        log._logger.info(f'Starting work on {fp}')

        results = run_variant_processing_pipeline(fp, client=client)

        ## Have to scatter the DFs prior to using submit
        #meta_df = client.scatter(results['metadata'], broadcast=True)
        #effect_df = client.scatter(results['effects'], broadcast=True)

        #meta_future = client.submit(dfio.save_dataset, meta_df, name=fp, outdir=meta_dir)
        #effect_future = client.submit(
        #    dfio.save_dataset, effect_df, name=fp, outdir=effect_dir
        #)
        effect_future = dfio.save_dataset_in_background(
            results['effects'], name=fp, outdir=effect_dir
        )
        meta_future = dfio.save_dataset_in_background(
            results['metadata'], name=fp, outdir=meta_dir
        )

        ## Since this function is meant to be called from the main below, we keep the
        ## data saving futures for gathering later on
        futures.append({
            'effects': effect_future,
            'metadata': meta_future
        })

    log._logger.info('Returning')
    return futures


def run_mm10_variant_processing_pipeline(
    input: str = globe._fp_mm10_variant_raw,
    client: Client = None,
    **kwargs
) -> Dict[str, Future]:
    """
    mm10 build wrapper function for run_initial_variant_processing_pipeline.

    arguments

    returns
    """

    client = get_client() if client is None else client

    return run_variant_processing_pipeline(input, client=client)


def run_complete_mm10_variant_processing_pipeline(
    input: str = globe._fp_mm10_variant_raw,
    effect_path: str = globe._fp_mm10_variant_effect,
    meta_path: str = globe._fp_mm10_variant_meta,
    client: Client = None,
    **kwargs
) -> Dict[str, Future]:
    """
    28 min.
    """

    if not client:
        client = get_client()

    results = run_variant_processing_pipeline(input, client=client)

    ## Have to scatter the DFs prior to using submit otherwise a puppy dies
    meta_df = client.scatter(results['metadata'], broadcast=True)
    effect_df = client.scatter(results['effects'], broadcast=True)

    meta_future = client.submit(dfio.save_dataset, meta_df, output=effect_path)
    effect_future = client.submit(dfio.save_dataset, effect_df, output=meta_path)

    ## Return the filepaths from the dataframe IO functions
    return {
        'effects': effect_future,
        'metadata': meta_future
    }


def run_gene_processing_pipeline(
    input: str,
    client: Client = None,
    **kwargs
) -> ddf.DataFrame:
    """
    Wrapper function for _process_genes which does all the heavy lifting.
    """

    client = get_client() if client is None else client

    return _process_genes(input, client=client)


def run_hg38_gene_processing_pipeline(
    input: str = globe._fp_hg38_gene_raw,
    **kwargs
) -> ddf.DataFrame:
    """
    hg38 build wrapper function for run_gene_processing_pipeline.
    """

    return run_gene_processing_pipeline(input)


def run_complete_hg38_gene_processing_pipeline(
    input: str = globe._fp_hg38_gene_raw,
    output: str = globe._fp_hg38_gene_meta,
    client: Client = None,
    **kwargs
) -> Future:
    """
    28 min.
    """

    client = get_client() if client is None else client

    ## Run the pipeline and process the genes
    gene_df = run_hg38_gene_processing_pipeline(input, client=client)

    ## Scatter to workers prior to saving
    gene_df = client.scatter(gene_df, broadcast=True)

    ## Write the dataframe to a file
    gene_future = client.submit(dfio.save_dataset, gene_df, output=output)

    return gene_future


def run_mm10_gene_processing_pipeline(
    input: str = globe._fp_mm10_gene_raw,
    **kwargs
) -> ddf.DataFrame:
    """
    mm10 build wrapper function for run_gene_processing_pipeline.
    """

    return run_gene_processing_pipeline(input)


def run_complete_mm10_gene_processing_pipeline(
    input: str = globe._fp_mm10_gene_raw,
    output: str = globe._fp_mm10_gene_meta,
    client: Client = None,
    **kwargs
) -> Future:
    """
    28 min.
    """

    client = get_client() if client is None else client

    ## Run the pipeline and process the genes
    gene_df = run_mm10_gene_processing_pipeline(input, client=client)

    ## Scatter to workers prior to saving
    gene_df = client.scatter(gene_df, broadcast=True)

    ## Write the dataframe to a file
    gene_future = client.submit(dfio.save_dataset, gene_df, output=output)

    return gene_future


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
        #memory='160GB',
        cores=1,
        processes=1,
        memory='80GB',
        local_directory='/tmp',
        walltime='03:00:00',
        job_extra=['-e logs', '-o logs'],
        env_extra=['cd $PBS_O_WORKDIR']
    )

    cluster.adapt(minimum=10, maximum=45)

    client = Client(cluster)

    Path('logs').mkdir(exist_ok=True)
    init_logging_partial = partial(log._initialize_logging, verbose=True)

    client.register_worker_callbacks(setup=init_logging_partial)

    #hg38_variants = run_hg38_processing_pipeline(
    #    Path(globe._dir_hg38_variant_raw, 'hg38-chromosome-10.gvf').as_posix()
    #)

    #hg38_genes = run_complete_hg38_gene_processing_pipeline()
    hg38_variants = run_complete_hg38_variant_processing_pipeline()
    client.gather(hg38_variants)
    #client.gather(hg38_genes)
    #mm10_variants = run_complete_mm10_variant_processing_pipeline()
    #client.gather(mm10_variants)
    ## Init logging on each worker
    #client.run(log._initialize_logging, verbose=True)

    #mm10_variants = run_mm10_variant_processing(client)
    #mm10_genes = run_mm10_gene_processing(client)

    #hg38_variants = run_hg38_variant_processing(client)
    #hg38_genes = run_hg38_gene_processing(client)

    #client.gather(mm10_variants)
    #client.gather(mm10_genes)
    #client.gather(hg38_variants)
    #client.gather(hg38_genes)

    #hg38_futures = run_hg38_variant_processing2(client)
    #client.gather(hg38_futures)
    #hg38_futures = run_hg38_variant_processing3(client)
    #df = read_gvf_file('data/variant/hg38/raw/chromosome-21.vcf')
    #df = process_gvf(df)

    #log._logger.info('Processing and saving variants')

    #tempdir = save_variants(df)

    #log._logger.info('Consolidating variants')

    #consolidate_saved_variants(tempdir, 'data/variant/hg38/processed/chromosome-21.vcf')

    log._logger.info('Done')
    #human_futures = run_human_feature_processing(client)
    #mouse_futures = run_mouse_feature_processing(client)

    #client.gather([human_futures, mouse_futures])

    client.close()

