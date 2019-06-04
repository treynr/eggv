#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: process.py
## desc: Functions for processing and formatting Ensembl variation and gene builds.

from dask.distributed import Client
from dask.distributed import Future
from dask.distributed import get_client
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


def read_gvf_file(fp: str) -> ddf.DataFrame:
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

    return ddf.read_csv(fp, sep='\t', comment='#', header=None, names=header)


def read_gtf_file(fp: str) -> ddf.DataFrame:
    """
    Read a GTF file into a Dask dataframe.
    GTF format specifications can be found here:
    https://useast.ensembl.org/info/website/upload/gff.html

    arguments
        fp: filepath to the GTF file

    returns
        a dask dataframe
    """

    ## Header fields for the GFF file
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

    return ddf.read_csv(fp, sep='\t', comment='#', header=None, names=header)


def process_gvf(df: ddf.DataFrame) -> ddf.DataFrame:
    """
    Process and format GVF fields into an intermediate data representation.
    Filters out fields that are not necessary (for our purposes), extracts variant and
    reference alleles, extracts the MAF, parses out variant effects and adds new rows
    for each effect + transcript.

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
    df['effect'] = df.attr.str.extract(r'Variant_effect=(.+?);', expand=False)

    ## Each variant can have multiple effects, so we split the variant effects into their
    ## own individual rows while still retaining the original variant metadata per new row
    df = df.map_partitions(
        lambda d: d.drop('effect', axis=1).join(
            d.effect.str.split(',', expand=True)
                .stack()
                .reset_index(drop=True, level=1)
                .rename('effect')
        )
    )

    ## Then separate out the actual effect and the gene transcript
    ## dask 1.2.2 has a bug where split() doesn't work at all. Works in 1.2.1 though.
    df['transcript'] = df.effect.str.split(' ').str.get(3)
    df['effect'] = df.effect.str.split(' ').str.get(0)

    ## Not all variants produce transcript effects
    df['effect'] = df.effect.fillna('intergenic')
    df['transcript'] = df.transcript.fillna('NA')

    df = df.reset_index(drop=True)

    ## Keep only things we need
    return df[[
        'chromosome', 'rsid', 'start', 'end', 'observed', 'maf', 'effect', 'transcript'
    ]]


def process_gtf(df: ddf.DataFrame) -> pd.DataFrame:
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
    df['transcript_id'] = df.attr.str.extract(r'transcript_id "(ENST\d+)"', expand=False)

    ## Attempt to extract the gene biotype from the attributes
    df['biotype'] = df.attr.str.extract(r'biotype "(\w+)"', expand=False)
    df['biotype'] = df.gene_name.fillna('NA')

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


def isolate_chromosome_variants(df: ddf.DataFrame, chrom: str) -> ddf.DataFrame:
    """
    Filter variants specific to a single chromosome.
    """

    return df[df.chromosome == f'chr{chrom}']


def save_distributed_variants(df: ddf.DataFrame) -> str:
    """
    Write variants to a file. Assumes these variants have been distributed using dask
    dataframes. Writes each dataframe partition to a temp folder.
    """

    temp = tf.mkdtemp(dir=globe._dir_data)

    df.to_csv(temp, sep='\t', index=False, compute=True)

    return temp


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
    variant_fp = Path(globe._dir_hg38_variant_raw, '*.vcf')
    #variant_fp = Path(globe._dir_hg38_variant_raw, 'chromosome-2.vcf')

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
        filtered_df = client.scatter(filtered_df, broadcast=True)
        #print(filtered_df.result().head())
        #print(filtered_df.head())
        saved_fp = client.submit(save_distributed_variants, filtered_df)
        consolidated = client.submit(consolidate_saved_variants, saved_fp, output)

        futures.append(consolidated)
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
        cores=2,
        processes=2,
        memory='80GB',
        walltime='01:30:00',
        job_extra=['-e logs', '-o logs'],
        env_extra=['cd $PBS_O_WORKDIR']
    )

    cluster.adapt(minimum=10, maximum=100)

    client = Client(cluster)

    init_logging_partial = partial(log._initialize_logging, verbose=True)

    client.register_worker_callbacks(setup=init_logging_partial)

    ## Init logging on each worker
    #client.run(log._initialize_logging, verbose=True)

    hg38_futures = run_hg38_variant_processing(client)
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

