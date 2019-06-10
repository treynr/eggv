#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: process.py
## desc: Functions for processing and formatting Ensembl variation and gene builds.

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

    ## Sometimes type inference fails since not all chromosomes (seqid) are numbers
    return ddf.read_csv(
        fp, sep='\t', comment='#', header=None, names=header, dtype={'seqid': 'object'}
    )


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

    return ddf.read_csv(
        fp, sep='\t', comment='#', header=None, names=header, dtype={'seqname': 'object'}
    )


def process_gvf(df: ddf.DataFrame) -> ddf.DataFrame:
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

    ## Not all variants produce transcript effects
    #df['effect'] = df.veffect.fillna('intergenic')
    #df['transcript'] = df.veffect.fillna('NA')

    ## Then separate out the actual effect and the gene transcript
    ## dask 1.2.2 has a bug where split() doesn't work when expand==False.
    ## Works in 1.2.1 though.
    #df.loc[df.veffect.notnull(), 'transcript'] = df[df.veffect.notnull()].veffect.str.split(' ').str.get(3)
    #print(df.veffect.head(n=100))
    ## There's a bug with the dask split implementation so we have to do it this way
    #df['transcript'] = df.veffect.map_partitions(lambda s: s.str.split(' ').str.get(3))
    #df['effect'] = df.veffect.map_partitions(lambda s: s.str.split(' ').str.get(0))
    #df['effect'] = df.veffect.str.split(' ').str.get(0)


    #df['veffect'] = df.veffect.str.split(' ')
    #df['effect'] = '0'
    #df['transcript'] = '0'
    #df['transcript'] = df.veffect.str.get(3)
    #df['effect'] = df.veffect.str.get(0)
    #df['transcript'] = df.veffect.str.split(' ')
    #df['transcript'] = df.transcript.str.get(3)
    #df['effect'] = df.veffect.str.split(' ')
    #df['effect'] = df.effect.str.get(0)
    ## Had to do this weird mask thing cause exceptions kept getting thrown for some
    ## (but not all) inputs: AttributeError: 'Series' object has no attribute 'split'.

    #df['transcript'] = df.veffect.mask(df.veffect.notnull(), df.veffect.split(' ').str.get(3))
    #df['effect'] = df.veffect.mask(df.veffect.notnull(), df.veffect.split(' ').str.get(0))
    ##df['effect'] = df.effect.str.split(' ').str.get(0)
    #df['effect'] = df.effect.fillna('intergenic')
    #df['transcript'] = df.transcript.fillna('NA')

    df = df.reset_index(drop=True)

    ## Keep only things we need
    return df[[
        'chromosome', 'rsid', 'start', 'end', 'observed', 'maf', 'effect', 'transcript'
    ]]


def isolate_variant_metadata(df: ddf.DataFrame) -> ddf.DataFrame:
    """
    Process the intermediate variant format into one that only keeps individual
    variant metadata.

    :param df:
    :return:
    """

    return df[['chromosome', 'rsid', 'start', 'end', 'observed', 'maf']].drop_duplicates(
        subset=['rsid']
    )


def isolate_variant_effects(df: ddf.DataFrame) -> ddf.DataFrame:
    """
    Process the intermediate variant format into one that only keeps variant effects and
    the transcript modifications. This is used later on to annotate genes to variants.

    :param df:
    :return:
    """

    return df[['rsid', 'effect', 'transcript']]


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


def process_variants(
    client: Client,
    fp: str,
    effect_dir: str = None,
    meta_dir: str = None,
    effect_out: str = None,
    meta_out: str = None
) -> Future:
    """
    28 min.
    """

    if not effect_dir:
        effect_dir = Path(fp).parent

    if not meta_dir:
        meta_dir = Path(fp).parent

    if not effect_out:
        effect_out = Path(effect_dir, Path(Path(fp).stem).with_suffix('.tsv')).as_posix()

    if not meta_out:
        meta_out = Path(meta_dir, Path(Path(fp).stem).with_suffix('.meta.tsv')).as_posix()

    ## Read the GVF file and annotate header fields
    raw_df = read_gvf_file(fp)

    ## Completely process the given GVF file into an intermediate format suitable
    ## for our needs
    processed_df = process_gvf(raw_df)

    ## Isolate variant metadata and effects
    effect_df = isolate_variant_effects(processed_df)
    meta_df = isolate_variant_metadata(processed_df)

    ## Persist the processed data form on the workers
    effect_df = client.persist(effect_df)
    meta_df = client.persist(meta_df)

    ## Scatter the lazy df to the workers otherwise dask complains
    effect_df = client.scatter(effect_df, broadcast=True)
    meta_df = client.scatter(meta_df, broadcast=True)

    ## Save the distributed dataframe to a temp folder
    #saved_fp = client.submit(dfio.save_distributed_dataframe, processed_df)
    effect_fp = client.submit(dfio.save_distributed_dataframe, effect_df)
    meta_fp = client.submit(dfio.save_distributed_dataframe, meta_df)

    ## Consolidate individual DF partitions from the temp folder into a single, final
    ## processed dataset
    #consolidated = client.submit(dfio.consolidate_separate_partitions, saved_fp, output)
    effect_final = client.submit(
        dfio.consolidate_separate_partitions, effect_fp, effect_out
    )
    meta_final = client.submit(dfio.consolidate_separate_partitions, meta_fp, meta_out)

    return {'effects': effect_final, 'metadata': meta_final}


def process_genes(
    client: Client,
    input: str,
    output: str
) -> Future:
    """
    28 min.
    """

    ## Read the GTF file and annotate header fields
    raw_df = read_gtf_file(input)

    ## Completely process the given GTF file into a format suitable for our needs
    processed_df = process_gtf(raw_df)

    ## Persist the processed data form on the workers
    processed_df = client.persist(processed_df)

    ## Scatter the lazy df to the workers otherwise dask complains
    processed_df = client.scatter(processed_df, broadcast=True)

    ## Save the distributed dataframe to a temp folder
    saved_fp = client.submit(dfio.save_distributed_dataframe, processed_df)

    ## Consolidate individual DF partitions from the temp folder into a single, final
    ## processed dataset
    consolidated = client.submit(dfio.consolidate_separate_partitions, saved_fp, output)

    return consolidated


def run_hg38_single_variant_processing(
    input: str,
    client: Client = None,
    outdir: str = globe._dir_hg38_variant_proc,
    **kwargs
) -> Future:
    """
    Run the variant processing pipeline on a single file (usually contains variants
    from a single chromosome). Useful for custom pipeline steps that don't have to rely
    on the run_hg38_variant_processing function to retrieve ALL variants.
    Mostly just a wrapper for the process_variants function.
    """

    if not client:
        client = get_client()

    ## If this function was sent to a worker, we should secede to free up processing room
    try:
        secede()

    except ValueError:
        ## Not a worker
        pass

    return process_variants(client, input, outdir)


def run_hg38_variant_processing(
    client: Client,
    indir: str = globe._dir_hg38_variant_raw,
    effect_dir: str = globe._dir_hg38_variant_proc,
    meta_dir: str = globe._dir_hg38_variant_meta,
    **kwargs
) -> Future:
    """
    28 min.
    """

    futures = []

    for chrom in globe._var_human_chromosomes:

        variant_fp = Path(indir, f'chromosome-{chrom}.gvf')

        future = process_variants(
            client, variant_fp, effect_dir=effect_dir, meta_dir=meta_dir
        )

        futures.append(future)

    return futures


def run_hg38_gene_processing(
    client: Client = None,
    input: str = globe._fp_hg38_gene_raw,
    output: str = globe._fp_hg38_gene_processed,
    **kwargs
) -> Future:
    """
    28 min.
    """

    if not client:
        client = get_client()

    ## If this function was sent to a worker, we should secede to free up processing room
    try:
        secede()

    except ValueError:
        ## Not a worker
        pass

    return process_genes(client, input, output)


def run_mm10_variant_processing(
    client: Client,
    input: str = globe._fp_mm10_variant_raw,
    effect_out: str = globe._fp_mm10_variant_processed,
    meta_out: str = globe._fp_mm10_variant_meta,
    **kwargs
) -> Future:
    """
    28 min.
    """

    return process_variants(client, input, effect_out=effect_out, meta_out=meta_out)


def run_mm10_gene_processing(
    client: Client,
    input: str = globe._fp_mm10_gene_raw,
    output: str = globe._fp_mm10_gene_processed,
    **kwargs
) -> Future:
    """
    28 min.
    """

    return process_genes(client, input, output=output)


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
        #cores=2,
        #processes=2,
        #memory='80GB',
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

    client.register_worker_callbacks(setup=init_logging_partial)

    ## Init logging on each worker
    #client.run(log._initialize_logging, verbose=True)

    mm10_variants = run_mm10_variant_processing(client)
    #mm10_genes = run_mm10_gene_processing(client)

    #hg38_variants = run_hg38_variant_processing(client)
    #hg38_genes = run_hg38_gene_processing(client)

    client.gather(mm10_variants)
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

