#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: process.py
## desc: Functions for processing and formatting Ensembl variation and gene builds.

from dask.distributed import Client
from dask.distributed import Future
from dask.distributed import get_client
from glob import glob
from pathlib import Path
from typing import Dict
from typing import List
import dask.dataframe as ddf
import logging
import numpy as np

from . import dfio
from . import log
from .globe import Globals

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

    ## Sometimes type inference fails since not all chromosomes (seqid) are numbers, and
    ## contigs are often listed too.
    return ddf.read_csv(
        fp,
        sep='\t',
        comment='#',
        header=None,
        names=header,
        dtype={'seqid': 'object'},
        blocksize='150MB'
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
        fp,
        sep='\t',
        comment='#',
        header=None,
        names=header,
        dtype={'seqname': 'object'}
    )


def _process_gvf(df: ddf.DataFrame) -> ddf.DataFrame:
    """
    Process and format GVF fields into an intermediate data representation.
    Filters out fields that are not necessary (for our purposes), extracts variant and
    reference alleles, extracts the MAF, parses out variant effects and adds new rows
    for each effect + modified transcript.

    arguments
        df: unprocessed dask dataframe

    returns
        a processed dataframe
    """

    ## Only keep fields we need
    df = df[['seqid', 'start', 'end', 'attr']]

    ## Rename to chromosome
    df = df.rename(columns={'seqid': 'chromosome'})

    ## Add the 'chr' prefix since that's usually pretty standard and in some cases,
    ## required by other third party tools
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
    df['maf'] = df.maf.astype(np.float32)

    ## Attempt to extract the variant effect, of which there may be several. The variant
    ## effect attribute looks something like this:
    ## Variant_effect=non_coding_transcript_variant 0 ncRNA ENST00000566940,
    ## intron_variant 0 primary_transcript ENST00000566940,
    ## non_coding_transcript_variant 1 ncRNA ENST00000566940,
    df['effect'] = df.attr.str.extract(r'Variant_effect=(.+?);', expand=False)
    df['effect'] = df.effect.fillna('intergenic')

    ## Each variant can have multiple effects, so we split the variant effects into their
    ## own individual rows while still retaining the original variant metadata per new row
    df = df.assign(effect=df.effect.str.split(',')).explode('effect')

    ## Fill in missing effects and split to separate effects/transcripts
    df['effect'] = df.effect.fillna('intergenic')
    df['effect'] = df.effect.str.split(' ')

    ## Some transcripts will have NaN values, these are replaced by 'NA' later on
    ## in the to_csv function
    df['transcript'] = df.effect.str.get(3).fillna('')
    df['transcript'] = df.transcript.astype(str)

    ## Replace with the actual effect
    df['effect'] = df.effect.str.get(0)

    ## Keep only things we need
    return df[[
        'chromosome', 'rsid', 'start', 'end', 'observed', 'maf', 'effect', 'transcript'
    ]]


def _process_gtf(df: ddf.DataFrame) -> ddf.DataFrame:
    """
    Process and format GTF fields into an intermediate data representation.
    Filters out fields that are not necessary (for our purposes), extracts variant and
    reference alleles, extracts the MAF, parses out variant effects and adds new rows
    for each effect + transcript.

    arguments
        df: unprocessed dataframe

    returns
        a processed dataframe
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


def _process_genes(input: str) -> ddf.DataFrame:
    """
    Read and process an input file containing an organism's Ensembl gene build.
    Persists the processed dask dataframe onto the dask cluster.

    arguments
        input: input filepath to the gene build

    returns
        a dataframe
    """

    ## Read the GTF file and annotate header fields
    raw_df = _read_gtf_file(input)

    ## Completely process the given GTF file into a format suitable for our needs
    processed_df = _process_gtf(raw_df)

    return processed_df


def _process_variants(fp: str) -> ddf.DataFrame:
    """
    Read and process an input file containing an organism's Ensembl variant build.

    arguments
        fp: filepath to a variant (GVF) file

    returns
        a dask dataframe
    """

    ## Read the GVF file and annotate header fields
    raw_df = _read_gvf_file(fp)

    ## Completely process the given GVF file
    return _process_gvf(raw_df)


def isolate_variant_effects(df: ddf.DataFrame, npartitions: int = 350) -> ddf.DataFrame:
    """
    Given a dataframe of processed variants, isolate and retain variant effects altered
    transcripts.

    arguments
        df:          dataframe containing processed variant data
        npartitions: number of partitions to split the dataframe into after removing
                     duplicates. A partition of 1 is NOT recommended for large datasets.

    returns
        a dataframe
    """

    ## The duplicates should only be present within partitions since we haven't
    ## repartitioned yet and variant effects were added per partition
    return (
        df[['rsid', 'effect', 'transcript']]
            .map_partitions(lambda d: d.drop_duplicates())
    )


def isolate_variant_metadata(df: ddf.DataFrame, npartitions: int = 350) -> ddf.DataFrame:
    """
    Given a dataframe of processed variants, isolate and retain variant metadata.

    arguments
        df:          dataframe containing processed variant data
        npartitions: number of partitions to split the dataframe into after removing
                     duplicates. A partition of 1 is NOT recommended for large datasets.

    returns
        a dataframe
    """

    ## The duplicates should only be present within partitions since we haven't
    ## repartitioned yet and variant effects were added per partition
    return (
        df[['chromosome', 'start', 'end', 'rsid', 'observed', 'maf']]
            .map_partitions(lambda d: d.drop_duplicates(subset='rsid'))
    )


def run_variant_processing_pipeline(
    input: str,
    client: Client = None
) -> Dict[str, ddf.DataFrame]:
    """
    Completely process a set of variants from a given GVF file, isolate and return
    variant metadata and effects.

    arguments
        input:  input variant filepath in GVF format
        client: dask client object

    returns
        a dict containing two keys
            dict:
                effects:  dataframe containing variant effects
                metadata: dataframe containing variant metadata
    """

    client = get_client() if client is None else client

    ## Read and process to an intermediate format
    processed_df = _process_variants(input)
    processed_df = client.persist(processed_df)

    ## Isolate variant metadata and effects
    effect_df = isolate_variant_effects(processed_df)
    meta_df = isolate_variant_metadata(processed_df)

    return {
        'effects': effect_df,
        'metadata': meta_df,
    }


## ugh these function names are so fucking long
def run_complete_hg38_variant_processing_pipeline(
    indir: str = None,
    effect_dir: str = None,
    meta_dir: str = None,
    client: Client = None
) -> List[Future]:
    """
    Run the variant processing pipeline step for all hg38 build chromosomes.
    Basically a wrapper for run_variant_processing_pipeline but also saves the results
    to disk.

    arguments
        indir:      directory filepath containing hg38 variant files
        effect_dir: output directory to save variant effects
        meta_dir:   output directory to save variant metadata
        client:     dask client object

    returns
        a list of futures containing dicts to effect and metadata filepaths for variants
        on each chromosome. Dicts have the following format:
            dict:
                effects:  future containing filepath to processed effects
                metadata: future containing filepath to processed metadata
    """

    ## This shouldn't be necessary since this function should never be called when
    ## a mouse genome build is specified but w/e
    globals = Globals().reinitialize(build='hg38')

    if indir is None:
        indir = globals.dir_variant_raw

    if effect_dir is None:
        effect_dir = globals.dir_variant_effects

    if meta_dir is None:
        meta_dir = globals.dir_variant_meta

    client = get_client() if client is None else client

    futures = []

    ## The input directory should have a bunch of GVF files if no custom output filenames
    ## were used
    for fp in glob(f'{indir}/*.gvf'):

        log._logger.info(f'Starting work on {fp}')

        results = run_variant_processing_pipeline(fp)

        ## Persist the processed dataframes onto the cluster workers
        effects = client.persist(results['effects'])
        metadata = client.persist(results['metadata'])

        ## Save the processed dataframes in the background
        effect_future = dfio.save_dataset_in_background3(
            effects,
            output=Path(effect_dir, Path(Path(fp).stem).with_suffix('.tsv')).as_posix()
        )

        meta_future = dfio.save_dataset_in_background3(
            metadata,
            output=Path(meta_dir, Path(Path(fp).stem).with_suffix('.tsv')).as_posix()
        )

        futures.append({
            'effects': effect_future,
            'metadata': meta_future
        })

    return futures


def run_complete_mm10_variant_processing_pipeline(
    input: str = None,
    effect_path: str = None,
    meta_path: str = None,
) -> Dict[str, Future]:
    """
    28 min.
    """

    ## This shouldn't be necessary since this function should never be called when
    ## a human genome build is specified but w/e
    globals = Globals().reinitialize(build='mm10')

    if input is None:
        input = globals.fp_variant_raw

    if effect_path is None:
        effect_path = globals.fp_variant_effects

    if meta_path is None:
        meta_path = globals.fp_variant_meta

    results = run_variant_processing_pipeline(input)

    effect_future = dfio.save_dataset_in_background(
        results['effects'], output=effect_path
    )
    meta_future = dfio.save_dataset_in_background(
        results['metadata'], output=meta_path
    )

    ## Return the filepaths from the dataframe IO functions
    return {
        'effects': effect_future,
        'metadata': meta_future
    }


def run_gene_processing_pipeline(input: str) -> ddf.DataFrame:
    """
    Wrapper function for _process_genes which does all the heavy lifting.
    """

    df = _process_genes(input)

    return df


def run_complete_hg38_gene_processing_pipeline(
    input: str = None,
    output: str = None,
    dedup_output: str = None,
) -> Future:
    """
    28 min.
    """

    globals = Globals().reinitialize(build='hg38')

    if input is None:
        input = globals.fp_gene_raw

    if output is None:
        output = globals.fp_gene_meta

    if dedup_output is None:
        dedup_output = globals.fp_gene_dedup

    ## Run the pipeline and process the genes
    gene_df = run_gene_processing_pipeline(input)

    ## Write the dataframe to a file
    gene_future = dfio.save_dataset_in_background(gene_df, output=output)

    ## Generate a deduplicated version based on genes alone
    dedup_future = dfio.save_dataset_in_background(
        gene_df.drop_duplicates(subset=['gene_id'], split_out=20), output=dedup_output
    )

    return [gene_future, dedup_future]


def run_complete_mm10_gene_processing_pipeline(
    input: str = None,
    output: str = None,
    dedup_output: str = None,
    **kwargs
) -> Future:
    """
    28 min.
    """

    globals = Globals().reinitialize(build='mm10')

    if input is None:
        input = globals.fp_gene_raw

    if output is None:
        output = globals.fp_gene_meta

    if dedup_output is None:
        dedup_output = globals.fp_gene_dedup

    ## Run the pipeline and process the genes
    gene_df = run_gene_processing_pipeline(input)

    ## Write the dataframe to a file
    gene_future = dfio.save_dataset_in_background(gene_df, output=output)

    ## Generate a deduplicated version based on genes alone
    dedup_future = dfio.save_dataset_in_background(
        gene_df.drop_duplicates(subset=['gene_id'], split_out=150), output=dedup_output
    )

    return [gene_future, dedup_future]

