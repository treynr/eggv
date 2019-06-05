#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: process.py
## desc: Functions for processing and formatting Ensembl variation and gene builds.

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


def process_variants(
    client: Client,
    fp: str,
    outdir: str = None,
    output: str = None
) -> Future:
    """
    28 min.
    """

    if not outdir:
        outdir = Path(fp).parent

    if not output:
        output = Path(outdir, Path(Path(fp).stem).with_suffix('.tsv')).as_posix()

    ## Read the GVF file and annotate header fields
    raw_df = read_gvf_file(fp)

    ## Completely process the given GVF file into a format suitable for our needs
    processed_df = process_gvf(raw_df)

    ## Persist the processed data form on the workers
    processed_df = client.persist(processed_df)

    ## Scatter the lazy df to the workers otherwise dask complains
    processed_df = client.scatter(processed_df, broadcast=True)

    ## Save the distributed dataframe to a temp folder
    saved_fp = client.submit(save_distributed_variants, processed_df)

    ## Consolidate individual DF partitions from the temp folder into a single, final
    ## processed dataset
    consolidated = client.submit(consolidate_saved_variants, saved_fp, output)

    return consolidated


def run_hg38_variant_processing(
    client: Client,
    indir: str = globe._dir_hg38_variant_raw,
    outdir: str = globe._dir_hg38_variant_proc
) -> Future:
    """
    28 min.
    """

    futures = []

    for chrom in globe._var_human_chromosomes:

        variant_fp = Path(indir, f'chromosome-{chrom}.gvf')

        future = process_variants(client, variant_fp, outdir)

        futures.append(future)

    return futures


def run_mm10_variant_processing(
    client: Client,
    input: str = globe._fp_mm10_variant_raw,
    output: str = globe._fp_mm10_variant_processed
) -> Future:
    """
    28 min.
    """

    return process_variants(client, input, output=output)


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

    #cluster.adapt(minimum=10, maximum=38)

    #client = Client(cluster)

    init_logging_partial = partial(log._initialize_logging, verbose=True)

    client.register_worker_callbacks(setup=init_logging_partial)

    ## Init logging on each worker
    #client.run(log._initialize_logging, verbose=True)

    mm10_futures = run_mm10_variant_processing(client)

    client.gather(mm10_futures)

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

