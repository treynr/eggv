#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: dfio.py
## desc: DataFrame IO functions used by more than one module.

from dask.base import tokenize
from dask.distributed import Client
from dask.distributed import as_completed
from dask.distributed import get_client
from pathlib import Path
import dask.dataframe as ddf
import logging
import pandas as pd
import shutil
import tempfile as tf

from . import globe
from . import log

logging.getLogger(__name__).addHandler(logging.NullHandler())


def save_distributed_dataframe_partitions(df: ddf.DataFrame, outdir: str = None) -> str:
    """
    Write each dataframe partition, from a distributed dask dataframe, to a folder.

    arguments
        df:     a dask dataframe
        outdir: output directory to write partitions to

    returns
        the output directory path
    """

    if not outdir:
        outdir = tf.mkdtemp(dir=globe._dir_data)

    df.to_csv(outdir, sep='\t', index=False, na_rep='NA')

    return outdir


def consolidate_separate_partitions(indir: str, output: str) -> str:
    """
    Read separate dask dataframe partition files from a single folder and
    concatenate them into a single file.

    arguments
        indir:  input directory filepath
        output: output filepath

    returns
        the output filepath
    """

    log._logger.info(f'Finalizing {output}')

    first = True

    with open(output, 'w') as ofl:
        for ifp in Path(indir).iterdir():
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
    shutil.rmtree(indir)

    return output


def save_distributed_dataframe(df: ddf.DataFrame, output: str) -> str:
    """
    Combines the previous two functions, save_distributed_dataframe_partitions and
    consolidate_separate_partitions into a single function.

    arguments
        df:     a dask dataframe
        output: output filepath

    returns
        the output filepath
    """

    outdir = save_distributed_dataframe_partitions(df)

    return consolidate_separate_partitions(outdir, output)


def save_dataset(
    df: ddf.DataFrame,
    name: str = 'dataset',
    ext: str = '.tsv',
    outdir: str = './',
    output: str = None
) -> str:
    """
    Terrible ass generic name but idk what else to name it and it's used throughout by
    different parts of the pipeline. Basically a wrapper for save_distributed_dataframe
    but will autogenerate an output path if one isn't given.
    If you're calling this function using client.submit, you should scatter the dataframe
    beforehand or horrible shit is gonna happen to you.

    arguments
        df:     dask dataframe
        name:   an input path or filename used to generate a filename for the output path
        ext:    file extension
        outdir: output directory
        output: output filepath

    returns
        the output filepath
    """

    ## Grab the filename part of name, replace its extension with ext, and attach it to
    ## the given output directory
    if not output:
        output = Path(outdir, Path(Path(name).stem).with_suffix(ext)).as_posix()

    ## Write out the dataframe partition and consolidate them into a single file
    ## If this function (save_dataset) or it's wrapper isn't executed using
    ## client.submit, this dfio function will block the main thread.
    return save_distributed_dataframe(df, output)


#def consolidate_separate_partitions2(partitions, indir: str, output: str) -> str:
def consolidate_separate_partitions2(partitions, output: str) -> str:
    """
    Read separate dask dataframe partition files from a single folder and
    concatenate them into a single file.

    arguments
        indir:  input directory filepath
        output: output filepath

    returns
        the output filepath
    """

    log._logger.info(f'Finalizing {output}')

    first = True

    #client = get_client()

    #futures = [client.compute(p) for p in partitions]

    with open(output, 'w') as ofl:
    #for fut, partition_fp in as_completed(futures, with_results=True):
        for part in partitions:
            #log._logger.info(part)
            #log._logger.info(fut)
            #log._logger.info(type(partition_fp))
            #log._logger.info(partition_fp)
            #log._logger.info(fut.result())
            #exit()
            with open(part, 'r') as ifl:

                ## If this is the first file being read then we include the header
                if first:
                    ofl.write(ifl.read())

                    first = False

                ## Otherwise skip the header so it isn't repeated throughout
                else:
                    next(ifl)

                    ofl.write(ifl.read())

    ## Assume the input directory is a temp one and remove it since it's no longer needed
    shutil.rmtree(Path(partitions[0]).parent)

    return output


def write_dataframe_partition(df: pd.DataFrame, output: str) -> str:
    """
    Write each dataframe partition, from a distributed dask dataframe, to a folder.

    arguments
        df:     a dask dataframe
        outdir: output directory to write partitions to

    returns
        the output directory path
    """

    df.to_csv(output, sep='\t', index=False, na_rep='NA')

    return output


def write_dataframe_partitions(df: ddf.DataFrame) -> str:
    """
    Write each dataframe partition, from a distributed dask dataframe, to a folder.

    arguments
        df:     a dask dataframe
        outdir: output directory to write partitions to

    returns
        the output directory path
    """

    client = get_client()
    outdir = tf.mkdtemp(dir=globe._dir_data)
    paths = []

    for i, partition in enumerate(df.to_delayed()):
        path = Path(outdir, tokenize(partition.key)).resolve().as_posix()

        paths.append(client.submit(
            write_dataframe_partition, client.compute(partition), path
        ))


    return paths

def save_dataset_in_background(
    df: ddf.DataFrame,
    name: str = 'dataset',
    ext: str = '.tsv',
    outdir: str = './',
    output: str = None,
    client: Client = None
) -> str:
    """
    Terrible ass generic name but idk what else to name it and it's used throughout by
    different parts of the pipeline. Basically a wrapper for save_distributed_dataframe
    but will autogenerate an output path if one isn't given.
    If you're calling this function using client.submit, you should scatter the dataframe
    beforehand or horrible shit is gonna happen to you.

    arguments
        df:     dask dataframe
        name:   an input path or filename used to generate a filename for the output path
        ext:    file extension
        outdir: output directory
        output: output filepath

    returns
        the output filepath
    """

    ## Grab the filename part of name, replace its extension with ext, and attach it to
    ## the given output directory
    if not output:
        output = Path(outdir, Path(Path(name).stem).with_suffix(ext)).as_posix()

    client = get_client() if client is None else client

    log._logger.info('Writing dataframe partitions')
    path_futures = write_dataframe_partitions(df)

    return client.submit(consolidate_separate_partitions2, path_futures, output)

    """
    tmp_out = tf.mkdtemp(dir=globe._dir_data)

    ## Convert the dataframe to a list of futures
    #delayeds = df.to_csv(tmp_out, sep='\t', index=False, na_rep='NA', compute=False)
    log._logger.info('Delaying dataframe')
    delayeds = df.to_delayed()
    #futures = [client.compute(d) for d in delayeds]
    log._logger.info('Made futures')

    ## Write out the dataframe partition and consolidate them into a single file
    ## If this function (save_dataset) or it's wrapper isn't executed using
    ## client.submit, this dfio function will block the main thread.
    #return client.submit(consolidate_separate_partitions2, futures, tmp_out, output)
    return client.submit(consolidate_separate_partitions2, delayeds, tmp_out, output)
    """

