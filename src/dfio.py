#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: dfio.py
## desc: Dask DataFrame IO functions.

from dask.distributed import Future
from dask.distributed import get_client
from glob import glob
from pathlib import Path
import dask.dataframe as ddf
import logging
import shutil
import tempfile as tf

from . import log
from .globe import Globals

logging.getLogger(__name__).addHandler(logging.NullHandler())


def consolidate_separate_partitions3(partitions, tempdir: str, output: str) -> str:
    """
    Concatenate separate partition files into a single file.

    arguments
        partitions: actually a list of futures but this is never used since dask's
                    df.to_csv function doesn't return anything when compute=false.
                    This argument is simply used to force dask to wait until all
                    separate partitions have been written to disk.
        tempdir:    temp directory containing partitions
        output:     output filepath

    returns
        the output filepath
    """

    log._logger.info(f'Finalizing {output}')

    first = True

    with open(output, 'w') as ofl:
        for part in glob(Path(tempdir, '*').as_posix()):
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
    shutil.rmtree(tempdir)

    return output


def save_dataframe_async(df: ddf.DataFrame, output: str) -> Future:
    """
    Save a distributed dask dataframe to disk in a manner that dosen't block other
    computations.
    The function converts each dask dataframe partition to a series of delayed objects,
    writes each partition to disk simultaneously if possible, then merges the separate
    files into a single file.

    arguments
        df:     dask dataframe
        output: output filepath

    returns
        the output filepath
    """

    client = get_client()

    ## The temp directory should be a directory that all workers can access, otherwise
    ## when the time comes to concatenate all the files, workers might not have access
    tempdir = tf.mkdtemp(dir=Globals().dir_data)

    ## Save partitions as they complete to a temp directory accessible by all workers.
    ## By specifying compute=false, we return a series of delayed objects and don't block
    delayeds = df.to_csv(
        Path(tempdir, '*').as_posix(),
        sep='\t',
        index=False,
        na_rep='NA', compute=False
    )

    ## Convert delayed instances to futures
    futures = [client.compute(d) for d in delayeds]

    ## Consolidate separate files into a single file
    future = client.submit(consolidate_separate_partitions3, futures, tempdir, output)

    return future

