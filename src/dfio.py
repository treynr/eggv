#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: dfio.py
## desc: DataFrame IO functions used by more than one module.

from dask.distributed import get_client
from pathlib import Path
import dask.dataframe as ddf
import logging
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

