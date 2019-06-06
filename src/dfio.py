#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: dfio.py
## desc: DataFrame IO functions used by more than one module.

from pathlib import Path
import dask.dataframe as ddf
import logging
import shutil
import tempfile as tf

from . import globe
from . import log

logging.getLogger(__name__).addHandler(logging.NullHandler())


def save_distributed_dataframe(df: ddf.DataFrame, outdir: str = None) -> str:
    """
    Write variants to a file. Assumes these variants have been distributed using dask
    dataframes. Writes each dataframe partition to a temp folder.
    """

    if not outdir:
        outdir = tf.mkdtemp(dir=globe._dir_data)

    #print(f'saving {outdir}')
    #print(type(df))
    #print(df.head())

    df.to_csv(outdir, sep='\t', index=False, na_rep='NA')

    return outdir


def consolidate_separate_partitions(input: str, output: str) -> str:
    """
    Read separate dask dataframe partition files and concatenate them
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

    return output

