#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: retrieve.py
## desc: Functions for retrieving and decompressing genomic feature (variant and gene)
##       builds from Ensembl.

from dask.distributed import Client
from dask.distributed import Future
from dask.distributed import LocalCluster
from pathlib import Path
from typing import List
import gzip
import logging
import requests as req
import shutil

from . import globe
from . import log

logging.getLogger(__name__).addHandler(logging.NullHandler())


def _download(url, output):
    """
    Download a file at the given URL and save it to disk.

    arguments
        url:    file URL
        output: output filepath
    """

    try:
        response = req.get(url, stream=True)

        ## Throw exception for any HTTP errors
        response.raise_for_status()

        with open(output, 'wb') as fl:
            for chunk in response.iter_content(chunk_size=1024):
                fl.write(chunk)

    except Exception as e:
        log._logger.error('Request exception occurred: %s', e)
        raise


def _unzip(fp: str, output: str = None, **kwargs) -> None:
    """
    Unzip a gzipped file to the given output path.

    arguments
        fp:     zipped input filepath
        output: output path
        kwargs: used to trick dask into creating Future dependencies for this function
    """

    log._logger.info('Decompressing: %s', fp)

    ## Assuming the input has a file extension like '.tar.gz' this well get rid of '.gz'
    if not output:
        output = Path(fp).with_suffix('')

    with gzip.open(fp, 'rb') as gfl, open(output, 'wb') as ufl:
        shutil.copyfileobj(gfl, ufl)

    return output


def _download_ensembl_build(url: str, output: str, force: bool = False):
    """
    Download a genomic feature build from Ensembl. See globe.py for relevant URLs.

    arguments
        url:    URL pointing to the Ensembl feature build
        output: output filepath of the downloaded, compressed build
        force:  if true, retrieve the dataset even if it already exists locally
    """

    log._logger.info('Downloading genomic feature build: %s', url)

    if Path(output).exists() and not force:
        log._logger.warning(
            'The Ensembl build (%s) already exists, use force=True to retrieve it', output
        )

        return

    _download(url, output)


def download_hg38_gene_build(
    url: str = globe._url_hg38_gene,
    output: str = globe._fp_hg38_gene_compressed,
    force: bool = False
) -> str:
    """
    Download the hg38 gene feature build.

    arguments
        See download_ensembl_build for argument descriptions. This function is simply a
        wrapper.

    returns
        the output filepath
    """

    _download_ensembl_build(url, output, force)

    return output


def download_hg38_variant_build(
    chrom: str,
    url: str = globe._url_hg38_variation,
    output: str = globe._dir_hg38_variant_raw,
    force: bool = False
) -> str:
    """
    Download the hg38 variant feature build for a single chromosome.

    arguments
        chrom:  the chromosome number/letter
        url:    partially completed URL to the Ensembl variant build
        output: directory where the downloaded variant build will be stored
        force:  if true, retrieve the dataset even if it already exists locally

    returns
        the output filepath
    """

    ## Add the chromosome number to the URL, which should now resolve correctly
    url = url.format(chrom)

    ## Create the full output filepath
    output = Path(output, f'chromosome-{chrom}.gvf.gz').as_posix()

    _download_ensembl_build(url, output, force)

    return output


def download_mm10_gene_build(
    url: str = globe._url_mm10_gene,
    output: str = globe._fp_mm10_gene_compressed,
    force: bool = False
) -> str:
    """
    Download the mm10 gene feature build.

    arguments
        See download_ensembl_build for argument descriptions. This function is simply a
        wrapper.

    returns
        the output filepath
    """

    _download_ensembl_build(url, output, force)

    return output


def download_mm10_variant_build(
    url: str = globe._url_mm10_variation,
    output: str = globe._fp_mm10_variant_compressed,
    force: bool = False
) -> str:
    """
    Download the mm10 variant feature build.

    arguments
        See download_ensembl_build for argument descriptions. This function is simply a
        wrapper.

    returns
        the output filepath
    """

    _download_ensembl_build(url, output, force)

    return output


def run_hg38_variant_retrieval(client: Client, force: bool = False) -> List[Future]:
    """
    Executes the genomic variant retrieval step of the ETL pipeline for hg38 variants.

    arguments
        client: a dask Client object
        force:  if true, datasets will be downloaded even if they exist locally

    returns
        a list of Futures, one per chromosome variant build
    """

    futures = []

    for chrom in globe._var_human_chromosomes:

        ## Download from Ensembl
        dl = client.submit(download_hg38_variant_build, chrom, force=force)

        ## Decompress
        dl_unzip = client.submit(_unzip, dl)

        futures.append(dl_unzip)

    return futures


def run_hg38_gene_retrieval(client: Client, force: bool = False) -> Future:
    """
    Executes the genomic variant retrieval step of the ETL pipeline for hg38 variants.

    arguments
        client: a dask Client object
        force:  if true, datasets will be downloaded even if they exist locally

    returns
        a Future
    """

    ## Download from Ensembl
    dl = client.submit(download_hg38_gene_build, force=force)

    ## Decompress
    dl_unzip = client.submit(_unzip, dl)

    return dl_unzip


def run_mm10_variant_retrieval(client: Client, force: bool = False) -> List[Future]:
    """
    Executes the genomic variant retrieval step of the ETL pipeline for mm10 variants.

    arguments
        client: a dask Client object
        force:  if true, datasets will be downloaded even if they exist locally

    returns
        a Future
    """

    ## Download from Ensembl
    dl = client.submit(download_mm10_variant_build, force=force)

    ## Decompress
    dl_unzip = client.submit(_unzip, dl)

    return dl_unzip


def run_mm10_gene_retrieval(client: Client, force: bool = False) -> Future:
    """
    Executes the genomic variant retrieval step of the ETL pipeline for mm10 variants.

    arguments
        client: a dask Client object
        force:  if true, datasets will be downloaded even if they exist locally

    returns
        a Future
    """

    ## Download from Ensembl
    dl = client.submit(download_mm10_gene_build, force=force)

    ## Decompress
    dl_unzip = client.submit(_unzip, dl)

    return dl_unzip


if __name__ == '__main__':

    client = Client(LocalCluster(
        n_workers=8,
        processes=True
    ))

    log._initialize_logging(verbose=True)

    ## Init logging on each worker
    client.run(log._initialize_logging, verbose=True)

    hg38_variants = run_hg38_variant_retrieval(client, force=False)
    hg38_genes = run_hg38_gene_retrieval(client, force=False)

    mm10_variants = run_mm10_variant_retrieval(client, force=False)
    mm10_genes = run_mm10_gene_retrieval(client, force=False)

    client.gather([hg38_variants, hg38_genes, mm10_variants, mm10_genes])

    #client.gather(hg38_variants)
    #client.gather(hg38_genes)
    #client.gather([mm10_variants, mm10_genes])

    client.close()

