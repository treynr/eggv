#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: pipeline.py
## desc: Functions for composing the entire EGG:V pipeline from individual steps.
## auth: TR

from dask.distributed import as_completed
from glob import glob
from pathlib import Path
import logging

from . import __version__
from . import annotate
from . import configuration
from . import cluster
from . import dfio
from . import log
from . import process
from . import retrieve
from .globe import Globals

logging.getLogger(__name__).addHandler(logging.NullHandler())


def _initialize_cluster(config: configuration.Config):
    """
    Initialize the cluster of workers based on pipeline configuration.

    arguments
        config: pipeline configuration

    returns
        a dask client
    """

    return cluster.initialize_cluster(
        hpc=config.config['resources']['environment']['hpc'],
        temp=config.config['directories']['temp'],
        **config.config['resources']
    )


def _initialize_globals(config: configuration.Config) -> Globals:
    """
    Initialize data directories and filepaths.

    arguments
        config: pipeline configuration
    """

    return Globals(
        datadir=config.config['directories']['data'],
        build=config.config['species']
    )


def _run_complete_hg38_pipeline(config: configuration.Config) -> None:
    """
    Run the complete variant pipeline for the hg38 genome build.

    arguments
        config: pipeline configuration
    """

    log._logger.info(f'Initializing cluster...')

    client = _initialize_cluster(config)
    globals = _initialize_globals(config)

    ## Retrieve variants and genes from Ensembl
    #gene_future = retrieve.run_hg38_gene_retrieval()
    #variant_futures = retrieve.run_hg38_variant_retrieval()
    saved_futures = []

    variant_futures = [
        client.submit(lambda x: x, fp) for fp in glob(f'{globals.dir_variant_raw}/*.gvf')
    ]
    gene_future = client.submit(lambda x: x, globals.fp_gene_raw)

    ## Process the genes whenever they're ready
    processed_genes = client.submit(process.run_gene_processing_pipeline, gene_future)

    ## Save processed genes for later use. result() should return immediately b/c
    ## this is a future of a future
    saved_futures.append(client.submit(
        dfio.save_dataset_in_background3,
        processed_genes,
        output=globals.fp_gene_meta
    ).result())

    ## As individual variant files are retrieved and decompressed...
    for _, gvf in as_completed(variant_futures, with_results=True):

        log._logger.info(f'Processing variants from {gvf}...')

        ## Load, parse and process the GVF file
        processed_vars = process.run_variant_processing_pipeline(gvf)

        ## Persist the collections onto the workers
        effect_df = client.persist(processed_vars['effects'])
        meta_df = client.persist(processed_vars['metadata'])

        ## Save the variant effects and metadata for use later
        saved_futures.append(dfio.save_dataset_in_background3(
            effect_df,
            output=Path(
                globals.dir_variant_effects, Path(gvf).with_suffix('.tsv').name
            ).as_posix()
        ))
        saved_futures.append(dfio.save_dataset_in_background3(
            meta_df,
            output=Path(
                globals.dir_variant_meta, Path(gvf).with_suffix('.tsv').name
            ).as_posix()
        ))

        ## Block until the genes have finished processing
        annotations = annotate.run_annotation_pipeline(
            effect_df, processed_genes.result()
        )

        ## Persist separated intergenic and intragenic annotations onto workers
        intergenic = client.persist(annotations['intergenic'])
        intragenic = client.persist(annotations['intragenic'])

        log._logger.info(f'Saving annotations...')

        ## Save them whenever possible. result() should return immediately b/c this is
        ## a Future of a Future.
        saved_futures.append(client.submit(
            dfio.save_dataset_in_background3,
            intergenic,
            output=Path(globals.dir_annotated_inter, Path(gvf).with_suffix('.tsv').name)
        ).result())

        saved_futures.append(client.submit(
            dfio.save_dataset_in_background3,
            intragenic,
            output=Path(globals.dir_annotated_intra, Path(gvf).with_suffix('.tsv').name)
        ).result())

    log._logger.info(f'Waiting for the pipeline to finish...')

    ## Wait for everything to finish
    client.gather(saved_futures)
    client.close()

    log._logger.info(f'Done!')



def _run_complete_mm10_pipeline(config: configuration.Config) -> None:
    """
    Run the complete variant pipeline for the mm10 genome build.

    arguments
        config: pipeline configuration
    """

    log._logger.info(f'Initializing cluster...')

    client = _initialize_cluster(config)
    globals = _initialize_globals(config)

    ## Retrieve variants and genes from Ensembl
    #gene_future = retrieve.run_mm10_gene_retrieval()
    #variant_futures = retrieve.run_mm10_variant_retrieval()
    saved_futures = []

    variant_future = client.submit(lambda x: x, globals.fp_variant_raw)
    gene_future = client.submit(lambda x: x, globals.fp_gene_raw)

    ## Process the genes whenever they're ready
    processed_genes = client.submit(process.run_gene_processing_pipeline, gene_future)

    ## Save processed genes for later use. result() should return immediately b/c
    ## this is a future of a future
    saved_futures.append(client.submit(
        dfio.save_dataset_in_background3,
        processed_genes,
        output=globals.fp_gene_meta
    ).result())

    log._logger.info(f'Processing variants...')

    ## Block until the variants are done downloading/decompressing
    variants = variant_future.result()
    processed_vars = process.run_variant_processing_pipeline(variants)

    ## Persist the collections onto the workers
    effect_df = client.persist(processed_vars['effects'])
    meta_df = client.persist(processed_vars['metadata'])

    ## Save the variant effects and metadata for use later
    saved_futures.append(dfio.save_dataset_in_background3(
        effect_df,
        output=globals.fp_variant_effects
    ))
    saved_futures.append(dfio.save_dataset_in_background3(
        meta_df,
        output=globals.fp_variant_meta
    ))

    ## Block until the genes have finished processing
    annotations = annotate.run_annotation_pipeline(
        effect_df, processed_genes.result()
    )

    ## Persist separated intergenic and intragenic annotations onto workers
    intergenic = client.persist(annotations['intergenic'])
    intragenic = client.persist(annotations['intragenic'])

    ## Save them whenever possible. result() should return immediately b/c this is
    ## a Future of a Future.
    saved_futures.append(client.submit(
        dfio.save_dataset_in_background3,
        intergenic,
        output=globals.fp_annotated_inter
    ).result())

    saved_futures.append(client.submit(
        dfio.save_dataset_in_background3,
        intragenic,
        output=globals.fp_annotated_intra
    ).result())

    log._logger.info(f'Waiting for the pipeline to finish...')

    ## Wait for everything to finish
    client.gather(saved_futures)
    client.close()

    log._logger.info(f'Done!')


def run_complete_pipeline(config: configuration.Config) -> None:
    """
    Run the complete variation pipeline.

    arguments
        config: pipeline configuration
    """

    if config.config['species'] == 'hg38':
        _run_complete_hg38_pipeline(config)

    else:
        _run_complete_mm10_pipeline(config)

