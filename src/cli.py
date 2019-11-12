#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: cli.py
## desc: CLI interface for EGG:V.
## auth: TR

from dask.distributed import get_client
import click
import functools
import logging

from . import __version__
from . import annotate
from . import configuration
from . import cluster
from . import log
from . import pipeline
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


def _initialize_globals(config: configuration.Config) -> None:
    """
    Initialize data directories and filepaths.

    arguments
        config: pipeline configuration
    """

    Globals(datadir=config.config['directories']['data'], build=config.config['species'])


def _run_retrieve_step(config: configuration.Config) -> None:
    """
    Run the data retrieval step of the pipeline.

    arguments
        config: pipeline configuration
    """

    client = _initialize_cluster(config)
    force = config.config['overwrite']
    _initialize_globals(config)

    if config.config['species'] == 'hg38':
        ## This is actually a list of futures, one per chromosome
        variant_future = retrieve.run_hg38_variant_retrieval(force=force)
        gene_future = retrieve.run_hg38_gene_retrieval(force=force)

    else:
        variant_future = retrieve.run_mm10_variant_retrieval(force=force)
        gene_future = retrieve.run_mm10_gene_retrieval(force=force)

    ## Wait for downloading and decompression to finish
    client.gather([variant_future, gene_future])
    client.close()


def _run_process_step(config: configuration.Config) -> None:
    """
    Run the gene and variant processing step of the pipeline.
    Assumes the required files have already been retrieved and stored to disk.

    arguments
        config: pipeline configuration
    """

    _initialize_cluster(config)
    _initialize_globals(config)

    if config.config['species'] == 'hg38':
        ## This is actually a list of futures, one per chromosome
        variants = process.run_complete_hg38_variant_processing_pipeline()
        genes = process.run_complete_hg38_gene_processing_pipeline()

    else:
        variants = process.run_complete_mm10_variant_processing_pipeline() # type: ignore
        genes = process.run_complete_mm10_gene_processing_pipeline()

    client = get_client()

    ## Wait for processing to finish
    client.gather(variants)
    client.gather(genes)
    client.close()


def _run_annotate_step(config: configuration.Config) -> None:
    """
    Run the variant annotation step of the pipeline.
    Assumes the required files have already been retrieved, processed, and stored to disk.

    arguments
        config: pipeline configuration
    """

    client = _initialize_cluster(config)
    _initialize_globals(config)

    if config.config['species'] == 'hg38':
        ## This is actually a list of futures, one per chromosome
        ann_future = annotate.run_complete_hg38_annotation_pipeline()

    else:
        ann_future = annotate.run_complete_mm10_annotation_pipeline()

    ## Wait for processing to finish
    client.gather(ann_future)
    client.close()


def _handle_common_options(ctx, conpath=None, force=None, species=None):
    """
    Handle common click options.
    """

    config = configuration.Config()

    ## Update the config or look in places where it may exist
    if conpath:
        config.reload(conpath)

    if force is not None:
        config.config['overwrite'] = force

    if species is not None:
        config.config['species'] = species

    ctx.ensure_object(dict)
    ctx.obj['config'] = config

    return ctx


def _verify_options(config: configuration.Config) -> None:
    """
    Ensure required options are set.
    """

    if not config.config['species']:
        log._logger.error('You must specify a species (-s/--species)')
        exit(1)


## I don't like the fact that you can't pass options common to all subcommands
## after the subcommand, so this effectively lets you do so. It's a fucking hack
## and does weird shit, but w/e.
def _common_options(f):
    @click.option(
        '-c',
        '--config',
        default=None,
        type=click.Path(readable=True, resolve_path=True),
        help='configuration file'
    )
    @click.option(
        '-f',
        '--force',
        default=None,
        is_flag=True,
        help='force data retrieval and overwrite local copies if they exist'
    )
    @click.option(
        '-s',
        '--species',
        default=None,
        type=click.Choice(['hg38', 'mm10'], case_sensitive=False),
        help='run the pipeline for the given species'
    )
    @click.version_option(version=__version__.__version__, prog_name='egg-v')
    @functools.wraps(f)
    def wrap(*args, **kwargs):
        return f(*args, **kwargs)

    return wrap


@click.group()
@_common_options
@click.pass_context
def cli(ctx, config: str, force: bool, species: str) -> None:

    ctx.ensure_object(dict)
    ctx = _handle_common_options(ctx, config, force, species)

    log._initialize_logging(verbose=True)


@cli.command('retrieve')
@_common_options
@click.pass_context
def _cmd_retrieve(ctx, config: str, force: bool, species: str) -> None:
    """
    \b
    Retrieve gene and variant builds from Ensembl.
    Store the data to disk.
    """

    ctx.ensure_object(dict)
    ## Options are rehandled for each subcommand since they can be specified before
    ## or after subcommands.
    ctx = _handle_common_options(ctx, config, force, species)

    _verify_options(ctx.obj['config'])


@cli.command('process')
@_common_options
@click.pass_context
def _cmd_process(ctx, config: str, force: bool, species: str) -> None:
    """
    \b
    Parse and process gene and variant builds from Ensembl.
    Store the results to disk.
    """

    ctx.ensure_object(dict)
    ctx = _handle_common_options(ctx, config, force, species)

    _verify_options(ctx.obj['config'])

    _run_process_step(ctx.obj['config'])


@cli.command('annotate')
@_common_options
@click.pass_context
def _cmd_annotate(ctx, config: str, force: bool, species: str) -> None:
    """
    \b
    Annotate intragenic variants to their corresponding genes.
    Store annotated variants to disk.
    """

    ctx.ensure_object(dict)
    ctx = _handle_common_options(ctx, config, force, species)

    _verify_options(ctx.obj['config'])

    _run_annotate_step(ctx.obj['config'])


@cli.command('complete')
@_common_options
@click.pass_context
def _cmd_complete(ctx, config: str, force: bool, species: str) -> None:
    """
    Run the complete variant processing pipeline.
    """

    ctx.ensure_object(dict)
    ctx = _handle_common_options(ctx, config, force, species)

    _verify_options(ctx.obj['config'])

    pipeline.run_complete_pipeline(ctx.obj['config'])

if __name__ == '__main__':
    cli()
