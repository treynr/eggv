#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: cli.py
## desc: CLI interface for EGG:V.
## auth: TR

import click
import functools
import logging

from . import __version__
from . import configuration
from . import log
from . import pipeline

logging.getLogger(__name__).addHandler(logging.NullHandler())


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

    pipeline.run_retrieve_step(ctx.obj['config'])


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

    pipeline.run_process_step(ctx.obj['config'])


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

    pipeline.run_annotate_step(ctx.obj['config'])


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
