#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: hg38_pipeline.py
## desc: Run the variant integration ETL pipeline for human (hg38 build) datasets.

from dask.distributed import Client
from dask.distributed import LocalCluster
from dask_jobqueue import PBSCluster
from functools import partial
from pathlib import Path
import os
import sys

## sys path hack so we can import the scripts in the src/ dir.
## Assumes this script is being called from the parent directory
## i.e., python pipeline/human_pipeline.py
sys.path.append(os.path.abspath('./'))
sys.path.append(os.path.abspath('src'))

from src import log
from src import annotate
from src import process
from src import retrieve


if __name__ == '__main__':
    from argparse import ArgumentParser

    usage = 'usage: %s [options]'
    parser = ArgumentParser(usage=usage)

    parser.add_argument(
        '-l',
        '--local',
        action='store_true',
        dest='local',
        help='run the pipeline using a local cluster (HPC system, PBS/Torque is default)'
    )
    parser.add_argument(
        '--verbose',
        action='store_true',
        dest='verbose',
        help='clutter your screen with output'
    )

    args = parser.parse_args()

    init_logging_partial = partial(log._initialize_logging, verbose=args.verbose)

    init_logging_partial()

    Path('logs').mkdir(exist_ok=True)

    if args.local:

        cluster = LocalCluster(
            n_workers=12,
            processes=True
        )

    else:
        cluster = PBSCluster(
            name='variant-etl',
            queue='batch',
            interface='ib0',
            cores=2,
            processes=2,
            memory='60GB',
            walltime='02:00:00',
            job_extra=['-e logs', '-o logs'],
            env_extra=['cd $PBS_O_WORKDIR']
        )

        ## Dynamically adapt to computational requirements, a minimum of four jobs should
        ## always be running and an additional six jobs can be added to meet increased
        ## workloads
        cluster.adapt(minimum=10, maximum=80)

    log._logger.info('Starting cluster')

    client = Client(cluster)

    ## Newly added workers should initialize logging
    client.register_worker_callbacks(setup=init_logging_partial)

    log._logger.info('Retrieving genome features')

    raw_variants = retrieve.run_hg38_variant_retrieval(client, force=False)
    raw_genes = retrieve.run_hg38_gene_retrieval(client, force=False)

    ## Convert the list of variant futures into a dict so they can be used as kwargs, dask
    ## will automatically wait for them to finish before executing the processing step of
    ## the pipeline
    raw_variants = dict([(f'dep{t[0]}', t[1]) for t in enumerate(raw_variants)])

    log._logger.info('Processing genome features')

    processed_variants = process.run_hg38_variant_processing(client, **raw_variants)
    processed_genes = process.run_hg38_gene_processing(client, depends=raw_genes)

    ## We can gather since the results are small (they're filepaths)
    client.gather([
        processed_variants,
        processed_genes
    ])

    log._logger.info('Done')

    client.close()

