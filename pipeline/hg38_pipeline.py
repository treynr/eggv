#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: hg38_pipeline.py
## desc: Run the variant integration ETL pipeline for human (hg38 build) datasets.

from dask.distributed import Client
from dask.distributed import LocalCluster
from dask.distributed import fire_and_forget
from dask.distributed import as_completed
from dask.distributed import get_client
from dask.distributed import get_worker
from dask.distributed import secede
from dask_jobqueue import PBSCluster
from functools import partial
from pathlib import Path
import os
import sys

## sys path hack so we can import the scripts in the src/ dir.
## Assumes this script is being called from the parent directory
## i.e., python pipeline/human_pipeline.py
#sys.path.append(Path('./').resolve().as_posix())
#sys.path.append(Path('src').resolve().as_posix())

from ..src import dfio
from ..src import globe
from ..src import log
from ..src import annotate
from ..src import process
from ..src import retrieve

def run_hg38_variant_pipeline(client: Client):
    """

    :param client:
    :return:
    """

    ## Will hold a list of Futures representing IO operations (saving processed datasets)
    file_futures = []

    ## Retrieve the gene build from Ensembl
    raw_genes = retrieve.run_hg38_gene_retrieval(client, force=False)

    ## Process the raw gene dataset after it's finished downloading, returns a dask DF
    ## encapsulated by a Future
    gene_df_future = client.submit(
        process.run_hg38_gene_processing_pipeline, input=raw_genes
    )

    ## Scatter and let a worker save the file
    #file_futures.append(client.submit(
    #    dfio.save_dataset,
    #    client.scatter(genes_df, broadcast=True),
    #    output=globe._fp_hg38_gene_meta
    #))

    ## Retrieve genomic variant builds from Ensembl
    raw_variants = retrieve.run_hg38_variant_retrieval(client, force=False)
    annotations = []
    stats = []

    ## We can't do anything until the variants are finished downloading/decompressing,
    ## so we process them as they complete
    for _, variant_path in as_completed(raw_variants, with_results=True):

        ## Run the processing step for this chromosome, don't have to submit to workers
        ## b/c this should return lazy DFs
        processed = process.run_variant_processing_pipeline(variant_path)

        ## Variant metadata and effects
        meta_df = processed['metadata']
        effect_df = processed['effects']

        ## Save these intermediate datasets in the background while we continue
        file_futures.append(client.submit(
            dfio.save_dataset,
            client.scatter(meta_df, broadcast=True),
            name=variant_path,
            outdir=globe._dir_hg38_variant_meta
        ))
        file_futures.append(client.submit(
            dfio.save_dataset,
            client.scatter(effect_df, broadcast=True),
            name=variant_path,
            outdir=globe._dir_hg38_variant_effect
        ))

        ## Now annotate the variants using the effects and gene datasets that were
        ## just processed. This will block until the genes have finished processing
        ## since we can't do anything without them
        annotations = annotate.run_annotation_pipeline(effect_df, gene_df_future.result())

        ## separate (inter|intra)genic annotations
        intergenic = annotations['intergenic']
        intragenic = annotations['intragenic']

        ## Save these intermediate datasets in the background while we continue
        file_futures.append(client.submit(
            dfio.save_dataset,
            client.scatter(meta_df, broadcast=True),
            name=variant_path,
            outdir=globe._dir_hg38_variant_meta
        ))
        file_futures.append(client.submit(
            dfio.save_dataset,
            client.scatter(effect_df, broadcast=True),
            name=variant_path,
            outdir=globe._dir_hg38_variant_effect
        ))

        ## Should return immediately, Future of a dict of Futures
        #annotated = annotated.result()

        ## Isolate the annotation and mapping summary stats
        stats.append(annotated['stats'])
        del annotated['stats']

        annotations.append(annotated)

    ## Combine the mapping stats and save to a file
    stats = client.submit(annotate.combine_stats, stats)
    stats_fp = client.submit(
        annotate.write_annotation_stats, stats, globe._fp_hg38_annotation_stats
    )

    ## Compute these tasks but we don't care about gathering the results
    fire_and_forget(stats_fp)

    return annotations

    ## ...and process each variant file as the retrieval step is completed.
    #for var_future in as_completed(raw_variants):
    #    pv = client.submit(process.run_hg38_single_variant_processing
    #    processed_variants.append(
    #        process.run_hg38_single_variant_processing(client, var_future)
    #    )


    ## Convert the list of variant futures into a dict so they can be used as kwargs, dask
    ## will automatically wait for them to finish before executing the processing step of
    ## the pipeline
    #raw_variants = dict([(f'dep{t[0]}', t[1]) for t in enumerate(raw_variants)])
    #print(raw_variants)
    #exit()

    #log._logger.info('Processing genome features')

    #processed_variants = process.run_hg38_variant_processing(client, **raw_variants)

if __name__ == '__main__':
    from argparse import ArgumentParser

    usage = 'usage: %s [options]'
    parser = ArgumentParser(usage=usage)

    parser.add_argument(
        '-f',
        '--force',
        action='store_true',
        dest='force',
        help='force raw dataset retrieval and overwrite local copies if they exist'
    )
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

    if args.local:

        cluster = LocalCluster(
            n_workers=8,
            processes=True
        )

    else:
        cluster = PBSCluster(
            name='variant-etl',
            queue='batch',
            interface='ib0',
            cores=1,
            processes=1,
            memory='60GB',
            walltime='03:00:00',
            ## If your HPC cluster doesn't have local disks (e.g. only has network
            ## storage) or the local disks are really small, uncomment this or set it
            ## to another directory
            local_directory='/tmp',
            #resource_spec='nodes=1:ppn=1',
            ## Cadillac doesn't like when mem is in the resource_spec string
            #job_extra=['-l mem=60GB', '-e logs', '-o logs'],
            job_extra=['-e variation-logs', '-o variation-logs'],
            env_extra=['cd $PBS_O_WORKDIR']
        )

        Path('variation-logs').mkdir(exist_ok=True)

        ## Dynamically adapt to computational requirements, min/max # of jobs
        cluster.adapt(minimum=2, maximum=45)

        #print(cluster.job_script())
        #exit()

    log._logger.info('Initializing cluster')

    client = Client(cluster)

    ## Newly added workers should initialize logging
    client.register_worker_callbacks(setup=init_logging_partial)

    #log._logger.info('Retrieving genome features')

    """
    ## Retrieve + process genes
    raw_genes = retrieve.run_hg38_gene_retrieval(client, force=False)
    #processed_genes = process.run_hg38_gene_processing(client, depends=raw_genes)
    processed_genes = client.submit(process.run_hg38_gene_processing, depends=raw_genes)
    ## Should return immediately b/c it's a Future of a Future
    processed_genes = processed_genes.result()

    ## Retrieve variants...
    raw_variants = retrieve.run_hg38_variant_retrieval(client, force=False)
    processed_variants = []

    for future in raw_variants:
        pv = client.submit(process.run_hg38_single_variant_processing, future)

        processed_variants.append(pv.result())
    """


    ## ...and process each variant file as the retrieval step is completed.
    #for var_future in as_completed(raw_variants):
    #    pv = client.submit(process.run_hg38_single_variant_processing
    #    processed_variants.append(
    #        process.run_hg38_single_variant_processing(client, var_future)
    #    )
            

    ## Convert the list of variant futures into a dict so they can be used as kwargs, dask
    ## will automatically wait for them to finish before executing the processing step of
    ## the pipeline
    #raw_variants = dict([(f'dep{t[0]}', t[1]) for t in enumerate(raw_variants)])
    #print(raw_variants)
    #exit()

    #log._logger.info('Processing genome features')

    #processed_variants = process.run_hg38_variant_processing(client, **raw_variants)

    futures = run_hg38_variant_pipeline(client)

    client.gather(futures)

    ## We can gather since the results are small (they're filepaths)
    #client.gather([
    #    processed_variants,
    #    processed_genes
    #])

    log._logger.info('Done')

    client.close()

