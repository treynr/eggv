#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: cluster.py
## desc: Functions for dask cluster initialization.

from dask.distributed import Client
from dask.distributed import LocalCluster
from dask_jobqueue import PBSCluster
from functools import partial
from pathlib import Path
from typing import List
import dask
import logging
import os
import tempfile as tf

from . import log

logging.getLogger(__name__).addHandler(logging.NullHandler())


def _initialize_local_cluster(
    processes: int = os.cpu_count(),
    threads: bool = False,
    temp: str = None,
    **kwargs
) -> LocalCluster:
    """
    Initialize a local, single machine cluster.

    arguments
        processes: number of processes (workers) to use.
                   Use all available cores by default.
        threads:   use threads instead of processes
        temp:      location of the local working, or temp, directory

    returns
        a dask LocalCluster
    """

    if not temp:
        temp = tf.gettempdir()

    return LocalCluster(
        n_workers=processes,
        processes=(not threads),
        local_directory=temp
    )


def _initialize_pbs_cluster(
    name: str = 'epigenomics-integration-pipeline',
    queue: str = 'batch',
    interface: str = 'ib0',
    cores: int = 2,
    processes: int = 2,
    memory: str = '220GB',
    walltime: str = '00:30:00',
    env_extra: List[str] = None,
    log_dir: str = 'logs',
    temp: str = None,
    **kwargs
) -> PBSCluster:
    """
    Initialize a dask distributed cluster for submission on an HPC system running
    PBS/TORQUE.

    arguments
        name:      job name
        queue:     queue used for submission
        interface: interconnect interface (e.g. ib0 = Infiniband, eth0 = ethernet)
                   This is system specific and you should find the proper interface
                   first by running 'ip addr'.
        cores:     number of cores per job
        procs:     number of processes per job
        memory:    total memory per job, so memory = 120GB and cores = 2, means each
                   process will have 60GB of usable memory
        walltime:  max. runtime for each job
        env_extra: extra arguments to use with the submission shell script
        temp:      location of the local working, or temp, directory

    returns
        a PBSCluster
    """

    if not env_extra:
        env_extra = ['cd $PBS_O_WORKDIR']

    ## Ensure the log directory exists
    Path(log_dir).mkdir(parents=True, exist_ok=True)

    return PBSCluster(
        name=name,
        queue=queue,
        interface=interface,
        cores=cores,
        processes=processes,
        memory=memory,
        walltime=walltime,
        local_directory=temp,
        ## Helix requires this, kodiak doesn't like this
        resource_spec=f'nodes=1:ppn={cores}',
        job_extra=[
            f'-N {name}',
            f'-l mem={memory}',
            f'-e {log_dir}',
            f'-o {log_dir}'
        ],
        env_extra=env_extra
    )


def initialize_cluster(
    hpc: bool = True,
    jobs: int = 10,
    temp: str = tf.gettempdir(),
    verbose: bool = True,
    **kwargs
) -> Client:
    """
    Initialize a distributed dask cluster.

    arguments
        hpc:     if true, initialize a PBS cluster otherwise just use a single machine
                 local cluster
        verbose: logging verbosity
        jobs:    number of jobs to submit to an HPC cluster

    returns
        a dask client
    """

    ## Idk if this is actually needed
    dask.config.set({'temporary_directory': temp})
    dask.config.set({'local_directory': temp})

    if hpc:
        cluster = _initialize_pbs_cluster(**kwargs)
        cluster.scale(jobs=jobs)

    else:
        cluster = _initialize_local_cluster(**kwargs)

    client = Client(cluster)

    ## Run the logging init function on each worker and register the callback so
    ## future workers also run the function
    init_logging_partial = partial(log._initialize_logging, verbose=verbose)
    client.register_worker_callbacks(setup=init_logging_partial)

    return client


if __name__ == '__main__':
    pass

