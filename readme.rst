
EGG:V
=====

Epigenomics and genetics: variation (EGG:V) integration pipeline.
This is one of five ETL sub-pipelines designed to integrate and model
large-scale, heterogeneous epigenomics datasets in GeneWeaver__ (GW).
EGG:V processes and integrates variant metadata and annotations for use in GW.
Currently *H. sapiens* and *M. musculus* genome builds, **hg38** and **mm10**
respectively, are supported.

.. __: https://geneweaver.org


Usage
-----

.. code:: bash

    Usage: eggv [OPTIONS] COMMAND [ARGS]...

    Options:
      -c, --config PATH          configuration file
      -f, --force                force data retrieval and overwrite local copies
                                 if they exist
      -s, --species [hg38|mm10]  run the pipeline for the given species
      --version                  Show the version and exit.
      --help                     Show this message and exit.

    Commands:
      annotate  Annotate intragenic variants to their corresponding genes.
      complete  Run the complete variant processing pipeline.
      process   Parse and process gene and variant builds from Ensembl.
      retrieve  Retrieve gene and variant builds from Ensembl.

The complete variation ETL pipeline can be run by specifying the genome
build and the ``complete`` subcommand:

.. code:: bash

    $ eggv complete -s hg38

The complete pipeline is composed of a number of steps, each of which can be run
individually.
If steps are run independently, they should be run in the order
``retrieve → process → annotate``.


retrieve
''''''''

The ``retrieve`` step of the pipeline retrieves genomic variant and gene builds
from Ensembl__ and the `Ensembl variation`__ database.
Variants downloaded from Ensembl are stored in the
`Genome Variation Format (GVF)`__.
Run this step using the ``retrieve`` subcommand:

.. code:: bash

    $ eggv retrieve -s hg38

.. __: https://ensembl.org/index.html
.. __: https://ensembl.org/info/genome/variation/index.html
.. __: https://github.com/The-Sequence-Ontology/Specifications/blob/master/gvf.md


process
'''''''

The ``process`` step parses and formats genomic variants for later use.
Variant effects are isolated and stored separately from other metadata
(e.g. dbSNP ID, chromosome, alleles).
Run this step using the ``process`` subcommand:

.. code:: bash

    $ eggv process -s hg38


annotate
''''''''

The final ``annotate`` step identifies variants as either intergenic or intragenic.
Intragenic variants are mapped to their respective genes using previously retrieved
Ensembl gene builds.
Run this step using the ``annotate`` subcommand:

.. code:: bash

    $ eggv annotate -s hg38


Configuration
-------------

The pipeline can be configured using a YAML based configuration file.
The complete file, pipeline defaults, and option explanations are listed below.

.. code:: yaml

    resources:
      environment:
        hpc: true
        local: false
        cloud: false

      cores: 4
      processes: 4
      jobs: 15
      memory: '40GB'
      walltime: '05:00:00'
      interface: 'ib0'

    directories:
      data: 'data/'
      temp: ~

    scheduler: ~
    workers: ~
    overwrite: true
    species: ~


Options
'''''''

resources.environment.hpc
    boolean. If true, the pipeline will initialize a cluster on an HPC system running
    PBS/Torque.

resources.environment.local
    boolean. If true, the pipeline will initialize a local, single machine cluster.

resources.environment.cloud
    boolean. (Not yet implemented) If true, the pipeline will initialize a cluster
    for cloud environments.

resources.cores
    integer. The number of CPU cores available to each cluster worker process. This
    option only has an effect if running an HPC cluster.

resources.processes
    integer. The number of worker processes to use. If running an HPC cluster,
    the number of cores will be divided by the number of worker processes.
    So, if cores = 4 and processes = 2, two worker processes will spawn utilizing
    2 cores (threads) each. If cores = 4 and processes = 4, four worker processes will
    spawn utilizing 1 core each.

resources.processes
    integer. The number of worker nodes to use.
    This option only has an effect if running an HPC cluster.

resources.memory
    string. Worker process memory limits. If using a memory limit of 40GB with 4 worker
    processes, each worker has a limit of 10GB.
    This option only has an effect if running an HPC cluster.

resources.walltime
    string. Worker node time limits.
    This option only has an effect if running an HPC cluster.

resources.interface
    string. Network interface to use for worker-worker and worker-scheduler
    communication.
    'ib0' is Infiniband, 'eth0' is ethernet, etc.
    Use ``ip addr`` to identify the proper interface to use.
    This option only has an effect if running an HPC cluster.

directories.data
    string. The base directory path to store raw and processed datasets.

directories.temp
    string. The temp directory. If left blank the pipeline will automatically use
    system defaults.

scheduler
    string. The scheduler node address.

workers
    list. A list of worker node addresses.

overwrite
    boolean. Force data retrieval and overwrite local copies even if they already exist.

species
    string. The genome build to run the pipeline on.


Installation
------------

Idk yet.


Requirements
------------

The EGG:V pipeline has some hefty storage and memory requirements.


Storage
'''''''

To be safe, at least **500GB** of disk space should be available if both **hg38** and
**mm10** builds will be processed.
The sizes below are for Ensembl v95.

.. code:: bash

    249G    ./hg38/raw
    106G    ./hg38/effects
    27G     ./hg38/meta
    5.8G    ./hg38/annotated/intergenic
    49G     ./hg38/annotated/intragenic
    54G     ./hg38/annotated
    436G    ./hg38
    23G     ./mm10/raw
    21G     ./mm10/effects
    6.6G    ./mm10/meta
    2.0G    ./mm10/annotated/intergenic
    4.5G    ./mm10/annotated/intragenic
    6.5G    ./mm10/annotated
    56G     ./mm10
    492G    ./


Memory
''''''

The lowest amount of total available memory this pipeline has been tested with
is **450GB**.
Since processing is done in-memory, all at once, systems with total memory
below **400GB** might not be able to run the complete pipeline.


CPU
'''

Use as many CPU cores as you possibly can.
Seriously.


Software
''''''''

See ``requirements.txt`` for a complete list of required Python packages.
If running tests, ``requirements.dev.txt`` has several additional packages that are
needed.
The major requirements are:

- Python >= 3.7
- dask__
- pandas__
- numpy__

.. __: https://dask.org/
.. __: https://pandas.pydata.org/
.. __: https://numpy.org/

