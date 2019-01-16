
GRIPv
=====

GeneWeaver Resource Integration Pipeline for variants (GRIPv).
A proof of concept ETL pipeline for integrating variant metadata and annotations into
GeneWeaver__.

.. __: https://geneweaver.org


Usage
-----

Just a word of warning, the **hg38** pipeline will consume roughly ~400GB of disk 
space during processing.
To retrieve, process, and load variant metadata and intragenic/downstream/upstream
annotations for *H. sapiens* (**hg38**):

.. code:: bash

    $ ./run-hg38-pipeline.sh

An individual GVF file can be split up and processed in parallel using the
:code:`process-variants.sh` script:

.. code:: bash

    $ ./process-variants.sh lots-of-variants.gvf formatted-output.tsv


Configuration
-------------

The :code:`config.sh` script contains configuration variables such as directories and
resource URLs.
Loading data into GeneWeaver requires a :code:`secrets.sh` file which should be in the
following format:

.. code:: bash

    #!/usr/bin/env bash

    ## file: secrets.sh
    ## desc: Super secret infos.

    db_host='your-host'
    db_name='the-db-name'
    db_user='the-db-user'
    db_pass='the-db-password'
    db_port=5432


Installation
------------

The installation script will check to make sure the dependencies are met:

.. code:: bash

    $ ./install.sh

If dependencies are missing, you will be asked to install them or (in the case of the
miller requirement) the script will install them for you.


Requirements
''''''''''''

- bash >= 4.0
- PostgreSQL
- psql client
- miller__

.. __: https://github.com/johnkerl/miller

