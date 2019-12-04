#!/usr/bin/env python
# -*- encoding: utf-8 -*-

## file: configuration.py
## desc: Config file parsing and handling.
## auth: TR

from typing import Any
import yaml

_default_configuration = '''
## Specify computational resources
resources:
  environment:
    hpc: true
    local: false
    cloud: false

  cores: 4
  processes: 4
  jobs: 40
  memory: '80GB'
  walltime: '05:00:00'
  interface: 'ib0'

directories:
  ## Path to a data directory which should be accessible by all worker nodes
  data: 'data/'

  ## Path to temp directories to be used by worker nodes, if null then workers
  ## will generate their own temp directories.
  temp: ~

## Specify the address to a Dask scheduler node if necessary
scheduler: ~

## List of worker node addresses if necessary
workers: ~

## If true, always retrieve copies of the data and overwrite any that already exist
overwrite: true

## Run the given species specific pipeline. If not provided here, it must be
## given as an argument on the command line
species: ~
'''

class Config(object):
    """
    Singleton Config class for representing the pipeline configuration.
    """

    _instance = None
    ## So mypy doesn't bitch about dumb things
    config: Any = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)

        return cls._instance

    def __init__(self, config=None):

        if self.config is None:
            ## This just ensures all config variables are specified
            ## (i.e., a user's config file can be incomplete b/c this will load defaults)
            self.config = yaml.load(_default_configuration, Loader=yaml.FullLoader)

        if config:
            self.reload(config)

    def reload(self, config):
        """
        Reload and merge the configuration from the given file.

        arguments
            config: config filepath
        """

        self.config = {
            **self.config,
            **yaml.load(open(config, 'r'), Loader=yaml.FullLoader)
        }

    def reload_default(self):
        """
        Reload the default configuration settings.
        """

        self.config = yaml.load(_default_configuration, Loader=yaml.FullLoader)
