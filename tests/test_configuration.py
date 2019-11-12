#!/usr/bin/env python
# -*- coding: utf8 -*-

## file: test_configuration.py
## desc: Test functions in configuration.py.

from dask.distributed import Client
from dask.distributed import LocalCluster
from pathlib import Path
import pytest
import tempfile as tf

from src import configuration

@pytest.fixture(scope='module')
def root_dir():
    return Path(__file__).resolve().parent.as_posix()

@pytest.fixture(scope='module')
def sample_config_0(root_dir):
    return Path(root_dir, 'data/sample-config-0.yml').as_posix()


def test_default_config_loading():
    config = configuration.Config()

    assert 'resources' in config.config
    assert 'directories' in config.config
    assert config.config['workers'] is None
    assert config.config['scheduler'] is None
    assert config.config['overwrite'] == True
    assert config.config['species'] is None
    assert config.config['resources']['environment']['hpc'] == True
    assert config.config['resources']['environment']['local'] == False
    assert config.config['resources']['environment']['cloud'] == False
    assert config.config['resources']['cores'] == 4
    assert config.config['resources']['processes'] == 4
    assert config.config['resources']['jobs'] == 40
    assert config.config['resources']['memory'] == '80GB'


def test_config_loading_0(sample_config_0):
    config = configuration.Config()
    config.reload_default()
    config = configuration.Config(config=sample_config_0)

    assert config.config['resources']['environment']['hpc'] == True
    assert config.config['resources']['cores'] == 8
    assert config.config['resources']['processes'] == 8
    assert config.config['resources']['jobs'] == 10
    assert config.config['resources']['memory'] == '8GB'


def test_config_reloading(sample_config_0):
    config = configuration.Config()
    config.reload_default()
    config = configuration.Config()

    assert config.config['resources']['cores'] == 4
    assert config.config['resources']['processes'] == 4
    assert config.config['resources']['jobs'] == 40
    assert config.config['resources']['memory'] == '80GB'

    config.reload(sample_config_0)

    assert config.config['resources']['cores'] == 8
    assert config.config['resources']['processes'] == 8
    assert config.config['resources']['jobs'] == 10
    assert config.config['resources']['memory'] == '8GB'

