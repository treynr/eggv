#!/usr/bin/env python
# -*- coding: utf-8 -*-

from pathlib import Path
from setuptools import setup


_name = 'eggv'
_description = 'ETL pipeline for processing genetic variants'
_required = [
    'Click>=7.0.0',
    'dask>=2.6.0',
    'dask-jobqueue>=0.7.0',
    'distributed>=2.6.0',
    'fsspec>=0.4.1',
    'numpy>=1.17.3',
    'pandas>=0.25.3',
    'partd>=0.3.10',
    'pyarrow>=0.15.1',
    'PyYAML>=5.1',
    'requests>=2.22.0',
    'urllib3>=1.25.3'
]
_about = {}
_here = Path(__file__).parent.resolve()

with open(Path(_here, _name, '__version__.py')) as fl:
    exec(fl.read(), _about)

setup(
    name=_name,
    version=_about['__version__'],
    description=_description,
    author='TR',
    python_requires='>=3.7.0',
    install_requires=_required,
    license='GPL3',
    packages=['eggv'],
    entry_points={
        'console_scripts': ['eggv = eggv.__main__']
    }
)
