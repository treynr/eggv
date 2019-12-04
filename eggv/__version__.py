#!/usr/bin/env/python
# -*- coding: utf8 -*-

import subprocess

_version = (1, 2, 0)


def _get_build() -> str:
    """
    Returns the current git branch and commit hash.

    returns
        a string with the git branch and short (seven character) commit hash.
    """

    try:
        info = str(subprocess.check_output(
            ['git', 'rev-parse', '--short', 'HEAD'],
            stderr=subprocess.DEVNULL
        )).strip()
    except Exception:
        return ''

    return info


__version__ = f"{'.'.join(map(str, _version))}{_get_build()}".strip()

