"""Useful functions dealing with paths.

This module re-exports everything in :mod:`mt.path` (for example `make_dirs`, `remove` and
`chmod`), so `mt.base.path.remove` and `mt.path.remove` are the same function. Import from
:mod:`mt.path` in new code.

2023/01/14
==========

Soon this module will be replaced by :mod:`mt.path`.
"""

from mt.path import *
