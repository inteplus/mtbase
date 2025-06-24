"""Alias module of asyncio.

Instead of:

.. code-block:: python

   import asyncio

You do:

.. code-block:: python

   from mt import mpl

It will import the asyncio package.

As of 2025-06-04, apart from importing the asyncio package, it ensures that TaskGroup, Runner and
timeout components of asyncio exist, even for Python version 3.10, by importing the backport
package called `taskgroup`.

Please see Python package `asyncio`_ for more details.

.. _asyncio:
   https://docs.python.org/3/library/asyncio.html
"""

import sys
from asyncio import *
from asyncio import __version__

import asyncio as _asyncio

for key in _asyncio.__dict__:
    if key == "__doc__":
        continue
    globals()[key] = _asyncio.__dict__[key]

if sys.python_version < (3, 11):
try:
    from taskgroup import run, TaskGroup, timeout
except ImportError:
    raise ImportError(
        f"For python version {sys.python_version}, mt.asyncio relies on asyncio.timeout of Python 3.12, please install package 'taskgroup'."
    )
