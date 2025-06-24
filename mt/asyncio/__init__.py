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

from packaging.version import Version
import platform
from asyncio import *
import asyncio as _asyncio

for key in _asyncio.__dict__:
    if key == "__doc__":
        continue
    globals()[key] = _asyncio.__dict__[key]

if Version(platform.python_version()) < Version("3.11"):
    try:
        from taskgroup import run, TaskGroup, timeout
    except ImportError:
        import warnings

        warnings.warn
        (
            f"For python {platform.python_version()}, mt.asyncio relies on asyncio.timeout() of Python 3.11+ for efficiency reasons. Please pip install package 'taskgroup' if possible. Otherwise, please ignore this warning message as lack of asyncio.timeout() only affects efficiency."
        )
