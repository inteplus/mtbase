"""Alias module of asyncio.

Instead of:

.. code-block:: python

   import asyncio

You do:

.. code-block:: python

   from mt import asyncio

It re-exports everything in the standard :mod:`asyncio` package, so ``asyncio.run``,
``asyncio.sleep``, etc. work as usual.

As of 2025-06-04, apart from importing the asyncio package, on Python versions older than 3.11 it
tries to override ``run``, ``TaskGroup`` and ``timeout`` with the versions of the backport package
`taskgroup` so that these components exist even for Python 3.10. If `taskgroup` is not installed,
the standard names are kept as they are and a warning is emitted.

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

        warnings.warn(
            f"For python {platform.python_version()}, mt.asyncio relies on asyncio.timeout() of "
            "Python 3.11+ for efficiency reasons. Please pip install package 'taskgroup' if "
            "possible. Otherwise, please ignore this warning message as lack of asyncio.timeout() "
            "only affects efficiency."
        )
