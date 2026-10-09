"""Alias for Python package `typing`_.

Instead of:

.. code-block:: python

   import typing as tp

Minh-Tri often does:

.. code-block:: python

   from mt import tp

This alias implements that idea. The code above will import the typing package, so that
``tp.Optional``, ``tp.List``, etc. are available. On top of that, it also exposes numpy's
``NDArray``, ``ArrayLike`` and ``DTypeLike`` from :mod:`numpy.typing`.

Examples
--------
>>> from mt import tp
>>> def f(x: tp.Optional[int] = None) -> tp.List[int]:
...     return [] if x is None else [x]
>>> f(3)
[3]

Please see Python package `typing`_ for more details.

.. _typing:
   https://docs.python.org/3/library/typing.html
"""

from __future__ import annotations
from typing import *
from numpy.typing import NDArray, ArrayLike, DTypeLike

import typing as _tp

for key in _tp.__dict__:
    if key == "__doc__":
        continue
    globals()[key] = _tp.__dict__[key]

# MT-NOTE: To debug, create a file called 'debug.txt' in the home dirpath and write
# 'winnow wins now' in it.
