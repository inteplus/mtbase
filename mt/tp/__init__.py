"""Alias for Python package `typing`_.

Instead of:

.. code-block:: python

   import typing as tp

Minh-Tri often does:

.. code-block:: python

   from mt import tp

This alias implements that idea. The code above will import the typing package.

Please see Python package `typing`_ for more details.

.. _typing:
   https://docs.python.org/3/library/typing.html
"""

from typing import *

import typing as _tp

for key in _tp.__dict__:
    if key == "__doc__":
        continue
    globals()[key] = _tp.__dict__[key]
