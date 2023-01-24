"""Alias module of matplotlib's `pyplot`_.

Instead of:

.. code-block:: python

   import matplotlib.pyplot as plt

You do:

.. code-block:: python

   from mt import plt

It will import the matplotlib.pyplot package.

Please see Python package `pyplot`_ for more details.

.. _pyplot:
   https://matplotlib.org/stable/api/pyplot_summary.html
"""

try:
    from matplotlib.pyplot import *

    import matplotlib.pyplot as _plt

    for key in _plt.__dict__:
        if key == "__doc__":
            continue
        globals()[key] = _plt.__dict__[key]
except ImportError:
    raise ImportError(
        "Alias 'mt.plt' to 'matplotlib.pyplot' requires package 'matplotlib' be installed."
    )
