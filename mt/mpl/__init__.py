"""Alias module of matplotlib.

Instead of:

.. code-block:: python

   import matplotlib as mpl

You do:

.. code-block:: python

   from mt import mpl

It will import the matplotlib package.

Please see Python package `matplotlib`_ for more details.

.. _matplotlib:
   https://matplotlib.org/stable/api/index
"""

try:
    from matplotlib import *
    from matplotlib import __version__

    import matplotlib as _mpl

    for key in _mpl.__dict__:
        if "__doc__" in key:
            continue
        globals()[key] = _mpl.__dict__[key]
except ImportError:
    raise ImportError(
        "Alias 'mt.mpl' to 'maplotlib' requires package 'matplotlib' be installed."
    )
