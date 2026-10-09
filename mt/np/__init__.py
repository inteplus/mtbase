"""Additional utitlities dealing with numpy.

Instead of:

.. code-block:: python

   import numpy as np

You do:

.. code-block:: python

   from mt import np

It will import the numpy package plus the additional stuff implemented here:

- array helpers: :func:`ndarray_repr`, :func:`sigmoid`, :func:`asigmoid`, :func:`frombytes`,
  :func:`divide_no_nan`, :func:`to_b85`, :func:`from_b85`;
- sparse arrays: :class:`SparseNdarray`, :func:`sparse_vector`;
- matrix functions: :func:`psd_approx`, :func:`sqrtm`, :func:`powm`;
- image helpers: :func:`quantise_images`, :func:`dequantise_images`;
- :func:`get_class_color`.

Examples
--------
>>> from mt import np
>>> np.sigmoid(0.0)
0.5
>>> np.divide_no_nan(1.0, 0.0)
0.0

Please see the `numpy`_ package for more details.

.. _numpy:
   https://numpy.org/doc/stable/
"""

from numpy import *
from numpy import __version__

import numpy as _np

from .ndarray import *
from .sparse import *
from .matrix import *
from .image import *
from .class_color import get_class_color

ndarray = _np.ndarray
import numpy.core as core
from numpy.core import round
import numpy.lib as lib
import numpy.polynomial as polynomial
import numpy.linalg as linag
import numpy.fft as fft
import numpy.matlib as matlib
import numpy.random as random


__api__ = [
    "ndarray_repr",
    "sigmoid",
    "asigmoid",
    "frombytes",
    "divide_no_nan",
    "to_b85",
    "from_b85",
    "SparseNdarray",
    "sparse_vector",
    "psd_approx",
    "sqrtm",
    "powm",
    "quantise_images",
    "dequantise_images",
    "get_class_color",
]


def import_test():
    import platform
    from packaging import version as V
    import warnings

    python_version = platform.python_version()

    if V.parse(python_version) < V.parse("3.12"):
        if V.parse(__version__) >= V.parse("2"):
            # warnings.warn("numpy>=2 is not supported for Python <3.12", UserWarning)
            pass
    else:
        if V.parse(__version__) < V.parse("2.3.3"):
            # warnings.warn("numpy>=2.3.3 is required for Python 3.12+", UserWarning)
            pass


import_test()
del import_test
