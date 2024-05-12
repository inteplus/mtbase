"""Additional utitlities dealing with numpy.

Instead of:

.. code-block:: python

   import numpy as np

You do:

.. code-block:: python

   from mt import np

It will import the numpy package plus the additional stuff implemented here.

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
]
