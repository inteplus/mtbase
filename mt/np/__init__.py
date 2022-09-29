"""Alias module. Instead of 'import numpy as np' you can do 'from mt import np'."""


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
