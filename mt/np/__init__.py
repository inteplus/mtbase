"""Alias module. Instead of 'import numpy as np' you can do 'from mt import np'."""


from numpy import *
from numpy import __version__

import numpy as _np

from .ndarray import *
from .sparse import *
from .matrix import *
from .image import *

for key in _np.__dict__:
    if not key in globals():
        globals()[key] = _np.__dict__[key]
