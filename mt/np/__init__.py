'''Alias module. Instead of 'import numpy as np' you can do 'from mt import np'.'''

from .ndarray import *
from .sparse import *
from .matrix import *

from numpy import *
from numpy import __version__

import numpy as _np

for key in _np.__dict__:
    globals()[key] = _np.__dict__[key]

