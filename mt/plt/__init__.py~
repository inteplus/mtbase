'''Alias module. Instead of 'import matplotlib as mpl' you can do 'from mt import mpl'.'''

from matplotlib import *
from matplotlib import __version__

import matplotlib as _mpl

for key in _mpl.__dict__:
    globals()[key] = _mpl.__dict__[key]

