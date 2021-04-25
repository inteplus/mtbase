'''Alias module. Instead of 'import matplotlib.pyplot as plt' you can do 'from mt import plt'.'''

from matplotlib.pyplot import *

import matplotlib.pyplot as _plt

for key in _plt.__dict__:
    globals()[key] = _plt.__dict__[key]

