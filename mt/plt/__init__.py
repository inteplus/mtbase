'''Alias module. Instead of 'import matplotlib.pyplot as plt' you can do 'from mt import plt'.'''

try:
    from matplotlib.pyplot import *

    import matplotlib.pyplot as _plt

    for key in _plt.__dict__:
        globals()[key] = _plt.__dict__[key]
except ImportError:
    raise ImportError("Alias 'mt.plt' to 'matplotlib.pyplot' requires package 'matplotlib' be installed.")
