'''Alias module. Instead of 'import matplotlib as mpl' you can do 'from mt import mpl'.'''

try:
    from matplotlib import *
    from matplotlib import __version__

    import matplotlib as _mpl

    for key in _mpl.__dict__:
        globals()[key] = _mpl.__dict__[key]
except ImportError:
    raise ImportError("Alias 'mt.mpl' to 'maplotlib' requires package 'matplotlib' be installed.")
