import numpy as np
import pyximport
from pyximport import install

try:
    from pyximport import _pyximport3 as _pi
except ImportError:
    from pyximport import pyximport as _pi

old_get_distutils_extension = _pi.get_distutils_extension


def new_get_distutils_extension(modname, pyxfilename, language_level=None):
    extension_mod, setup_args = old_get_distutils_extension(
        modname, pyxfilename, language_level
    )
    extension_mod.language = "c++"
    extension_mod.include_dirs = [np.get_include()]
    return extension_mod, setup_args


_pi.get_distutils_extension = new_get_distutils_extension
