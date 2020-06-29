import numpy as np
import pyximport
from pyximport import install

old_get_distutils_extension = pyximport.pyximport.get_distutils_extension

def new_get_distutils_extension(modname, pyxfilename, language_level=None):
    extension_mod, setup_args = old_get_distutils_extension(modname, pyxfilename, language_level)
    extension_mod.language='c++'
    extension_mod.include_dirs = [np.get_include()]
    return extension_mod,setup_args

pyximport.pyximport.get_distutils_extension = new_get_distutils_extension
