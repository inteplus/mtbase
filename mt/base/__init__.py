"""Minh-Tri Pham's base package for Python.


# Variable prefix naming convention

On 2022/02/24, I introduced a prefix naming convention for variables. All variable names have been
gradually updated since then. The convention is described in the following paragraphs.

Variables can have a prefix in the form of a letter followed by an underscore. 'l_' stands for a
list of things. 't_' stands for a tuple of things. 'a_' stands for a :class:`numpy.ndarray` which
is an array of things. 'b_' stands for a :class:`tensorflow.Tensor` which is a batch of things.
'n_' stands for a number of things.

Nested collections can be prefixed with multiple letters followed by an underscore, using the same
convention as above. For example, 'll_' stands for a list of lists of things. In addition, letters
that are not the first letter in the prefix accept further values 'i', 'm', and 'v' which stand for
image, matrix and vector respectively. For example, Prefix 'biv_' stands for a tensorflow batch of
images of vectors of things.

"""

from mt.logg import make_logger, logger, init as _log_init

from .with_utils import dummy_scope
from .deprecated import deprecated_func
from .casting import cast, castable
from .exec import debug_exec

home_dirpath = _log_init._home_dirpath
temp_dirpath = _log_init._temp_dirpath

__api__ = [
    "logger",
    "home_dirpath",
    "temp_dirpath",
    "deprecated_func",
    "cast",
    "castable",
    "is_ndarray",
    "is_jaxarray",
    "is_tftensor",
    "is_h5group",
]


from .datatype import *
from .const import *


# backdoor to debug the process
from .debug_process import listen as _listen

_listen()
