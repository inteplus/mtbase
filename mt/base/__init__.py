"""Minh-Tri Pham's base package for Python.

Importing this package gives access to the following names: `logger`, `home_dirpath`,
`temp_dirpath`, :func:`deprecated_func`, :func:`cast` and :func:`castable`, :func:`debug_exec`
and :func:`debug_on`, the datatype checkers of :mod:`mt.base.datatype` (`is_ndarray`, ...),
`ansi_ul`, and the model parameter classes of :mod:`mt.base.model` (`ModelParams`, `NameScope`,
`MHAParams`, `ClassifierParams`, ...). Other utilities live in submodules, for example
:mod:`mt.base.s3`, :mod:`mt.base.bg_invoke`, :mod:`mt.base.object_store` and
:mod:`mt.base.locket`.

Side effect: on import, in the main thread of a posix system, the package registers a SIGUSR1
handler that lets the process be debugged remotely (see :mod:`mt.base.debug_process`).

Examples
--------
>>> from mt.base import is_ndarray
>>> import numpy as np
>>> is_ndarray(np.zeros(2))
True


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

from mt.traceback import LogicError  # for backward compatibility

from .deprecated import deprecated_func
from .casting import cast, castable
from .exec import debug_exec, debug_on

home_dirpath = _log_init._home_dirpath
temp_dirpath = _log_init._temp_dirpath

__api__ = [
    "logger",
    "home_dirpath",
    "temp_dirpath",
    "deprecated_func",
    "cast",
    "castable",
    "debug_exec",
    "debug_on",
    "is_ndarray",
    "is_torchtensor",
    "is_jaxarray",
    "is_tftensor",
    "is_keras_variable",
    "is_h5group",
    "TensorError",
    "ModelSyntaxError",
    "ModelParams",
    "NameScope",
    "MHAParams",
    "MHAPool2DCascadeParams",
    "MobileNetV3MixerParams",
    "ClassifierParams",
]


from .datatype import *
from .const import *
from .model import *


# backdoor to debug the process
from .debug_process import listen as _listen

_listen()
