"""Additional functions dealing with paths.

Instead of:

.. code-block:: python

   from os import path

You do:

.. code-block:: python

   from mt import path

It will import the path package plus the additional stuff implemented here.

Please see Python package `os.path`_ for more details.

.. _os.path:
   https://docs.python.org/3/library/os.path.html
"""

from os import utime, walk, chmod, listdir
from os.path import *
from glob import glob
from func_timeout import func_timeout

from mt.aio.path import (
    exists_asyn,
    remove_asyn,
    remove,
    make_dirs,
    make_dirs_asyn,
    lock,
    rename_asyn,
    rename,
    stat_asyn,
    stat,
)


__api__ = [
    "exists_asyn",
    "exists_timeout",
    "remove_asyn",
    "remove",
    "make_dirs",
    "make_dirs_asyn",
    "lock",
    "rename_asyn",
    "rename",
    "utime",
    "walk",
    "stat_asyn",
    "stat",
    "chmod",
    "listdir",
    "glob",
]


def exists_timeout(path: str, timeout: float = 1.0) -> bool:
    """Checks if a path exists for a number of seconds.

    Call this function rarely as the wrapping for the function to be run in a background thread is
    expensive.

    Parameters
    ----------
    path : str
        a path to a link, a file or a directory
    timeout : float
        number of seconds to wait before timeout.

    Returns
    -------
    bool
        whether or not the path exists

    Raises
    ------
    func_timeout.FunctionTimeout
        if the function times out
    """
    return func_timeout(timeout, exists, args=(path,))
