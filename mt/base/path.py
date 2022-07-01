'''Useful functions dealing with paths.'''

from os import utime, walk, chmod, listdir
from os.path import *
from glob import glob
from func_timeout import func_timeout

from .aio.path import exists_asyn, remove_asyn, remove, make_dirs, lock, rename_asyn, rename,\
    stat_asyn, stat


__all__ = [
    'exists_asyn', 'exists_timeout', 'remove_asyn', 'remove', 'make_dirs', 'lock', 'rename_asyn',
    'rename', 'utime', 'walk', 'stat_asyn', 'stat', 'chmod', 'listdir', 'glob'
]


def exists_timeout(path: str, timeout: float = 1.0) -> bool:
    '''Checks if a path exists for a number of seconds.

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
    '''
    return func_timeout(timeout, exists, args=(path,))
