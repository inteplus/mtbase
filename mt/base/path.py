'''Useful functions dealing with paths.

MT-NOTE: For backward compatibility only. Use module `mt.base.net` instead.'''


import os as _os
import os.path as _op
import shutil as _su
import atexit as _ex
import time as _t
import platform as _pl

from os import rename, utime, walk, stat, chmod
from os.path import *
from glob import glob

from . import logger
from .threading import Lock, ReadWriteLock, ReadRWLock, WriteRWLock


__all__ = ['remove', 'make_dirs', 'lock', 'rename', 'utime', 'walk', 'stat', 'chmod', 'glob']


_path_lock = Lock()

def remove(path):
    '''Removes a path completely, regardless of it being a file or a folder. If the path does not exist, do nothing.'''
    with _path_lock:
        if islink(path):
            _os.unlink(path)
        elif isfile(path):
            _os.remove(path)
        elif isdir(path):
            try:
                _su.rmtree(path)
            except OSError as e:
                if _pl.system() == 'Windows':
                    pass # this can sometimes fail on Windows
                raise e

def make_dirs(path, shared=True):
    '''Convenient invocation of `os.makedirs(path, exist_ok=True)`. If `shared` is True, every newly created folder will have permission 0o775.'''
    if not path: # empty path, just ignore
        return
    with _path_lock:
        if shared:
            stack = []
            while not exists(path):
                head, tail = split(path)
                if not head: # no slash in path
                    stack.append(tail)
                    path = '.'
                elif not tail: # slash at the end of path
                    path = head
                else: # normal case
                    stack.append(tail)
                    path = head
            while stack:
                tail = stack.pop()
                path = join(path, tail)
                _os.mkdir(path, 0o775)
                _os.chmod(path, mode=0o775)
        else:
            _os.makedirs(path, mode=0o775, exist_ok=True)


def lock(path, to_write=False):
    '''Returns the current MROW lock for a given path.

    Parameters
    ----------
        path : str
            local path
        to_write : bool
            whether lock to write or to read

    Returns
    -------
        lock : ReadRWLock or WriteRWLock
            an instance of WriteRWLock if to_write is True, otherwise an instance of ReadRWLock
    '''
    with lock.__lock0:
        # get the current lock, or create one if it needs be
        if not path in lock.__locks:
            # check if we need to cleanup
            lock.__cleanup_cnt += 1
            if lock.__cleanup_cnt >= 1024:
                lock.__cleanup_cnt = 0

                # accumulate those locks that are unlocked
                removed_paths = []
                for x in lock.__locks:
                    if lock.__locks[x].is_free():
                        removed_paths.append(x)

                # remove them
                for x in removed_paths:
                    lock.__locks.pop(x, None)

            # create a new lock
            lock.__locks[path] = ReadWriteLock()

        return WriteRWLock(lock.__locks[path]) if to_write else ReadRWLock(lock.__locks[path])

lock.__lock0 = Lock()
lock.__locks = {}
lock.__cleanup_cnt = 0


# exit function
def __exit_module():
    # repeatedly wait until all locks are free
    for i in range(1024):
        cnt = sum((not v.is_free() for k,v in lock.__locks.items()))
        if not cnt:
            break
        logger.info("waiting for {} path locks to be free...".format(cnt))
        _t.sleep(5)

_ex.register(__exit_module)
