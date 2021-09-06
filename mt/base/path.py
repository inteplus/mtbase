'''Useful functions dealing with paths.'''


import os as _os
import shutil as _su
import atexit as _ex
import time as _t
import platform as _pl
import aiofiles.os

from os import utime, walk, chmod, listdir
from os.path import *
from glob import glob

from . import logger
from .threading import Lock, ReadWriteLock, ReadRWLock, WriteRWLock
from .asyn import arun


__all__ = ['remove_asyn', 'remove', 'make_dirs', 'lock', 'rename_asyn', 'rename', 'utime', 'walk', 'stat_asyn', 'stat', 'chmod', 'listdir', 'glob']


_path_lock = Lock()


async def remove_asyn(path, asyn: bool = True):
    '''An asyn function that removes a path completely, regardless of it being a file or a folder.

    If the path does not exist, do nothing.

    Parameters
    ----------
    path : str
        a path to a link, a file or a directory
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously
    '''
    with _path_lock:
        if islink(path):
            _os.unlink(path)
        elif isfile(path):
            if asyn:
                await aiofiles.os.remove(path)
            else:
                _os.remove(path)
        elif isdir(path):
            try:
                _su.rmtree(path)
            except OSError as e:
                if _pl.system() == 'Windows':
                    pass # this can sometimes fail on Windows
                raise e


def remove(path, asynch: bool = False):
    '''An asynch function that removes a path completely, regardless of it being a file or a folder.

    If the path does not exist, do nothing.

    Parameters
    ----------
    path : str
        a path to a link, a file or a directory
    asynch : bool
        whether to invoke the function asynchronously (True) or synchronously (False)
    '''
    return arun(remove_asyn, path, asynch=asynch)


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
                try:
                    _os.mkdir(path, 0o775)
                except FileExistsError:
                    pass
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


async def rename_asyn(src, dst, asyn: bool = True):
    '''An asyn function that renames a file or a directory.

    Parameters
    ----------
    src : str
        path to the source file or directory
    dst : path
        new name also as a path
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously
    '''

    if asyn:
        retval = await aiofiles.os.rename(src, dst)
        return retval

    return _os.rename(src, dst)


def rename(src, dst, asynch: bool = False):
    '''An asynch function that renames a file or a directory.

    Parameters
    ----------
    src : str
        path to the source file or directory
    dst : path
        new name also as a path
    asynch : bool
        whether to invoke the function asynchronously (True) or synchronously (False)
    '''

    return arun(rename_asyn, src, dst, asynch=asynch)


async def stat_asyn(path, dir_fd=None, follow_symlinks=True, asyn: bool = True):
    '''An asyn function that performs a stat system call on the given path.

    Parameters
    ----------
    path : str
        Path to be examined; can be string, bytes, a path-like object or open-file-descriptor int.
    dir_fd : object, optional
        If not None, it should be a file descriptor open to a directory, and path should be a
        relative string; path will then be relative to that directory.
    follow_symlinks : bool
        If False, and the last element of the path is a symbolic link, stat will examine the
        symbolic link itself instead of the file the link points to.
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously

    Returns
    -------
    os.stat_result
        the resulting instance

    Notes
    -----
    dir_fd and follow_symlinks may not be implemented on your platform. If they are unavailable,
    using them will raise a NotImplementedError.

    It's an error to use dir_fd or follow_symlinks when specifying path as an open file descriptor.
    '''

    if asyn:
        retval = await aiofiles.os.stat(path, dir_fd=dir_fd, follow_symlinks=follow_symlinks)
        return retval

    return _os.stat(path, dir_fd=dir_fd, follow_symlinks=follow_symlinks)


def stat(path, dir_fd=None, follow_symlinks=True, asynch: bool = False):
    '''An asynch function that performs a stat system call on the given path.

    Parameters
    ----------
    path : str
        Path to be examined; can be string, bytes, a path-like object or open-file-descriptor int.
    dir_fd : object, optional
        If not None, it should be a file descriptor open to a directory, and path should be a
        relative string; path will then be relative to that directory.
    follow_symlinks : bool
        If False, and the last element of the path is a symbolic link, stat will examine the
        symbolic link itself instead of the file the link points to.
    asynch : bool
        whether to invoke the function asynchronously (True) or synchronously (False)

    Returns
    -------
    os.stat_result
        the resulting instance

    Notes
    -----
    dir_fd and follow_symlinks may not be implemented on your platform. If they are unavailable,
    using them will raise a NotImplementedError.

    It's an error to use dir_fd or follow_symlinks when specifying path as an open file descriptor.
    '''

    return arun(stat_asyn, path, dir_fd=dir_fd, follow_symlinks=follow_symlinks, asynch=asynch)


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
