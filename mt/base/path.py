'''Useful functions dealing with paths.'''

from typing import Union


import asyncio
import os as _os
import shutil as _su
import atexit as _ex
import time as _t
import platform as _pl
import aiofiles.os
from pathlib import Path, _ignore_error as pathlib_ignore_error

from os import utime, walk, chmod, listdir
from os.path import *
from glob import glob

from . import logger
from .threading import Lock, ReadWriteLock, ReadRWLock, WriteRWLock
from .aio import srun


__all__ = ['exists_asyn', 'exists_timeout', 'remove_asyn', 'remove', 'make_dirs', 'lock', 'rename_asyn', 'rename', 'utime', 'walk', 'stat_asyn', 'stat', 'chmod', 'listdir', 'glob']


_path_lock = Lock()


async def exists_asyn(path: Union[Path, str], context_vars: dict = {}):
    '''An asyn function that checks if a path exists, regardless of it being a file or a folder.

    Parameters
    ----------
    path : str
        a path to a link, a file or a directory
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.

    Returns
    -------
    bool
        whether or not the path exists

    Notes
    -----
    Just like :func:`os.path.exists`. The function returns False for broken symbolic links.
    '''
    if not context_vars['async']:
        return exists(path)

    try:
        await aiofiles.os.stat(str(path))
    except OSError as e:
        if not pathlib_ignore_error(e):
            raise
        return False
    except ValueError:
        # Non-encodable path
        return False
    return True


async def exists_timeout(path: Union[Path, str], timeout: float = 1.0):
    '''Checks if a path exists for a number of seconds, raising an :class:`asyncio.TimeoutError` if timeout.

    Call this function rarely as the wrapping is expensive.

    Parameters
    ----------
    path : str
        a path to a link, a file or a directory

    Returns
    -------
    bool
        whether or not the path exists

    Notes
    -----
    Just like :func:`os.path.exists`. The function returns False for broken symbolic links.
    '''
    task = asyncio.ensure_future(exists_asyn(path, context_vars={'async': True}))
    retval = await asyncio.wait_for(task, timeout=timeout)
    return retval


async def remove_asyn(path: Union[Path, str], context_vars: dict = {}):
    '''An asyn function that removes a path completely, regardless of it being a file or a folder.

    If the path does not exist, do nothing.

    Parameters
    ----------
    path : str
        a path to a link, a file or a directory
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
    '''
    with _path_lock:
        if islink(path):
            _os.unlink(path)
        elif isfile(path):
            if context_vars['async']:
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


def remove(path: Union[Path, str]):
    '''Removes a path completely, regardless of it being a file or a folder.

    If the path does not exist, do nothing.

    Parameters
    ----------
    path : str
        a path to a link, a file or a directory
    '''
    return srun(remove_asyn, path)


def make_dirs(path: Union[Path, str], shared: bool = True):
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


def lock(path: Union[Path, str], to_write: bool = False):
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


async def rename_asyn(src: Union[Path, str], dst: Union[Path, str], context_vars: dict = {}):
    '''An asyn function that renames a file or a directory.

    Parameters
    ----------
    src : str
        path to the source file or directory
    dst : path
        new name also as a path
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
    '''

    if context_vars['async']:
        retval = await aiofiles.os.rename(src, dst)
        return retval

    return _os.rename(src, dst)


def rename(src: Union[Path, str], dst: Union[Path, str]):
    '''Renames a file or a directory.

    Parameters
    ----------
    src : str
        path to the source file or directory
    dst : path
        new name also as a path
    '''

    return srun(rename_asyn, src, dst)


async def stat_asyn(path: Union[Path, str], dir_fd=None, follow_symlinks=True, context_vars: dict = {}):
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
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.

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

    if context_vars['async']:
        retval = await aiofiles.os.stat(path, dir_fd=dir_fd, follow_symlinks=follow_symlinks)
        return retval

    return _os.stat(path, dir_fd=dir_fd, follow_symlinks=follow_symlinks)


def stat(path: Union[Path, str], dir_fd=None, follow_symlinks=True):
    '''Performs a stat system call on the given path.

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

    return srun(stat_asyn, path, dir_fd=dir_fd, follow_symlinks=follow_symlinks)


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
