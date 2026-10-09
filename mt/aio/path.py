"""Useful functions dealing with paths and asyncio.

Asyn functions (named with an '_asyn' suffix, taking `context_vars`) have synchronous
counterparts without the suffix, which are the same functions run via :func:`mt.aio.srun`. The
module also re-exports :class:`pathlib.Path` and everything from :mod:`os.path`, and provides a
process-wide, per-path multiple-readers-single-writer lock, :func:`lock`.
"""

import errno
import os as _os
import shutil as _su
import atexit as _ex
import time as _t
import platform as _pl
import aiofiles.os
from aiofiles.base import wrap as aiofiles_wrap
from pathlib import Path

from os.path import *

from mt import tp, logg
from mt.threading import Lock, ReadWriteLock, ReadRWLock, WriteRWLock
from .base import srun, sleep

aiofiles_chmod = aiofiles_wrap(_os.chmod)


async def chmod_asyn(
    path: tp.Union[Path, str],
    mode: int,
    *args,
    dir_fd: tp.Optional[int] = None,
    follow_symlinks: bool = True,
    context_vars: dict = {},
) -> None:
    """An asyn function that changes the modes of path to the numeric mode.

    Parameters
    ----------
    path : str or Path
        a path to a link, a file or a directory
    mode : int
        the numeric mode to set for the path. See :func:`os.chmod` for details.
    args : tuple
        additional arguments to be passed to :func:`os.chmod`
    dir_fd : int, optional
        a file descriptor referring to a directory. If None, the path is interpreted relative to
        the current working directory.
    follow_symlinks : bool
        whether to follow symbolic links
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.

    See Also
    --------
    os.chmod
        The underlying function that performs the actual chmod system call. The parameters mode,
        dir_fd and follow_symlinks are passed to it directly.
    """
    if not context_vars["async"]:
        return _os.chmod(
            path, mode, *args, dir_fd=dir_fd, follow_symlinks=follow_symlinks
        )

    return await aiofiles_chmod(
        path, mode, *args, dir_fd=dir_fd, follow_symlinks=follow_symlinks
    )


async def exists_asyn(path: tp.Union[Path, str], context_vars: dict = {}):
    """An asyn function that checks if a path exists, regardless of it being a file or a folder.

    Parameters
    ----------
    path : str or Path
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

    Examples
    --------
    >>> import asyncio, tempfile
    >>> from mt.aio.path import exists_asyn
    >>> async def main():
    ...     with tempfile.TemporaryDirectory() as d:
    ...         return await exists_asyn(d, context_vars={"async": True})
    >>> asyncio.run(main())
    True
    >>> asyncio.run(exists_asyn("/nonexistent/path", context_vars={"async": True}))
    False
    """
    if not context_vars["async"]:
        return exists(path)

    try:
        return await aiofiles.os.path.exists(path)
    except OSError as e:
        from pathlib import _ignore_error

        if not _ignore_error(e):
            raise
        return False
    except ValueError:
        # Non-encodable path
        return False


async def remove_asyn(path: tp.Union[Path, str], context_vars: dict = {}):
    """An asyn function that removes a path completely, regardless of it being a file or a folder.

    If the path does not exist, do nothing. A symbolic link is unlinked, not followed. A directory
    is removed recursively. Only the removal of a regular file is truly asynchronous; links and
    directories are removed synchronously.

    Parameters
    ----------
    path : str or Path
        a path to a link, a file or a directory
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.

    See Also
    --------
    remove : the synchronous counterpart
    """
    if islink(path):
        _os.unlink(path)
    elif isfile(path):
        if context_vars["async"]:
            await aiofiles.os.remove(path)
        else:
            _os.remove(path)
    elif isdir(path):
        try:
            _su.rmtree(path)
        except OSError as e:
            if _pl.system() != "Windows":  # removal can sometimes fail on Windows
                raise


def remove(path: tp.Union[Path, str]):
    """Removes a path completely, regardless of it being a file or a folder.

    If the path does not exist, do nothing. A symbolic link is unlinked, not followed. A directory
    is removed recursively.

    Parameters
    ----------
    path : str or Path
        a path to a link, a file or a directory

    See Also
    --------
    remove_asyn : the asyn version

    Examples
    --------
    >>> import os, tempfile
    >>> from mt.aio.path import make_dirs, remove
    >>> with tempfile.TemporaryDirectory() as d:
    ...     p = os.path.join(d, "a", "b")
    ...     make_dirs(p)
    ...     remove(os.path.join(d, "a"))
    ...     os.path.exists(p)
    False
    """
    return srun(remove_asyn, path)


def make_dirs(path: tp.Union[Path, str], shared: bool = True):
    """Convenient invocation of `os.makedirs(path, exist_ok=True)`.

    If `shared` is True, every newly created folder will have permission 0o775 (explicitly
    chmodded, so independent of the umask). An empty path is ignored.

    Parameters
    ----------
    path : str or Path
        path to the folder to be created, along with any missing parent folders
    shared : bool
        whether every newly created folder is made group-writable (0o775)

    See Also
    --------
    make_dirs_asyn : the asyn version

    Examples
    --------
    >>> import os, tempfile
    >>> from mt.aio.path import make_dirs
    >>> with tempfile.TemporaryDirectory() as d:
    ...     p = os.path.join(d, "a", "b")
    ...     make_dirs(p)
    ...     make_dirs(p)  # already existing, no error
    ...     os.path.isdir(p), oct(os.stat(p).st_mode & 0o777)
    (True, '0o775')
    """
    return srun(make_dirs_asyn, path, shared=shared)


async def make_dirs_asyn(
    path: tp.Union[Path, str], shared: bool = True, context_vars: dict = {}
):
    """An asyn function wrapping `os.makedirs(path, exist_ok=True)`.

    If `shared` is True, every newly created folder will have permission 0o775 (explicitly
    chmodded, so independent of the umask). If `shared` is False, :func:`os.makedirs` is used
    with mode 0o775, subject to the umask. An empty path is ignored. At the end, it briefly waits
    (up to 3 times 0.1s) for the folder to become visible.

    Parameters
    ----------
    path : str or Path
        path to the folder to be created, along with any missing parent folders
    shared : bool
        whether every newly created folder is made group-writable (0o775)
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.

    See Also
    --------
    make_dirs : the synchronous counterpart
    """
    if not path:  # empty path, just ignore
        return
    if shared:
        stack = []
        while not await exists_asyn(path, context_vars=context_vars):
            head, tail = split(path)
            if not head:  # no slash in path
                stack.append(tail)
                path = "."
            elif not tail:  # slash at the end of path
                path = head
            else:  # normal case
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

    for i in range(3):
        if not await exists_asyn(path, context_vars=context_vars):
            await sleep(0.1, context_vars=context_vars)


def lock(path: tp.Union[Path, str], to_write: bool = False):
    """Returns the current MROW (multiple-readers-single-writer) lock for a given path.

    The locks are kept in a process-wide registry keyed by the path string, so every call with the
    same `path` in the same process shares one underlying read-write lock; different spellings of
    the same file are different keys. The returned object is a context manager. Locks that are
    free are garbage-collected from the registry from time to time. At interpreter exit, the
    process waits (up to a long time) for all path locks to be released.

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

    Examples
    --------
    >>> from mt.aio.path import lock
    >>> with lock("/some/file", to_write=True):
    ...     print("writing")
    writing
    >>> with lock("/some/file"):
    ...     print("reading")
    reading
    """
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

        return (
            WriteRWLock(lock.__locks[path])
            if to_write
            else ReadRWLock(lock.__locks[path])
        )


lock.__lock0 = Lock()
lock.__locks = {}
lock.__cleanup_cnt = 0


async def rename_asyn(
    src: tp.Union[Path, str],
    dst: tp.Union[Path, str],
    context_vars: dict = {},
    overwrite: bool = False,
):
    """An asyn function that renames a file or a directory.

    If the destination exists and `overwrite` is True, it is removed first (so the operation is
    not atomic).

    Parameters
    ----------
    src : str or Path
        path to the source file or directory
    dst : str or Path
        new name also as a path
    overwrite : bool
        whether or not (default) to overwrite the destination file if it exists
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.

    Raises
    ------
    FileExistsError
        if the target file exists and `overwrite` is False

    See Also
    --------
    rename : the synchronous counterpart
    """

    if await exists_asyn(dst, context_vars=context_vars):
        if overwrite:
            await remove_asyn(dst, context_vars=context_vars)
        else:
            raise FileExistsError(
                errno.EEXIST,
                f"Target file exists. Cannot rename source file '{src}' to",
                dst,
                src,
            )

    if context_vars["async"]:
        return await aiofiles.os.rename(src, dst)

    return _os.rename(src, dst)


def rename(src: tp.Union[Path, str], dst: tp.Union[Path, str], overwrite: bool = False):
    """Renames a file or a directory.

    Parameters
    ----------
    src : str or Path
        path to the source file or directory
    dst : str or Path
        new name also as a path
    overwrite : bool
        whether or not (default) to overwrite the destination file if it exists

    Raises
    ------
    FileExistsError
        if the target file exists and `overwrite` is False

    See Also
    --------
    rename_asyn : the asyn version

    Examples
    --------
    >>> import os, tempfile
    >>> from mt.aio.path import rename
    >>> with tempfile.TemporaryDirectory() as d:
    ...     for name in ("a", "b"):
    ...         with open(os.path.join(d, name), "w") as f:
    ...             n = f.write(name)
    ...     try:
    ...         rename(os.path.join(d, "a"), os.path.join(d, "b"))
    ...     except FileExistsError:
    ...         print("exists")
    ...     rename(os.path.join(d, "a"), os.path.join(d, "b"), overwrite=True)
    ...     sorted(os.listdir(d))
    exists
    ['b']
    """

    return srun(rename_asyn, src, dst, overwrite=overwrite)


async def stat_asyn(
    path: tp.Union[Path, str],
    dir_fd=None,
    follow_symlinks=True,
    context_vars: dict = {},
):
    """An asyn function that performs a stat system call on the given path.

    Parameters
    ----------
    path : str or Path
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

    See Also
    --------
    stat : the synchronous counterpart
    """

    if context_vars["async"]:
        return await aiofiles.os.stat(
            path, dir_fd=dir_fd, follow_symlinks=follow_symlinks
        )

    return _os.stat(path, dir_fd=dir_fd, follow_symlinks=follow_symlinks)


def stat(path: tp.Union[Path, str], dir_fd=None, follow_symlinks=True):
    """Performs a stat system call on the given path.

    Parameters
    ----------
    path : str or Path
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

    See Also
    --------
    stat_asyn : the asyn version

    Examples
    --------
    >>> import os, tempfile
    >>> from mt.aio.path import stat
    >>> with tempfile.TemporaryDirectory() as d:
    ...     fp = os.path.join(d, "a.txt")
    ...     with open(fp, "w") as f:
    ...         n = f.write("12345")
    ...     stat(fp).st_size
    5
    """

    return srun(stat_asyn, path, dir_fd=dir_fd, follow_symlinks=follow_symlinks)


async def relative_symlink_asyn(
    target: tp.Union[Path, str],
    destination: tp.Union[Path, str],
    context_vars: dict = {},
):
    """An asyn function that creates a symlink at ``destination`` pointing to ``target``.

    The link stores the path of ``target`` relative to the folder containing ``destination``, so
    the pair can be moved around together. The folder containing ``destination`` is created if it
    does not exist.

    Parameters
    ----------
    target : str or Path
        The target of the symlink (the file/directory that is pointed to)
    destination : str or Path
        The location of the symlink itself.
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.

    Raises
    ------
    FileExistsError
        if ``destination`` already exists

    See Also
    --------
    relative_symlink : the synchronous counterpart
    """
    target = Path(target)
    destination = Path(destination)
    target_dir = destination.parent
    await make_dirs_asyn(target_dir, context_vars=context_vars)
    relative_source = relpath(target, target_dir)
    dir_fd = _os.open(str(target_dir.absolute()), _os.O_RDONLY)
    # print(f"{relative_source} -> {destination.name} in {target_dir}")
    try:
        _os.symlink(relative_source, destination.name, dir_fd=dir_fd)
    finally:
        _os.close(dir_fd)


def relative_symlink(
    target: tp.Union[Path, str],
    destination: tp.Union[Path, str],
):
    """Creates a symlink at ``destination`` pointing to ``target``.

    The link stores the path of ``target`` relative to the folder containing ``destination``. The
    folder containing ``destination`` is created if it does not exist.

    Parameters
    ----------
    target : str or Path
        The target of the symlink (the file/directory that is pointed to)
    destination : str or Path
        The location of the symlink itself.

    Raises
    ------
    FileExistsError
        if ``destination`` already exists

    See Also
    --------
    relative_symlink_asyn : the asyn version

    Examples
    --------
    >>> import os, tempfile
    >>> from mt.aio.path import relative_symlink
    >>> with tempfile.TemporaryDirectory() as d:
    ...     target = os.path.join(d, "data.txt")
    ...     with open(target, "w") as f:
    ...         n = f.write("x")
    ...     relative_symlink(target, os.path.join(d, "links", "l"))
    ...     os.readlink(os.path.join(d, "links", "l"))
    '../data.txt'
    """
    return srun(relative_symlink_asyn, target, destination)


# exit function
def __exit_module():
    """Waits (up to 1024 times 5 seconds) at exit until all path locks of :func:`lock` are free."""
    # repeatedly wait until all locks are free
    for i in range(1024):
        cnt = sum((not v.is_free() for k, v in lock.__locks.items()))
        if not cnt:
            break
        logg.logger.info(f"waiting for {cnt} path locks to be free...")
        _t.sleep(5)


_ex.register(__exit_module)
