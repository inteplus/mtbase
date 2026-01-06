"""Additional `shutil`_ stuff related to the terminal.

Instead of:

.. code-block:: python

   import shutil

You do:

.. code-block:: python

   from mt import shutil

It will import shutil plus the additional stuff implemented here.

Please see Python package `shutil`_ for more details.

.. _shutil:
   https://docs.python.org/3/library/shutil.html
"""

import aioshutil as aio
from shutil import *


__all__ = ["stty_imgres", "stty_size"]


def stty_size():
    """Gets the terminal size.

    Returns the Linux-compatible console's number of rows and number of characters per
    row. If the information does not exist, returns (72, 128)."""

    res = get_terminal_size(fallback=(128, 72))
    return res[1], res[0]


def stty_imgres():
    """Gets the terminal resolution.

    Returns the Linux-compatible console's number of letters per row and the number of
    rows. If the information does not exist, returns (128, 72)."""

    res = get_terminal_size(fallback=(128, 72))
    return [res[0], res[1]]


async def copyfileobj_asyn(fsrc, fdst, length=16 * 1024, context_vars: dict = {}):
    """An asyn version of `shutil.copyfileobj`_.

    Parameters
    ----------
    fsrc : file object
        Source file object.
    fdst : file object
        Destination file object.
    length : int, optional
        Buffer size. Default is 16 * 1024.
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.

    .. _shutil.copyfileobj:
       https://docs.python.org/3/library/shutil.html#shutil.copyfileobj
    """
    if not context_vars.get("async", True):
        return copyfileobj(fsrc, fdst, length=length)

    return await aio.copyfileobj(fsrc, fdst, length=length)


async def copyfile_asyn(src, dst, *, follow_symlinks=True, context_vars: dict = {}):
    """An asyn version of `shutil.copyfile`_.

    Parameters
    ----------
    src : str
        Source file path.
    dst : str
        Destination file path.
    follow_symlinks : bool, optional
        Whether to follow symlinks. Default is True.
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.

    .. _shutil.copyfile:
       https://docs.python.org/3/library/shutil.html#shutil.copyfile
    """
    if not context_vars.get("async", True):
        return copyfile(src, dst, follow_symlinks=follow_symlinks)

    return await aio.copyfile(src, dst, follow_symlinks=follow_symlinks)


async def copymode_asyn(src, dst, *, follow_symlinks=True, context_vars: dict = {}):
    """An asyn version of `shutil.copymode`_.

    Parameters
    ----------
    src : str
        Source file path.
    dst : str
        Destination file path.
    follow_symlinks : bool, optional
        Whether to follow symlinks. Default is True.
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.

    .. _shutil.copymode:
       https://docs.python.org/3/library/shutil.html#shutil.copymode
    """
    if not context_vars.get("async", True):
        return copymode(src, dst, follow_symlinks=follow_symlinks)

    return await aio.copymode(src, dst, follow_symlinks=follow_symlinks)


async def copystat_asyn(src, dst, *, follow_symlinks=True, context_vars: dict = {}):
    """An asyn version of `shutil.copystat`_.

    Parameters
    ----------
    src : str
        Source file path.
    dst : str
        Destination file path.
    follow_symlinks : bool, optional
        Whether to follow symlinks. Default is True.
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.

    .. _shutil.copystat:
       https://docs.python.org/3/library/shutil.html#shutil.copystat
    """
    if not context_vars.get("async", True):
        return copystat(src, dst, follow_symlinks=follow_symlinks)

    return await aio.copystat(src, dst, follow_symlinks=follow_symlinks)


async def copy_asyn(src, dst, *, follow_symlinks=True, context_vars: dict = {}):
    """An asyn version of `shutil.copy`_.

    Parameters
    ----------
    src : str
        Source file path.
    dst : str
        Destination file path.
    follow_symlinks : bool, optional
        Whether to follow symlinks. Default is True.
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.

    .. _shutil.copy:
       https://docs.python.org/3/library/shutil.html#shutil.copy
    """
    if not context_vars.get("async", True):
        return copy(src, dst, follow_symlinks=follow_symlinks)

    return await aio.copy(src, dst, follow_symlinks=follow_symlinks)


async def copy2_asyn(src, dst, *, follow_symlinks=True, context_vars: dict = {}):
    """An asyn version of `shutil.copy2`_.

    Parameters
    ----------
    src : str
        Source file path.
    dst : str
        Destination file path.
    follow_symlinks : bool, optional
        Whether to follow symlinks. Default is True.
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.

    .. _shutil.copy2:
       https://docs.python.org/3/library/shutil.html#shutil.copy2
    """
    if not context_vars.get("async", True):
        return copy2(src, dst, follow_symlinks=follow_symlinks)

    return await aio.copy2(src, dst, follow_symlinks=follow_symlinks)
