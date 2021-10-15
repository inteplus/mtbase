'''Useful asyn functions dealing with files.'''


import os
import json
import tempfile
import asyncio
import aiofiles

from ..contextlib import asynccontextmanager
from .path import rename_asyn, rename


__all__ = ['read_binary', 'write_binary', 'read_text', 'write_text', 'json_load', 'json_save', 'mkdtemp']


async def read_binary(filepath, size: int = None, context_vars : dict = {}) -> bytes:
    '''An asyn function that opens a binary file and reads the content.

    Parameters
    ----------
    filepath : str
        path to the file
    size : int
        size to read from the beginning of the file, in bytes. If None is given, read the whole
        file.
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.

    Returns
    -------
    bytes
        the content read from file
    '''

    if context_vars['async']:
        async with aiofiles.open(filepath, mode='rb') as f:
            return await f.read(size)
    else:
        with open(filepath, mode='rb') as f:
            return f.read(size)


async def write_binary(filepath, buf: bytes, file_mode: int = 0o664, context_vars : dict = {}, file_write_delayed: bool = False):
    '''An asyn function that creates a binary file and writes the content.

    Parameters
    ----------
    filepath : str
        path to the file
    buf : bytes
        data (in bytes) to be written to the file
    file_mode : int
        file mode to be set to using :func:`os.chmod`. Only valid if fp is a string. If None is
        given, no setting of file mode will happen.
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
    file_write_delayed : bool
        Only valid in asynchronous mode. If True, wraps the file write task into a future and
        returns the future. In all other cases, proceeds as usual.

    Returns
    -------
    asyncio.Future or int
        either a future or the number of bytes written, depending on whether the file write
        task is delayed or not

    Notes
    -----
    The content is written to a file with '.mttmp' extension first before the file is renamed to
    the right file.
    '''

    if context_vars['async']:
        async def func(filepath, buf, file_mode):
            filepath2 = filepath+'.mttmp'
            async with aiofiles.open(filepath2, mode='wb') as f:
                retval = await f.write(buf)
            if file_mode is not None:  # chmod
                os.chmod(filepath2, file_mode)
            await rename_asyn(filepath2, filepath, overwrite=True)
            return retval
        coro = func(filepath, buf, file_mode)
        return asyncio.ensure_future(coro) if file_write_delayed else (await coro)

    filepath2 = filepath+'.mttmp'
    with open(filepath2, mode='wb') as f:
        retval = f.write(buf)
    if file_mode is not None:  # chmod
        os.chmod(filepath2, file_mode)
    rename(filepath2, filepath, overwrite=True)
    return retval


async def read_text(filepath, size: int = None, context_vars : dict = {}) -> str:
    '''An asyn function that opens a text file and reads the content.

    Parameters
    ----------
    filepath : str
        path to the file
    size : int
        size to read from the beginning of the file, in bytes. If None is given, read the whole
        file.
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.

    Returns
    -------
    str
        the content read from file
    '''

    if context_vars['async']:
        async with aiofiles.open(filepath, mode='rt') as f:
            return await f.read(size)
    else:
        with open(filepath, mode='rt') as f:
            return f.read(size)


async def write_text(filepath, buf: str, file_mode: int = 0o664, context_vars : dict = {}, file_write_delayed: bool = False):
    '''An asyn function that creates a text file and writes the content.

    Parameters
    ----------
    filepath : str
        path to the file
    buf : str
        data (in bytes) to be written to the file
    file_mode : int
        file mode to be set to using :func:`os.chmod`. Only valid if fp is a string. If None is
        given, no setting of file mode will happen.
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
    file_write_delayed : bool
        Only valid in asynchronous mode. If True, wraps the file write task into a future and
        returns the future. In all other cases, proceeds as usual.

    Returns
    -------
    asyncio.Future or int
        either a future or the number of bytes written, depending on whether the file write
        task is delayed or not

    Notes
    -----
    The content is written to a file with '.mttmp' extension first before the file is renamed to
    the right file.
    '''

    if context_vars['async']:
        async def func(filepath, buf, file_mode):
            filepath2 = filepath+'.mttmp'
            async with aiofiles.open(filepath2, mode='wt') as f:
                retval = await f.write(buf)
            if file_mode is not None:  # chmod
                os.chmod(filepath2, file_mode)
            await rename_asyn(filepath2, filepath, context_vars=context_vars, overwrite=True)
            return retval
        coro = func(filepath, buf, file_mode)
        return asyncio.ensure_future(coro) if file_write_delayed else (await coro)

    filepath2 = filepath+'.mttmp'
    with open(filepath2, mode='wt') as f:
        retval = f.write(buf)
    if file_mode is not None:  # chmod
        os.chmod(filepath2, file_mode)
    rename(filepath2, filepath, overwrite=True)
    return retval


async def json_load(filepath, context_vars : dict = {}, **kwargs):
    '''An asyn function that loads the json-like object of a file.

    Parameters
    ----------
    filepath : str
        path to the file
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
    kwargs : dict
        keyword arguments passed as-is to :func:`json.loads`

    Returns
    -------
    object
        the loaded json-like object
    '''

    content = await read_text(filepath, context_vars=context_vars)
    return json.loads(content, **kwargs)


async def json_save(filepath, obj, file_mode: int = 0o664, context_vars : dict = {}, file_write_delayed: bool = False, **kwargs):
    '''An asyn function that saves a json-like object to a file.

    Parameters
    ----------
    filepath : str
        path to the file
    obj : object
        json-like object to be written to the file
    file_mode : int
        file mode to be set to using :func:`os.chmod`. Only valid if fp is a string. If None is
        given, no setting of file mode will happen.
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
    file_write_delayed : bool
        Only valid in asynchronous mode. If True, wraps the file write task into a future and
        returns the future. In all other cases, proceeds as usual.
    kwargs : dict
        keyword arguments passed as-is to :func:`json.dumps`

    Returns
    -------
    asyncio.Future or int
        either a future or the number of bytes written, depending on whether the file write
        task is delayed or not
    '''

    content = json.dumps(obj, **kwargs)
    await write_text(filepath, content, file_mode=file_mode, context_vars=context_vars, file_write_delayed=file_write_delayed)


@asynccontextmanager
async def mkdtemp(context_vars : dict = {}):
    '''An asyn context manager that opens and creates a temporary directory.

    Parameters
    ----------
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.

    Returns
    -------
    tmpdir : object
        the context manager whose enter-value is a string containing the temporary dirpath.
    '''

    if context_vars['async']:
        async with aiofiles.tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir
    else:
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir
