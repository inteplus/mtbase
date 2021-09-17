'''Useful asyn functions dealing with files.'''


import json
import tempfile
import aiofiles

from ..contextlib import asynccontextmanager


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


async def write_binary(filepath, buf: bytes, context_vars : dict = {}):
    '''An asyn function that creates a binary file and writes the content.

    Parameters
    ----------
    filepath : str
        path to the file
    buf : bytes
        data (in bytes) to be written to the file
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
    '''

    if context_vars['async']:
        async with aiofiles.open(filepath, mode='wb') as f:
            return await f.write(buf)
    else:
        with open(filepath, mode='wb') as f:
            return f.write(buf)


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


async def write_text(filepath, buf: str, context_vars : dict = {}):
    '''An asyn function that creates a text file and writes the content.

    Parameters
    ----------
    filepath : str
        path to the file
    buf : str
        data (in bytes) to be written to the file
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
    '''

    if context_vars['async']:
        async with aiofiles.open(filepath, mode='wt') as f:
            return await f.write(buf)
    else:
        with open(filepath, mode='wt') as f:
            return f.write(buf)


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


async def json_save(filepath, obj, context_vars : dict = {}, **kwargs):
    '''An asyn function that saves a json-like object to a file.

    Parameters
    ----------
    filepath : str
        path to the file
    obj : object
        json-like object to be written to the file
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
    kwargs : dict
        keyword arguments passed as-is to :func:`json.dumps`
    '''

    content = json.dumps(obj, **kwargs)
    await write_text(filepath, content, context_vars=context_vars)


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
