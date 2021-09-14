'''Useful asyn functions dealing with files.'''


from typing import Union

import json
import tempfile
import aiofiles

from ..contextlib import asynccontextmanager


__all__ = ['read_binary', 'write_binary', 'read_text', 'write_text', 'json_load', 'json_save', 'mkdtemp']


async def read_binary(filepath, size: int = None, asyn: bool = True) -> bytes:
    '''An asyn function that opens a binary file and reads the content.

    Parameters
    ----------
    filepath : str
        path to the file
    size : int
        size to read from the beginning of the file, in bytes. If None is given, read the whole
        file.
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously

    Returns
    -------
    bytes
        the content read from file
    '''

    if asyn:
        async with aiofiles.open(filepath, mode='rb') as f:
            return await f.read(size)
    else:
        with open(filepath, mode='rb') as f:
            return f.read(size)


async def write_binary(filepath, buf: bytes, asyn: bool = True):
    '''An asyn function that creates a binary file and writes the content.

    Parameters
    ----------
    filepath : str
        path to the file
    buf : bytes
        data (in bytes) to be written to the file
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously
    '''

    if asyn:
        async with aiofiles.open(filepath, mode='wb') as f:
            return await f.write(buf)
    else:
        with open(filepath, mode='wb') as f:
            return f.write(buf)


async def read_text(filepath, size: int = None, asyn: bool = True) -> str:
    '''An asyn function that opens a text file and reads the content.

    Parameters
    ----------
    filepath : str
        path to the file
    size : int
        size to read from the beginning of the file, in bytes. If None is given, read the whole
        file.
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously

    Returns
    -------
    str
        the content read from file
    '''

    if asyn:
        async with aiofiles.open(filepath, mode='rt') as f:
            return await f.read(size)
    else:
        with open(filepath, mode='rt') as f:
            return f.read(size)


async def write_text(filepath, buf: str, asyn: bool = True):
    '''An asyn function that creates a text file and writes the content.

    Parameters
    ----------
    filepath : str
        path to the file
    buf : str
        data (in bytes) to be written to the file
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously
    '''

    if asyn:
        async with aiofiles.open(filepath, mode='wt') as f:
            return await f.write(buf)
    else:
        with open(filepath, mode='wt') as f:
            return f.write(buf)


async def json_load(filepath, asyn: bool = True, **kwargs):
    '''An asyn function that loads the json-like object of a file.

    Parameters
    ----------
    filepath : str
        path to the file
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously
    kwargs : dict
        keyword arguments passed as-is to :func:`json.loads`

    Returns
    -------
    object
        the loaded json-like object
    '''

    content = await read_text(filepath, asyn=asyn)
    return json.loads(content, **kwargs)


async def json_save(filepath, obj, asyn: bool = True, **kwargs):
    '''An asyn function that saves a json-like object to a file.

    Parameters
    ----------
    filepath : str
        path to the file
    obj : object
        json-like object to be written to the file
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously
    kwargs : dict
        keyword arguments passed as-is to :func:`json.dumps`
    '''

    content = json.dumps(obj, **kwargs)
    await write_text(filepath, content, asyn=asyn)


@asynccontextmanager
async def mkdtemp(asyn: bool = True):
    '''An asyn context manager that opens and creates a temporary directory.

    Parameters
    ----------
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously

    Returns
    -------
    tmpdir : object
        the context manager whose enter-value is a string containing the temporary dirpath.
    '''

    if asyn:
        async with aiofiles.tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir
    else:
        with tempfile.TemporaryDirectory() as tmpdir:
            yield tmpdir
