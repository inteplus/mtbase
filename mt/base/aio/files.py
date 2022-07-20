'''Useful asyn functions dealing with files.'''


import os
import json
import tempfile
import asyncio
import aiofiles

from ..contextlib import asynccontextmanager
from .path import rename_asyn, rename, dirname, make_dirs


__all__ = ['safe_chmod', 'safe_rename', 'read_binary', 'write_binary',
           'read_text', 'write_text', 'json_load', 'json_save', 'mkdtemp',
           'CreateFileH5']


async def safe_chmod(filepath: str, file_mode: int = 0o664):
    try:
        return os.chmod(filepath, file_mode)
    except FileNotFoundError:
        await asyncio.sleep(1)
        return os.chmod(filepath, file_mode)


async def safe_rename(filepath: str, new_filepath: str, context_vars: dict = {}):
    try:
        return await rename_asyn(filepath, new_filepath, context_vars=context_vars, overwrite=True)
    except FileNotFoundError:
        await asyncio.sleep(1)
        return await rename_asyn(filepath, new_filepath, context_vars=context_vars, overwrite=True)


async def read_binary(filepath, size: int = None, context_vars: dict = {}) -> bytes:
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


async def write_binary(filepath, buf: bytes, file_mode: int = 0o664, context_vars: dict = {}, file_write_delayed: bool = False):
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
                await safe_chmod(filepath2, file_mode=file_mode)
            await safe_rename(filepath2, filepath, context_vars=context_vars)
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


async def read_text(filepath, size: int = None, context_vars: dict = {}) -> str:
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


async def write_text(filepath, buf: str, file_mode: int = 0o664, context_vars: dict = {}, file_write_delayed: bool = False):
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
                await safe_chmod(filepath2, file_mode=file_mode)
            await safe_rename(filepath2, filepath, context_vars=context_vars)
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


async def json_load(filepath, context_vars: dict = {}, **kwargs):
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


async def json_save(filepath, obj, file_mode: int = 0o664, context_vars: dict = {}, file_write_delayed: bool = False, **kwargs):
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
async def mkdtemp(context_vars: dict = {}):
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

class CreateFileH5:
    '''A context for creating an HDF5 file safely.

    It creates a temporary HDF5 file for writing. Once the user exits the context, the file is
    chmodded and renamed to a given name. Any intermediate folder that does not exist is created
    automatically.

    The context can be synchronous or asynchronous, by specifying the 'async' keyword and argument
    'context_vars'.

    Parameters
    ----------
    filepath : str
        local file path to be written to
    file_mode : int
        file mode to be set to using :func:`os.chmod`. If None is given, no setting of file mode
        will happen.
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not. Only
        used for the asynchronous mode.
    logger : logging.Logger, optional
        logger for debugging purposes

    Attributes
    ----------
    tmp_filepath : str
        the local file path of the temporary HDF5 file
    handle : h5py.File
        the handle of the temporary HDF5 file
    '''

    def __init__(self, filepath: str, file_mode: int = 0o664, context_vars: dict = {}, logger=None):
        dirpath = dirname(filepath)
        make_dirs(dirpath, shared=True)
        self.filepath = filepath
        self.file_mode = file_mode
        self.context_vars = context_vars
        self.logger = logger

    def __enter__(self):
        try:
            import h5py
        except ImportError:
            if logger:
                logger.error("Need h5py create file '{}'.".format(self.filepath))
            raise

        self.tmp_filepath = self.filepath+'.mttmp'
        self.handle = h5py.File(self.tmp_filepath, mode='w')
        return self

    async def __aenter__(self):
        return self.__enter__()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        if not exc_value is None: # successful
            if logger:
                logger.warn("File '{}' not removed.".format(self.tmp_filepath))
            return

        if self.file_mode is not None:  # chmod
            os.chmod(self.tmp_filepath, file_mode)
        rename(self.tmp_filepath, self.filepath, overwrite=True)

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        if not exc_value is None: # successful
            if logger:
                logger.warn("File '{}' not removed.".format(self.tmp_filepath))
            return

        if self.file_mode is not None:  # chmod
            await safe_chmod(self.tmp_filepath, self.file_mode)
        await safe_rename(self.tmp_filepath, self.filepath, context_vars=self.context_vars)
