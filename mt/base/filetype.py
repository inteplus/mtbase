'''Extra subroutines using package 'filetype' asynchronously.'''


import aiofiles
from filetype import *
import filetype

from .asyn import srun, read_binary


__all__ = ['read_file_header', 'is_image', 'image_match']


async def read_file_header(filepath, asyn: bool = True):
    '''An asyn function that reads the file header so :module:`filetype` can work on.

    Parameters
    ----------
    filepath : str
        path to the file whose first 261 bytes would be read
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously

    Returns
    -------
    buf : bytes
        first 261 bytes of the file content or a ValueError is raised if the file is shorter
    '''

    buf = await read_binary(filepath, 261, asyn=asyn)
    if len(buf) < 261:
        raise ValueError("Corrupted file '{}' with only {} bytes.".format(filepath, len(buf)))

    return buf


def is_image(filepath, asynch: bool = False):
    '''An asynch function that checks if a file is an image.

    Parameters
    ----------
    filepath : str
        path to the file that can be an image file
    asynch : bool
        whether or not the file I/O is done asynchronously. If True, you must use keyword 'await'
        to invoke the function

    Returns
    -------
    bool
        whether or not the file is an image file

    See Also
    --------
    :func:`filetype.is_image`
        the wrapped function
    '''

    async def async_func(filepath):
        if not isinstance(filepath, str):
            return filetype.is_image(filepath)

        buf = await read_file_header(filepath)
        return is_image(buf)

    return async_func(filepath) if asynch else filetype.is_image(filepath)


def image_match(filepath, asynch: bool = False):
    '''An asynch function that obtains the image file type.

    Parameters
    ----------
    filepath : str
        path to the file that can be an image file
    asynch : bool
        whether or not the file I/O is done asynchronously. If True, you must use keyword 'await'
        to invoke the function

    Returns
    -------
    filetype.Type
        the file type, with mime and extension attributes
    '''

    async def async_func(filepath):
        if not isinstance(filepath, str):
            return filetype.image_match(filepath)

        buf = await read_file_header(filepath)
        return filetype.image_match(buf)

    return async_func(filepath) if asynch else filetype.image_match(filepath)
