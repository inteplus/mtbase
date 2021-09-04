'''Extra subroutines using package 'filetype' asynchronously.'''


import aiofiles
import filetype
from filetype import *


__all__ = ['read_file_header', 'is_image', 'image_match']


async def read_file_header(filepath):
    '''Asynchronously reads the file header so :module:`filetype` can work on.

    Parameters
    ----------
    filepath : str
        path to the file whose first 261 bytes would be read

    Returns
    -------
    buf : bytes
        first 261 bytes of the file content or a ValueError is raised if the file is shorter
    '''

    async with aiofiles.open(filepath, mode='rb') as f:
        buf = await f.read(261)

    if len(buf) < 261:
        raise ValueError("Corrupted file '{}' with only {} bytes.".format(filepath, len(buf)))

    return buf


def is_image(filepath, asynchronous: bool = False):
    '''Checks if a file is an image.

    Parameters
    ----------
    filepath : str
        path to the file that can be an image file
    asynchronous : bool
        whether or not the file reading is done asynchronously. If True, you must use 'await'
        keyword to process the returned value

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

    return async_func(filepath) if asynchronous else filetype.is_image(filepath)


def image_match(filepath, asynchronous: bool = False):
    '''Obtains the image file type.

    Parameters
    ----------
    filepath : str
        path to the file that can be an image file
    asynchronous : bool
        whether or not the file reading is done asynchronously. If True, you must use 'await'
        keyword to process the returned value

    Returns
    -------
    retval : filetype.Type
        the file type, with mime and extension attributes
    '''

    async def async_func(filepath):
        if not isinstance(filepath, str):
            return filetype.image_match(filepath)

        buf = await read_file_header(filepath)
        return filetype.image_match(buf)

    return async_func(filepath) if asynchronous else filetype.image_match(filepath)
