'''Extra subroutines using package 'filetype' asynchronously.'''


import asyncio
import aiofiles
from filetype import *


__all__ = ['read_file_header', 'is_image_aio', 'image_match_aio']


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


async def is_image_aio(filepath):
    '''Checks if a file is an image, asyncrhonously.

    Parameters
    ----------
    filepath : str
        path to the file that can be an image file

    Returns
    -------
    bool
        whether or not the file is an image file
    '''

    if not isinstance(filepath, str):
        return is_image(filepath)

    buf = await read_file_header(filepath)
    return is_image(buf)


async def image_match_aio(filepath):
    '''Obtains the image file type, asyncrhonously.

    Parameters
    ----------
    filepath : str
        path to the file that can be an image file

    Returns
    -------
    retval : filetype.Type
        the file type, with mime and extension attributes
    '''

    if not isinstance(filepath, str):
        return image_match(filepath)

    buf = await read_file_header(filepath)
    return image_match(buf)
