'''An asynchronous version of imghdr.what.'''


import asyncio
import aiofiles
from imghdr import what


__all__ = ['what_aio']

async def what_aio(filepath):
    '''Checks the image type of a file by reading the first 32 bytes.

    Parameters
    ----------
    filepath : str
        path to the file that can be an image file

    Returns
    -------
    image_type : str or None
        image file type, or None if not detectable

    Raises
    ------
    ValueError
        if the file has less than 32 bytes
    '''

    if not isinstance(filepath, str):
        return what(filepath)

    async with aiofiles.open(filepath, mode='rb') as f:
        h = await f.read(32)

    if len(h) < 32:
        raise ValueError("Corrupted file '{}' with only {} bytes.".format(filepath, len(h)))

    return what(None, h=h)
