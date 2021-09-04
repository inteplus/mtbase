'''Useful subroutines dealing with files asynchronously.'''


import aiofiles


__all__ = ['read_binary_async', 'read_binary']


async def read_binary_async(filepath, size: int = None):
    '''Opens a binary file and reads the content asynchronously.

    Parameters
    ----------
    filepath : str
        path to the file
    size : int
        size to read from the beginning of the file, in bytes. If None is given, read the whole
        file.

    Returns
    -------
    bytes
        the content read from file
    '''

    async with aiofiles.open(filepath, mode='rb') as f:
        content = await f.read(size)
    return content


def read_binary(filepath, size: int = None):
    '''Opens a binary file and reads the content in the usual IO-blocking way.

    Parameters
    ----------
    filepath : str
        path to the file
    size : int
        size to read from the beginning of the file, in bytes. If None is given, read the whole
        file.

    Returns
    -------
    bytes
        the content read from file
    '''
    with open(filepath, mode='rb') as f:
        return f.read(size)
