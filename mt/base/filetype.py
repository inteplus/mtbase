"""Extra subroutines using package 'filetype' asynchronously."""


import aiofiles
from filetype import *
import filetype

from mt.aio import srun, read_binary


__all__ = ["read_file_header", "is_image_asyn", "image_match_asyn"]


async def read_file_header(filepath, context_vars: dict = {}):
    """An asyn function that reads the file header so :module:`filetype` can work on.

    Parameters
    ----------
    filepath : str
        path to the file whose first 261 bytes would be read
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.

    Returns
    -------
    buf : bytes
        first 261 bytes of the file content or a ValueError is raised if the file is shorter
    """

    buf = await read_binary(filepath, 261, context_vars=context_vars)
    if len(buf) < 261:
        raise ValueError(
            "Corrupted file '{}' with only {} bytes.".format(filepath, len(buf))
        )

    return buf


async def is_image_asyn(filepath, context_vars: dict = {}):
    """An asyn function that checks if a file is an image.

    Parameters
    ----------
    filepath : str
        path to the file that can be an image file
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.

    Returns
    -------
    bool
        whether or not the file is an image file

    See Also
    --------
    :func:`filetype.is_image`
        the wrapped function
    """

    if not context_vars["async"] or not isinstance(filepath, str):
        return filetype.is_image(filepath)

    buf = await read_file_header(filepath, context_vars=context_vars)
    return filetype.is_image(buf)


async def image_match_asyn(filepath, context_vars: dict = {}):
    """An asyn function that obtains the image file type.

    Parameters
    ----------
    filepath : str
        path to the file that can be an image file
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.

    Returns
    -------
    filetype.Type
        the file type, with mime and extension attributes
    """

    if not context_vars["async"] or not isinstance(filepath, str):
        return filetype.image_match(filepath)

    buf = await read_file_header(filepath, context_vars=context_vars)
    return filetype.image_match(buf)
