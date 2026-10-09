"""Extra subroutines using package 'filetype' asynchronously.

Everything public in :mod:`filetype` is re-exported here. The functions in this module accept
the same inputs but can read the file header asynchronously. They take a `context_vars`
dictionary, as created by :func:`mt.base.s3.create_context_vars`, whose key 'async' selects
asynchronous or synchronous reading. Note that the argument has an empty dictionary as default
value, which means synchronous mode as a missing key 'async' is treated as False.
"""


from filetype import *
import filetype

from mt.aio import read_binary


__all__ = ["read_file_header", "is_image_asyn", "image_match_asyn"]


async def read_file_header(filepath, context_vars: dict = {}):
    """An asyn function that reads the file header so :mod:`filetype` can work on.

    Parameters
    ----------
    filepath : str
        path to the file whose first 261 bytes would be read
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not
        (False if missing).

    Returns
    -------
    buf : bytes
        first 261 bytes of the file content

    Raises
    ------
    ValueError
        if the file is shorter than 261 bytes, which is treated as a corrupted file
    """

    buf = await read_binary(filepath, 261, context_vars=context_vars)
    if len(buf) < 261:
        raise ValueError(
            f"Corrupted file '{filepath}' with only {len(buf)} bytes."
        )

    return buf


async def is_image_asyn(filepath, context_vars: dict = {}):
    r"""An asyn function that checks if a file is an image.

    In asynchronous mode and when `filepath` is a string, only the first 261 bytes of the file are
    read, using :func:`read_file_header`. Otherwise, `filepath` (which can also be bytes or a
    bytearray) is handed straight to :func:`filetype.is_image`.

    Parameters
    ----------
    filepath : str
        path to the file that can be an image file. A bytes-like object holding the header is
        also accepted.
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not
        (False if missing).

    Returns
    -------
    bool
        whether or not the file is an image file

    Raises
    ------
    ValueError
        in asynchronous mode, if the file is shorter than 261 bytes

    See Also
    --------
    :func:`filetype.is_image`
        the wrapped function

    Examples
    --------
    >>> import asyncio
    >>> from mt.base.filetype import is_image_asyn
    >>> png_header = b"\x89PNG\r\n\x1a\n" + bytes(300)
    >>> asyncio.run(is_image_asyn(png_header, context_vars={"async": False}))
    True
    >>> asyncio.run(is_image_asyn(b"hello", context_vars={"async": False}))
    False
    """

    if not context_vars.get("async", False) or not isinstance(filepath, str):
        return filetype.is_image(filepath)

    buf = await read_file_header(filepath, context_vars=context_vars)
    return filetype.is_image(buf)


async def image_match_asyn(filepath, context_vars: dict = {}):
    r"""An asyn function that obtains the image file type.

    It follows the same reading rules as :func:`is_image_asyn`.

    Parameters
    ----------
    filepath : str
        path to the file that can be an image file. A bytes-like object holding the header is
        also accepted.
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not
        (False if missing).

    Returns
    -------
    filetype.Type or None
        the file type, with mime and extension attributes, or None if the content is not
        a recognised image

    Raises
    ------
    ValueError
        in asynchronous mode, if the file is shorter than 261 bytes

    Examples
    --------
    >>> import asyncio
    >>> from mt.base.filetype import image_match_asyn
    >>> png_header = b"\x89PNG\r\n\x1a\n" + bytes(300)
    >>> kind = asyncio.run(image_match_asyn(png_header, context_vars={"async": False}))
    >>> kind.mime, kind.extension
    ('image/png', 'png')
    """

    if not context_vars.get("async", False) or not isinstance(filepath, str):
        return filetype.image_match(filepath)

    buf = await read_file_header(filepath, context_vars=context_vars)
    return filetype.image_match(buf)
