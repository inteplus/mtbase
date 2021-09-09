'''Useful subroutines dealing with downloading http files.'''


from typing import Optional
import aiohttp
import requests

from .contextlib import asynccontextmanager

from .aio import sleep, write_binary
from .path import chmod


__all__ = ['download', 'download_and_chmod']


@asynccontextmanager
async def create_http_session(asyn: bool = True):
    '''An asyn context manager that creates a http session.

    Parameters
    ----------
    asyn : bool
        whether the session is asynchronous or synchronous

    Returns
    -------
    http_session : aiohttp.ClientSession, optional
        an open client session in asynchronous mode. Otherwise, None.
    '''
    if asyn:
        async with aiohttp.ClientSession() as http_session:
            yield http_session
    else:
        yield None


async def download(url, asyn: bool = True, context_vars: dict = {}):
    '''An asyn function that opens a binary file and reads the content.

    Parameters
    ----------
    url : str
        a http or https URL
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously
    context_vars : dict
        dictionary of context variables with which the function runs. In asynchronous mode,
        variable 'http_session' must exist and hold an enter-result of an async with statement
        invoking :func:`create_http_session`.

    Returns
    -------
    bytes
        the content downloaded from the web

    Raises
    ------
    IOError
        if there is a problem downloading
    '''

    if not asyn:
        return requests.get(url).content

    http_session = context_vars['http_session']
    if http_session is None:
        raise ValueError("In asynchronous mode, an open client session is needed. But None has been provided.")

    async with http_session.get(url) as response:
        if response.status < 200 or response.status >= 300:
            raise IOError("Unhealthy response while downloading '{}'. Status: {}. Content-type: {}.".format(url, response.status, response.headers['content-type']))
        content = await response.read()
    return content


async def download_and_chmod(url, filepath, file_mode=0o664, asyn: bool = True, context_vars: dict = {}):
    '''An asyn function that downloads an http or https url as binary to a file with predefined permissions.

    Parameters
    ----------
    url : str
       an http or https url
    filepath : str
       file location to save the content to
    file_mode : int
        to be passed directly to `os.chmod()` if not None
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously
    context_vars : dict
        dictionary of context variables with which the function runs. In asynchronous mode,
        variable 'http_session' must exist and hold an enter-result of an async with statement
        invoking :func:`create_http_session`.
    '''

    content = await download(url, asyn=asyn, context_vars=context_vars)

    await write_binary(filepath, content, asyn=asyn)

    if file_mode:  # chmod, attempt 1
        try:
            chmod(filepath, file_mode)
        except FileNotFoundError: # attempt 2
            try:
                await sleep(0.1, asyn=asyn)
                chmod(filepath, file_mode)
            except FileNotFoundError: # attempt 3
                try:
                    await sleep(1, asyn=asyn)
                    chmod(filepath, file_mode)
                except FileNotFoundError: # attemp 4
                    await sleep(10, asyn=asyn)
                    chmod(filepath, file_mode)
