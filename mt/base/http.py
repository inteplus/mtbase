'''Useful subroutines dealing with downloading http files.'''


from typing import Optional
import aiohttp
import requests

from .asyn import sleep, write_binary
from .path import chmod


__all__ = ['download', 'download_and_chmod']


async def download(url, asyn: bool = True, http_session: Optional[aiohttp.ClientSession] = None):
    '''An asyn function that opens a binary file and reads the content.

    Parameters
    ----------
    url : str
        a http or https URL
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously
    http_session : aiohttp.ClientSession, optional
        If the mode is asynchronous, an open client session must be provided. Otherwise the
        argument is ignored.

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

    if http_session is None:
        raise ValueError("In asynchronous mode, an open client session is needed. But None has been provided.")

    async with http_session.get(url) as response:
        if response.status < 200 or response.status >= 300:
            raise IOError("Unhealthy response while downloading '{}'. Status: {}. Content-type: {}.".format(url, response.status, response.headers['content-type']))
        content = await response.read()
    return content


async def download_and_chmod(url, filepath, file_mode=0o664, asyn: bool = True, http_session: Optional[aiohttp.ClientSession] = None):
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
    http_session : aiohttp.ClientSession, optional
        If the mode is asynchronous, an open client session must be provided. Otherwise the
        argument is ignored.
    '''

    content = await download(url, asyn=asyn, http_session=http_session)

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
