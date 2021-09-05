'''Useful subroutines dealing with downloading http files.'''


from typing import Optional
import aiohttp
import requests


__all__ = ['download']


async def download(url, asyn: bool = True, session: Optional[aiohttp.ClientSession] = None):
    '''An asyn function that opens a binary file and reads the content.

    Parameters
    ----------
    url : str
        a http or https URL
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously
    session : aiohttp.ClientSession, optional
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

    if session is None:
        raise ValueError("In asynchronous mode, an open client session is needed. But None has been provided.")

    async with session.get(url) as response:
        if response.status < 200 or response.status >= 300:
            raise IOError("Unhealthy response while downloading '{}'. Status: {}. Content-type: {}.".format(url, response.status, response.headers['content-type']))
        content = await response.read()
    return content
