"""Useful subroutines dealing with downloading http files.

The functions are asynchronous-capable: they take a `context_vars` dictionary, usually created
by :func:`mt.base.s3.create_context_vars`, which tells whether to run asynchronously or
synchronously and carries the shared http session. They need network access.
"""


import re
import asyncio
import aiohttp
import requests
import errno

from mt import ctx, aio, path


__all__ = ["download", "download_and_chmod"]


@ctx.asynccontextmanager
async def create_http_session(asyn: bool = True):
    """An asyn context manager that creates a http session.

    Parameters
    ----------
    asyn : bool
        whether the session is asynchronous or synchronous

    Returns
    -------
    http_session : aiohttp.ClientSession or None
        an open client session in asynchronous mode, closed upon leaving the `async with`
        statement. Otherwise, None.

    Examples
    --------
    >>> import asyncio
    >>> from mt.base.http import create_http_session
    >>> async def main():
    ...     async with create_http_session(asyn=False) as http_session:
    ...         return http_session
    ...
    >>> print(asyncio.run(main()))
    None
    """
    if asyn:
        async with aiohttp.ClientSession() as http_session:
            yield http_session
    else:
        yield None


async def download(url, context_vars: dict = {}):
    """An asyn function that downloads the content of a http or https url.

    In synchronous mode, :func:`requests.get` is used, without timeout. In asynchronous mode, the
    shared aiohttp session is used and the download is retried twice (after waiting 1s and then 10s)
    if the connection is dropped or times out. A response whose status is not 2xx is never retried.
    Whatever the mode, a short content (at most 5000 bytes) containing the phrase "Access Denied" or
    "AccessDenied" is treated as an error, as some servers return such a page with a 200 status.

    Parameters
    ----------
    url : str
        a http or https URL
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In asynchronous mode, variable 'http_session' must exist and hold an enter-result of an
        async with statement invoking :func:`create_http_session`.

    Returns
    -------
    bytes
        the content downloaded from the web

    Raises
    ------
    ConnectionAbortedError
        in asynchronous mode, if the response status is not 2xx
    requests.HTTPError
        in synchronous mode, if the response status is an error
    PermissionError
        if forbidden word "Access Denied" or "AccessDenied" is detected
    ValueError
        in asynchronous mode, if `context_vars['http_session']` is None

    See Also
    --------
    download_and_chmod : saves the downloaded content to a file
    """

    def check_content(content):
        """Returns the content, or raises PermissionError if it is a short "Access Denied" page."""
        if len(content) > 5000:
            return content
        if re.search(b"Access ?Denied", content):
            raise PermissionError(
                errno.EACCES,
                'Forbidden word "Access Denied" or "AccessDenied" detected in the content.',
                url,
            )
        return content

    if not context_vars["async"]:
        response = requests.get(url)
        if not response.ok:
            response.raise_for_status()
        return check_content(response.content)

    http_session = context_vars["http_session"]
    if http_session is None:
        raise ValueError(
            "In asynchronous mode, an open client session is needed. But None has been provided."
        )

    try:
        async with http_session.get(url) as response:
            if response.status < 200 or response.status >= 300:
                raise ConnectionAbortedError(
                    errno.ECONNABORTED,
                    f"Unhealthy response while downloading '{url}'. Status: {response.status}. "
                    f"Content-type: {response.headers['content-type']}.",
                    url,
                )
            content = await response.read()
    except (
        aiohttp.client_exceptions.ClientOSError,
        aiohttp.client_exceptions.ServerDisconnectedError,
        asyncio.exceptions.TimeoutError,
    ):
        try:
            await asyncio.sleep(1)  # wait 1s
            async with http_session.get(url) as response:
                if response.status < 200 or response.status >= 300:
                    raise ConnectionAbortedError(
                        errno.ECONNABORTED,
                        f"Unhealthy response while downloading '{url}'. Status: {response.status}. "
                        f"Content-type: {response.headers['content-type']}.",
                        url,
                    )
                content = await response.read()
        except (
            aiohttp.client_exceptions.ClientOSError,
            aiohttp.client_exceptions.ServerDisconnectedError,
            asyncio.exceptions.TimeoutError,
        ):
            await asyncio.sleep(10)  # wait 10s
            async with http_session.get(url) as response:
                if response.status < 200 or response.status >= 300:
                    raise ConnectionAbortedError(
                        errno.ECONNABORTED,
                        f"Unhealthy response while downloading '{url}'. Status: {response.status}. "
                        f"Content-type: {response.headers['content-type']}.",
                        url,
                    )
                content = await response.read()
    if not response.ok:
        response.raise_for_status()
    return check_content(content)


async def download_and_chmod(url, filepath, file_mode=0o664, context_vars: dict = {}):
    """An asyn function that downloads an http or https url as binary to a file with predefined
    permissions.

    After writing the file, the permissions are set. If the file is not visible yet (as may happen
    on network file systems), the chmod is retried after 0.1s, 1s and 10s.

    Parameters
    ----------
    url : str
        an http or https url
    filepath : str
        file location to save the content to
    file_mode : int, optional
        permission bits to be passed directly to `os.chmod()`, for example `0o664`. If None
        or 0, the permissions are left untouched.
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In asynchronous mode, variable 'http_session' must exist and hold an enter-result of an
        async with statement invoking :func:`create_http_session`.

    Raises
    ------
    ConnectionAbortedError
        if the downloaded content is empty, or for any reason given in :func:`download`

    See Also
    --------
    download : downloads the content without saving it
    """

    content = await download(url, context_vars=context_vars)

    if len(content) == 0:  # no content?
        raise ConnectionAbortedError(
            errno.ECONNABORTED,
            f"The downloaded content of '{url}' is empty.",
            url,
        )

    await aio.write_binary(filepath, content, context_vars=context_vars)

    if file_mode:  # chmod, attempt 1
        try:
            path.chmod(filepath, file_mode)
        except FileNotFoundError:  # attempt 2
            try:
                await aio.sleep(0.1, context_vars=context_vars)
                path.chmod(filepath, file_mode)
            except FileNotFoundError:  # attempt 3
                try:
                    await aio.sleep(1, context_vars=context_vars)
                    path.chmod(filepath, file_mode)
                except FileNotFoundError:  # attemp 4
                    await aio.sleep(10, context_vars=context_vars)
                    path.chmod(filepath, file_mode)
