'''Useful subroutines dealing with asyn and asynch functions.

An asyn function is a coroutine function (declared with 'async def') but can operate in one of two
modes: asynchronously and synchronously, specified by a boolean keyword argument 'asyn'.
Asynchronicity means as usual an asynchronous function declared with 'async def'. However,
synchronicity means that the function can be invoked without an event loop, via invoking
:func:`srun`. Asyn functions are good for building library subroutines supporting both asynchronous
and synchronous modes, but they break backward compatibility because of they 'async' requirement.

An asynch function is a normal function (declared with 'def') but can operate in one of two modes:
asynchronously and synchronously, specified by a boolean keyword argument 'asynch'. When in
asynchronous mode, the function returns a coroutine that must be intercepted with keyword 'await',
as if this is a coroutine function. When in synchronous mode, the function behaves like a normal
function. Asynch functions are good for backward compatibility, because they are normal functions
that can pretend to be a coroutine function, but they are bad for developing library subroutines
supporting both asynchronous and synchronous modes.

'''


import time
import asyncio
import aiofiles


__all__ = ['srun', 'arun', 'arun2', 'sleep', 'read_binary', 'write_binary']


def srun(asyn_func, *args, **kwargs) -> object:
    '''Invokes an asyn function synchronously, without using keyword 'await'.

    Parameters
    ----------
    asyn_func : function
        an asyn function taking 'asyn' as a keyword argument
    args : list
        postitional arguments to be passed to the function
    kwargs : dict
        other keyword arguments to be passed to the function

    Returns
    -------
    object
        whatever the function returns
    '''

    try:
        coro = asyn_func(*args, asyn=False, **kwargs)
        coro.send(None)
        coro.close()
    except StopIteration as e:
        return e.value


def arun(asyn_func, *args, asynch: bool = False, **kwargs) -> object:
    '''Invokes an asyn function from inside an asynch function.

    Parameters
    ----------
    asyn_func : function
        an asyn function (declared with 'async def')
    args : list
        positional arguments of the asyn function
    asynch : bool
        whether to invoke the function asynchronously (True) or synchronously (False)
    kwargs : dict
        keyword arguments of the asyn function

    Returns
    -------
    object
        whatver the asyn function returns
    '''

    async def async_func(*args, **kwargs):
        return await asyn_func(*args, **kwargs)

    def sync_func(*args, **kwargs):
        return srun(asyn_func, *args, **kwargs)

    func = async_func if asynch else sync_func
    return func(*args, **kwargs)


async def arun2(asynch_func, *args, asyn: bool = True, **kwargs) -> object:
    '''Invokes an asynch function from inside an asyn function.

    Parameters
    ----------
    asyn_func : function
        an asyn function (declared with 'async def')
    args : list
        positional arguments of the asyn function
    asynch : bool
        whether to invoke the function asynchronously (True) or synchronously (False)
    kwargs : dict
        keyword arguments of the asyn function

    Returns
    -------
    object
        whatver the asyn function returns
    '''

    if asyn:
        retval = await asynch_func(*args, asynch=True, **kwargs)
    else:
        retval = asynch_func(*args, asynch=False, **kwargs)
    return retval


async def sleep(secs: float, asyn: bool = True):
    '''An asyn function that sleeps for a number of seconds.

    In asynchronous mode, it invokes :func:`asyncio.sleep`. In synchronous mode, it invokes
    :func:`time.sleep`.

    Parameters
    ----------
    secs : float
        number of seconds to sleep
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously
    '''

    if asyn:
        await asyncio.sleep(secs)
    else:
        time.sleep(secs)


async def read_binary(filepath, size: int = None, asyn: bool = True):
    '''An asyn function that opens a binary file and reads the content.

    Parameters
    ----------
    filepath : str
        path to the file
    size : int
        size to read from the beginning of the file, in bytes. If None is given, read the whole
        file.
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously

    Returns
    -------
    bytes
        the content read from file
    '''

    if asyn:
        async with aiofiles.open(filepath, mode='rb') as f:
            return await f.read(size)
    else:
        with open(filepath, mode='rb') as f:
            return f.read(size)


async def write_binary(filepath, buf: bytes, asyn: bool = True):
    '''An asyn function that creates a binary file and writes the content.

    Parameters
    ----------
    filepath : str
        path to the file
    buf : bytes
        data (in bytes) to be written to the file
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously

    Returns
    -------
    bytes
        the content read from file
    '''

    if asyn:
        async with aiofiles.open(filepath, mode='wb') as f:
            return await f.write(buf)
    else:
        with open(filepath, mode='wb') as f:
            return f.write(buf)
