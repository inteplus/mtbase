'''Base module dealing with asyn and asynch functions.'''

import time
import asyncio


__all__ = ['srun', 'arun', 'arun2', 'sleep', 'yield_control']


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


async def yield_control():
    '''Yields the control back to the current event loop.'''
    fut = asyncio.Future()
    fut.set_result(None)
    await fut
