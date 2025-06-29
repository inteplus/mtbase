"""Base module dealing with asyn and asynch functions."""

import time
import asyncio

from ..ipython import inside_ipython


if inside_ipython():
    try:
        import nest_asyncio

        nest_asyncio.apply()
        ipython = "inside_with_asyncio_nested"
    except ImportError:
        ipython = "inside_without_asyncio_nested"
else:
    ipython = "outside"


def srun(asyn_func, *args, extra_context_vars: dict = {}, **kwds) -> object:
    """Invokes an asyn function synchronously, without using keyword 'await'.

    Parameters
    ----------
    asyn_func : function
        an asyn function taking 'asyn' as a keyword argument
    *args : iterable
        postitional arguments to be passed to the function
    extra_context_vars : dict
        additional context variables to be passed to the function. The 'context_vars' keyword to be
        passed to the function will be `{'async': False}.update(extra_context_vars)`
    **kwds : dict
        other keyword arguments to be passed to the function

    Returns
    -------
    object
        whatever the function returns
    """

    try:
        context_vars = {"async": False}
        context_vars.update(extra_context_vars)
        coro = asyn_func(*args, context_vars=context_vars, **kwds)
        coro.send(None)
        coro.close()
    except StopIteration as e:
        return e.value


def arun(asyn_func, *args, context_vars: dict = {}, **kwds) -> object:
    """Invokes an asyn function from inside an asynch function.

    Parameters
    ----------
    asyn_func : function
        an asyn function (declared with 'async def')
    *args : iterable
        positional arguments of the asyn function
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
    **kwds : dict
        other keyword arguments of the asyn function

    Returns
    -------
    object
        whatver the asyn function returns
    """

    async def async_func(*args, **kwds):
        return await asyn_func(*args, **kwds)

    def sync_func(*args, **kwds):
        return srun(asyn_func, *args, **kwds)

    func = async_func if context_vars["async"] else sync_func
    return func(*args, context_vars=context_vars, **kwds)


async def arun2(asynch_func, *args, context_vars: dict = {}, **kwds) -> object:
    """Invokes an asynch function from inside an asyn function.

    Parameters
    ----------
    asyn_func : function
        an asyn function (declared with 'async def')
    *args : iterable
        positional arguments of the asyn function
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
    **kwds : dict
        other keyword arguments of the asynch function

    Returns
    -------
    object
        whatver the asyn function returns
    """

    if context_vars["async"]:
        retval = await asynch_func(*args, context_vars=context_vars, **kwds)
    else:
        retval = asynch_func(*args, context_vars=context_vars, **kwds)
    return retval


async def sleep(secs: float, context_vars: dict = {}):
    """An asyn function that sleeps for a number of seconds.

    In asynchronous mode, it invokes :func:`asyncio.sleep`. In synchronous mode, it invokes
    :func:`time.sleep`.

    Parameters
    ----------
    secs : float
        number of seconds to sleep
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
    """

    if context_vars["async"]:
        await asyncio.sleep(secs)
    else:
        time.sleep(secs)


async def yield_control():
    """Yields the control back to the current event loop."""
    await asyncio.sleep(0)
