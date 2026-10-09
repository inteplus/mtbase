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

    The coroutine is driven by a single `send(None)`, so the asyn function must complete without
    ever actually suspending, which holds when it only awaits other asyn functions run in
    synchronous mode (e.g. :func:`sleep`) and never awaits a real future. If the coroutine
    suspends, it is closed and None is returned silently. No event loop is needed.

    Parameters
    ----------
    asyn_func : function
        an asyn function (declared with 'async def') taking 'context_vars' as a keyword argument
    *args : iterable
        postitional arguments to be passed to the function
    extra_context_vars : dict
        additional context variables to be passed to the function. The 'context_vars' keyword to be
        passed to the function will be `{'async': False}` updated with `extra_context_vars`
    **kwds : dict
        other keyword arguments to be passed to the function

    Returns
    -------
    object
        whatever the function returns

    See Also
    --------
    arun : to invoke an asyn function from inside an asynch function

    Examples
    --------
    >>> from mt import aio
    >>> async def double_later(x, context_vars={}):
    ...     await aio.sleep(0.01, context_vars=context_vars)
    ...     return 2 * x
    >>> aio.srun(double_later, 3)
    6
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

    In asynchronous mode (`context_vars['async']` is True), it returns a coroutine that must be
    awaited. In synchronous mode, it is meant to run the function via :func:`srun`.

    Note that in the current implementation, the synchronous branch forwards `context_vars` both
    explicitly and via `**kwds` to :func:`srun`, which raises a :class:`TypeError`. Use
    :func:`srun` directly in synchronous mode.

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
        in asynchronous mode, a coroutine whose result is whatever the asyn function returns

    See Also
    --------
    arun2 : the opposite direction, invoking an asynch function from an asyn function

    Examples
    --------
    >>> import asyncio
    >>> from mt import aio
    >>> async def double_later(x, context_vars={}):
    ...     await aio.sleep(0.01, context_vars=context_vars)
    ...     return 2 * x
    >>> def asynch_double(x, context_vars={}):  # an asynch function
    ...     return aio.arun(double_later, x, context_vars=context_vars)
    >>> async def main():
    ...     return await asynch_double(4, context_vars={"async": True})
    >>> asyncio.run(main())
    8
    """

    async def async_func(*args, **kwds):
        return await asyn_func(*args, **kwds)

    def sync_func(*args, **kwds):
        return srun(asyn_func, *args, **kwds)

    func = async_func if context_vars["async"] else sync_func
    return func(*args, context_vars=context_vars, **kwds)


async def arun2(asynch_func, *args, context_vars: dict = {}, **kwds) -> object:
    """Invokes an asynch function from inside an asyn function.

    The asynch function is called with `context_vars` as a keyword argument. In asynchronous mode
    the coroutine it returns is awaited; in synchronous mode its return value is used directly.

    Parameters
    ----------
    asynch_func : function
        an asynch function (declared with 'def'), taking 'context_vars' as a keyword argument
    *args : iterable
        positional arguments of the asynch function
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
    **kwds : dict
        other keyword arguments of the asynch function

    Returns
    -------
    object
        whatever the asynch function returns (after awaiting it in asynchronous mode)

    See Also
    --------
    arun : the opposite direction, invoking an asyn function from an asynch function

    Examples
    --------
    >>> import asyncio
    >>> from mt import aio
    >>> def plus_one(x, context_vars={}):  # an asynch function in synchronous mode
    ...     return x + 1
    >>> async def main():
    ...     return await aio.arun2(plus_one, 1, context_vars={"async": False})
    >>> asyncio.run(main())
    2
    """

    if context_vars["async"]:
        retval = await asynch_func(*args, context_vars=context_vars, **kwds)
    else:
        retval = asynch_func(*args, context_vars=context_vars, **kwds)
    return retval


async def sleep(secs: float, context_vars: dict = {}):
    """An asyn function that sleeps for a number of seconds.

    In asynchronous mode, it invokes :func:`asyncio.sleep`, yielding control to the event loop. In
    synchronous mode, it invokes :func:`time.sleep`, blocking the thread.

    Parameters
    ----------
    secs : float
        number of seconds to sleep
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.

    Examples
    --------
    >>> import asyncio
    >>> from mt import aio
    >>> asyncio.run(aio.sleep(0.01, context_vars={"async": True}))
    >>> aio.srun(aio.sleep, 0.01)
    """

    if context_vars["async"]:
        await asyncio.sleep(secs)
    else:
        time.sleep(secs)


async def yield_control():
    """Yields the control back to the current event loop for one iteration.

    Await it inside a long-running coroutine to let other tasks run. It is equivalent to
    `await asyncio.sleep(0)`.

    Examples
    --------
    >>> import asyncio
    >>> from mt import aio
    >>> asyncio.run(aio.yield_control())
    """
    await asyncio.sleep(0)
