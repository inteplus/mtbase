"""Concurrency using asyncio and multiprocessing.

It provides, from simplest to heaviest: :func:`aio_work_generator` (asyncio in one process),
:func:`asyn_work_generator` (asyncio in several processes, reporting per-task outcomes), and
:func:`asyn_pmap` (a multiprocessing pool of asyncio workers, mapping over an iterable).
"""

import typing as tp
import asyncio
import multiprocessing as mp
import queue as _q
import itertools
import functools

from mt import traceback

from .base import split_works
from ..ctx import nullcontext


async def aio_work_generator(
    func,
    num_work_ids,
    skip_null: bool = True,
    func_args: tuple = (),
    func_kwargs: dict = {},
    max_concurrency: int = 1024,
):
    """An asynchronous generator that does some works and yields the work results.

    This function uses asyncio to do works concurrently. The number of concurrent works is
    optionally upper-bounded by `max_concurrency`. It is of good use when the works are IO-bound.
    Otherwise, :class:`WorkIterator` or :func:`serial_work_generator` are more suitable options.

    Results are yielded in completion order, not in work id order. All coroutine objects are
    created upfront (but only started when scheduled). The first exception raised by a work is
    re-raised from the generator, ending it.

    Parameters
    ----------
    func : function
        a coroutine function (defined with 'async def') representing the work process. The function
        takes as input a non-negative integer 'work_id' and optionally some keyword arguments. It
        returns some result in the form of `(work_id, ...)` if successful else None.
    num_work_ids : int
        number of works to iterate over without using multiprocessing or multithreading.
    skip_null : bool, optional
        whether or not to skip the iteration that contains None as the work result.
    func_args : tuple
        additional positional arguments to be passed to the function as-is
    func_kwargs : dict
        additional keyword arguments to be passed to the function as-is
    max_concurrency : int
        the maximum number of concurrent works at any time, good for managing memory allocations.
        If None is given, all works will be scheduled to run at once.

    Returns
    -------
    object
        an asynchronous generator yielding each result in the form `(work_id, ...)`

    See Also
    --------
    mt.concurrency.serial_work_generator : the serial version

    Examples
    --------
    >>> import asyncio
    >>> from mt.concurrency import aio_work_generator
    >>> async def square(work_id, offset=0):
    ...     await asyncio.sleep(0.01)
    ...     return (work_id, work_id * work_id + offset)
    >>> async def main():
    ...     results = [r async for r in aio_work_generator(square, 4, max_concurrency=2)]
    ...     return sorted(results)
    >>> asyncio.run(main())
    [(0, 0), (1, 1), (2, 4), (3, 9)]
    >>> async def main2():
    ...     gen = aio_work_generator(square, 2, func_kwargs={"offset": 100})
    ...     return sorted([r async for r in gen])
    >>> asyncio.run(main2())
    [(0, 100), (1, 101)]
    """

    coros = [
        func(work_id, *func_args, **func_kwargs) for work_id in range(num_work_ids)
    ]

    if max_concurrency is None:
        for coro in asyncio.as_completed(coros):
            result = await coro
            if not skip_null or result is not None:
                yield result
    else:
        pos = 0
        cur_task_list = []
        while (pos < num_work_ids) or (len(cur_task_list) > 0):
            # add tasks to run concurrently
            spare = min(max_concurrency - len(cur_task_list), num_work_ids - pos)
            if spare > 0:
                new_task_list = [
                    asyncio.ensure_future(coros[pos + i]) for i in range(spare)
                ]
                cur_task_list.extend(new_task_list)
                pos += spare

            # get some tasks done
            done_task_list, cur_task_list = await asyncio.wait(
                cur_task_list, return_when=asyncio.FIRST_COMPLETED
            )
            cur_task_list = list(cur_task_list)

            # yield the results
            for done_task in done_task_list:
                e = done_task.exception()
                if e is not None:
                    raise e
                result = done_task.result()
                if not skip_null or result is not None:
                    yield result


async def run_asyn_works_in_context(
    progress_queue: mp.Queue,
    func,
    func_args: tuple = (),
    func_kwargs: dict = {},
    context_id=None,
    work_id_list: list = [],
    max_concurrency: int = 1024,
    context_vars: dict = {},
):
    """Invokes the same asyn function with different work ids concurrently and asynchronously,
    in a given context.

    The function is called as `func(work_id, *func_args, context_vars=context_vars,
    **func_kwargs)` for every work id in `work_id_list`, as asyncio tasks, with at most
    `max_concurrency` of them alive at a time. Progress messages are put into `progress_queue`.

    Parameters
    ----------
    progress_queue: multiprocessing.Queue
        a shared queue so that the main process can observe the progress inside the context. See
        notes below. Any object with a `put_nowait` method works.
    func : function
        an asyn function that may return something and may raise an Exception. The function must
        have the first argument being the work id, and accept the keyword argument
        'context_vars', which is the dictionary given below.
    func_args : tuple
        additional positional arguments to be passed to the function as-is
    func_kwargs : dict
        additional keyword arguments to be passed to the function as-is
    context_id : int, optional
        the context id to be assigned to the new context. Default is None if we do not care.
    work_id_list : list
        list of work ids to be passed to the function
    max_concurrency : int
        the maximum number of concurrent works in the context at any time, good for managing memory
        allocations. If None is given, all works in the context will be scheduled to run at once.
    context_vars : dict
        a dictionary of context variables passed as-is to the function. It should include
        `context_vars['async']` to tell the function whether to run asynchronously or not. If the
        function uses S3 or HTTP, it should also hold 's3_client' (see
        :func:`mt.base.s3.create_s3_client`) and, in asynchronous mode, 'http_session' (see
        :func:`mt.base.http.create_http_session`). You can use
        :func:`mt.base.s3.create_context_vars` to create a dictionary like this.

    Notes
    -----
    This function returns nothing but while it is running, the progress queue regularly receives
    messages. Each message is a tuple `(context_id, message_code, work_id, ...)`. The message codes
    are: 'task_scheduled' (when the task is created), 'task_returned' (extra item: the returned
    value), 'task_raised' (extra items: the exception and the call stack as text) and
    'task_cancelled'. The user should process the progress queue.

    If KeyboardInterrupt is raised during the invocation, all remaining unscheduled tasks are
    cancelled. This means for the case that max_concurrency is None, KeyboardInterrupt has no
    effect.

    See Also
    --------
    asyn_work_generator : runs this function in several processes and yields the messages

    Examples
    --------
    >>> import asyncio, queue
    >>> from mt.concurrency import run_asyn_works_in_context
    >>> async def inc(work_id, offset=0, context_vars={}):
    ...     if work_id == 1:
    ...         raise ValueError("bad id")
    ...     return work_id + offset
    >>> async def main():
    ...     q = queue.Queue()
    ...     await run_asyn_works_in_context(
    ...         q, inc, func_kwargs={"offset": 10}, context_id=0, work_id_list=[0, 1, 2],
    ...         max_concurrency=1, context_vars={"async": True},
    ...     )
    ...     return [q.get_nowait()[:4] for _ in range(q.qsize())]
    >>> asyncio.run(main())
    [(0, 'task_scheduled', 0), (0, 'task_returned', 0, 10), (0, 'task_scheduled', 1),
     (0, 'task_raised', 1, ValueError('bad id')), (0, 'task_scheduled', 2),
     (0, 'task_returned', 2, 12)]
    """

    def get_done_task_result(task, work_id):
        if task.cancelled():
            return (context_id, "task_cancelled", work_id)
        e = task.exception()
        if e is not None:
            import io

            tracestack = io.StringIO()
            task.print_stack(file=tracestack)
            return (context_id, "task_raised", work_id, e, tracestack.getvalue())
        result = task.result()
        return (context_id, "task_returned", work_id, result)

    import asyncio

    keyboard_interrupted = False

    if max_concurrency is None:
        task_map = {}
        for work_id in work_id_list:
            task = asyncio.ensure_future(
                func(work_id, *func_args, context_vars=context_vars, **func_kwargs)
            )
            progress_queue.put_nowait((context_id, "task_scheduled", work_id))
            task_map[task] = work_id

        while len(task_map) > 0:
            done_task_set, _ = await asyncio.wait(
                task_map.keys(), return_when=asyncio.FIRST_COMPLETED
            )

            for task in done_task_set:
                work_id = task_map[task]
                msg = get_done_task_result(task, work_id)
                progress_queue.put_nowait(msg)
                del task_map[task]
                if msg[1] == "task_raised" and isinstance(msg[3], KeyboardInterrupt):
                    keyboard_interrupted = True

    else:
        cur_pos = 0
        cur_task_map = {}

        while cur_pos < len(work_id_list) or len(cur_task_map) > 0:
            # add tasks to run concurrently
            spare = min(
                max_concurrency - len(cur_task_map), len(work_id_list) - cur_pos
            )
            if spare > 0:
                for i in range(spare):
                    work_id = work_id_list[cur_pos + i]
                    if keyboard_interrupted:
                        progress_queue.put_nowait(
                            (context_id, "task_cancelled", work_id)
                        )
                    else:
                        task = asyncio.ensure_future(
                            func(
                                work_id,
                                *func_args,
                                context_vars=context_vars,
                                **func_kwargs,
                            )
                        )
                        progress_queue.put_nowait(
                            (context_id, "task_scheduled", work_id)
                        )
                        cur_task_map[task] = work_id
                cur_pos += spare

            # get some tasks done
            done_task_set, _ = await asyncio.wait(
                cur_task_map.keys(), return_when=asyncio.FIRST_COMPLETED
            )
            for task in done_task_set:
                work_id = cur_task_map[task]
                msg = get_done_task_result(task, work_id)
                progress_queue.put_nowait(msg)
                del cur_task_map[task]
                if msg[1] == "task_raised" and isinstance(msg[3], KeyboardInterrupt):
                    keyboard_interrupted = True


async def asyn_work_generator(
    func,
    func_args: tuple = (),
    func_kwargs: dict = {},
    num_processes=None,
    num_works: int = 0,
    max_concurrency: int = 1024,
    profile=None,
    debug_logger=None,
    progress_queue=None,
    timeout: int = 300,
):
    """An asyn generator that does a large number of works concurrently and yields the work results.

    Internally, it splits the list of work ids into blocks and invokes
    :func:`run_asyn_works_in_context` to run each block in a new context in a separate process. It
    uses a multiprocessing queue to facilitate the communications between the processes. Every time
    a task is done, either returning a result or raising an exception, it yields that information.
    Work ids are assigned to the processes randomly (see :func:`mt.concurrency.split_works`), and
    yielded messages come in completion order.

    Each subprocess runs inside an asynchronous context created by
    :func:`mt.base.s3.create_context_vars`, so S3 access is required by the current
    implementation. It is therefore not demonstrated with an offline example here.

    Parameters
    ----------
    func : function
        an asyn function that may return something and may raise an Exception. The function must
        have the first argument being the work id, and accept the keyword argument 'context_vars'.
        The context variables provided to the function are automatically created via invoking
        :func:`mt.base.s3.create_context_vars`. It must be picklable or inherited by fork.
    func_args : tuple
        additional positional arguments to be passed to the function as-is
    func_kwargs : dict
        additional keyword arguments to be passed to the function as-is
    num_processes : int
        the number of processes to be created. If not specified, it is equal to the number of CPUs.
    num_works : int
        number of works
    max_concurrency : int
        the maximum number of concurrent works in each context at any time, good for managing
        memory allocations. If None is given, all works in each context will be scheduled to run at
        once.
    profile : str, optional
        one of the profiles specified in the AWS. The default is used if None is given.
    debug_logger : logging.Logger or equivalent
        logger for debugging purposes, if needed
    progress_queue: multiprocessing.Queue
        a shared queue so that the main process can observe the progress inside the context. If not
        provided, one will be created internally.
    timeout: int, optional
        timeout, in `(0.1*second)` unit. If no message arrives from any process for this long
        (300 units, i.e. 30 seconds, by default), the remaining processes are terminated and the
        generator ends silently.

    Returns
    -------
    object
        an asynchronous generator. Each yielded item is a tuple `(message_code, work_id, ...)`
        with message code 'task_returned' (extra item: the returned value), 'task_raised' (extra
        items: the exception and the call stack as text) or 'task_cancelled'. It yields nothing if
        `num_works` is not positive.

    Raises
    ------
    KeyboardInterrupt
        at the end, if a keyboard interruption was caught while waiting

    Notes
    -----
    The context ids are zero-based. The number of contexts is equal to the number of processes.
    The work ids are task ids and are zero-based. The messages related to when a task is done are
    yielded (without the context id).

    Asyncio and KeyboardInterrupt are not happy with each other.
    https://bugs.python.org/issue42683
    """

    if num_works <= 0:
        return

    # for communicating between processes
    queue = mp.Queue() if progress_queue is None else progress_queue

    num_buckets = mp.cpu_count() if num_processes is None else num_processes
    work_id_list_list = split_works(num_works, num_buckets)

    def worker_process(
        progress_queue: mp.Queue,
        func,
        func_args: tuple = (),
        func_kwargs: dict = {},
        context_id=None,
        work_id_list: list = [],
        max_concurrency: int = 1024,
        profile=None,
    ):
        import asyncio
        from mt.base import s3

        async def asyn_func():
            async with s3.create_context_vars(
                asyn=True, profile=profile
            ) as context_vars:
                content = await run_asyn_works_in_context(
                    progress_queue,
                    func,
                    func_args=func_args,
                    func_kwargs=func_kwargs,
                    context_id=context_id,
                    work_id_list=work_id_list,
                    max_concurrency=max_concurrency,
                    context_vars=context_vars,
                )
            return content

        progress_queue.put_nowait((context_id, "context_created"))
        try:
            asyncio.run(asyn_func())
        except KeyboardInterrupt as e:  # asyncio sucks
            from mt.traceback import extract_stack_compact

            progress_queue.put_nowait(
                (context_id, "context_raised", e, extract_stack_compact())
            )
        except Exception as e:
            from mt.traceback import extract_stack_compact
            from mt.logg import logger

            logger.warn_last_exception()
            logger.warn(
                "Uncaught exception from a subprocess of mt.base.asyn_work_generator."
            )
            progress_queue.put_nowait(
                (context_id, "context_raised", e, extract_stack_compact())
            )
        progress_queue.put_nowait((context_id, "context_destroyed"))

    # launch the concurrency suite
    process_list = []
    for context_id in range(num_buckets):
        p = mp.Process(
            target=worker_process,
            args=(queue, func),
            kwargs={
                "func_args": func_args,
                "func_kwargs": func_kwargs,
                "context_id": context_id,
                "work_id_list": work_id_list_list[context_id],
                "max_concurrency": max_concurrency,
                "profile": profile,
            },
            daemon=True,
        )
        p.start()
        process_list.append(p)

    # wait for every task to be done
    num_running_buckets = num_buckets
    wait_cnt = 0
    keyboard_interrupted = False
    num_works_done = 0
    while (num_running_buckets > 0) and wait_cnt < timeout:
        try:
            msg = queue.get_nowait()
            wait_cnt = 0
            # if debug_logger:
            # debug_logger.debug(f"asyn_work_generator: {msg}")
            if msg[1] == "context_destroyed":
                num_running_buckets -= 1
            elif msg[1] in ("task_returned", "task_cancelled", "task_raised"):
                num_works_done += 1
                # if num_works_done % 1000 == 0:
                # if debug_logger:
                # debug_logger.debug(
                #     f"asyn_work_generator: {num_works_done}/{num_works} works done"
                # )
                yield msg[1:]
        except _q.Empty:
            try:
                await asyncio.sleep(0.1)
            except KeyboardInterrupt:
                if debug_logger:
                    debug_logger.warn(
                        "Keyboard interrupted. Will reraise when the time is right."
                    )
                keyboard_interrupted = True
            wait_cnt += 1

    # clean up
    if wait_cnt < timeout:  # healthy
        for p in process_list:
            p.join()
    else:  # something wrong
        if debug_logger:
            debug_logger.debug(
                f"asyn_work_generator: {0.1 * timeout:.1f}s timeout reached."
            )
        for p in process_list:
            p.terminate()  # ouch!

    if keyboard_interrupted:
        raise KeyboardInterrupt(
            "Keyboard interrupted while asyn_work_generator() is running."
        )


def batched(iterable, n):
    """Batches data into tuples of length n. The last batch may be shorter.

    Parameters
    ----------
    iterable : iterable
        the input iterable
    n : int
        the batch size. It must be at least 1.

    Returns
    -------
    object
        a generator of tuples

    Raises
    ------
    ValueError
        if `n` is less than 1 (raised when the generator is first advanced)

    See Also
    --------
    unbatched : an operation that flattens the batches back, for processed batches

    Examples
    --------
    >>> from mt.concurrency.aio import batched
    >>> list(batched("ABCDEFG", 3))
    [('A', 'B', 'C'), ('D', 'E', 'F'), ('G',)]
    """
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(itertools.islice(it, n)):
        yield batch


def unbatched(iterable):
    """Flattens an iterable of `(ok, batch)` pairs, as produced by the pool functions, into items.

    Parameters
    ----------
    iterable : iterable
        iterable of pairs `(ok, x)`. If `ok` is True, `x` is a list of items to be yielded one by
        one. Otherwise `x` is an exception to be raised.

    Returns
    -------
    object
        a generator of items

    Raises
    ------
    Exception
        the exception `x` of the first pair that has a False `ok`

    Examples
    --------
    >>> from mt.concurrency.aio import unbatched
    >>> list(unbatched([(True, [1, 2]), (True, [3])]))
    [1, 2, 3]
    >>> list(unbatched([(True, [1]), (False, ValueError("bad"))]))
    Traceback (most recent call last):
        ...
    ValueError: bad
    """
    for ok, x in iterable:
        if not ok:
            raise x
        for y in x:
            yield y


def pool_initializer(
    pool_func: tp.Callable,
    init_func: tp.Optional[tp.Callable] = None,
    init_func_args: tuple = (),
    init_func_kwargs: dict = {},
):
    """Initialiser of a worker process of :func:`asyn_pmap`: invokes `init_func`, if any.

    Parameters
    ----------
    pool_func : callable
        the function that the pool will run. Unused.
    init_func : callable, optional
        a function returning None, invoked with the arguments below
    init_func_args : tuple
        positional arguments for `init_func`
    init_func_kwargs : dict
        keyword arguments for `init_func`
    """
    if init_func is not None:
        init_func(*init_func_args, **init_func_kwargs)


def sequential_pool_func(
    t_items: tuple,
    asyn_func: tp.Optional[tp.Callable] = None,
    asyn_func_args: tuple = (),
    asyn_func_kwargs: dict = {},
    cvc_func: tp.Optional[tp.Callable] = None,
    cvc_func_args: tuple = (),
    cvc_func_kwargs: dict = {},
):
    """Processes a block of items one after another in a new event loop. Used by :func:`asyn_pmap`.

    Inside a single asynchronous context, `asyn_func` is awaited for each item in order.

    Parameters
    ----------
    t_items : tuple
        the block of items
    asyn_func : function
        an asyn function called as `asyn_func(item, *asyn_func_args, context_vars=context_vars,
        **asyn_func_kwargs)`
    asyn_func_args : tuple
        additional positional arguments for `asyn_func`
    asyn_func_kwargs : dict
        additional keyword arguments for `asyn_func`
    cvc_func : function, optional
        a function returning an asynchronous context manager whose enter-result is the
        `context_vars` dictionary. If None, `context_vars={"async": True}`.
    cvc_func_args : tuple
        positional arguments for `cvc_func`
    cvc_func_kwargs : dict
        keyword arguments for `cvc_func`

    Returns
    -------
    ok : bool
        True if all items were processed, False if an exception was raised
    outputs : list or Exception
        if `ok`, the list of outputs in the order of the items; otherwise the exception raised

    Examples
    --------
    >>> from mt.concurrency.aio import sequential_pool_func
    >>> async def double(x, offset=0, context_vars={}):
    ...     return 2 * x + offset
    >>> sequential_pool_func((1, 2, 3), asyn_func=double)
    (True, [2, 4, 6])
    >>> sequential_pool_func((1, 2), asyn_func=double, asyn_func_kwargs={"offset": 1})
    (True, [3, 5])
    >>> async def fail(x, context_vars={}):
    ...     raise KeyError(x)
    >>> sequential_pool_func((1, 2), asyn_func=fail)
    (False, KeyError(1))
    """
    import asyncio

    async def func(t_items: tuple):
        if cvc_func is None:
            ctx = nullcontext({"async": True})
        else:
            ctx = cvc_func(*cvc_func_args, **cvc_func_kwargs)
        async with ctx as context_vars:
            l_outputs = []
            for item in t_items:
                output = await asyn_func(
                    item,
                    *asyn_func_args,
                    context_vars=context_vars,
                    **asyn_func_kwargs,
                )
                l_outputs.append(output)

            return l_outputs

    try:
        return True, asyncio.run(func(t_items))
    except Exception as e:
        return False, e


def concurrent_pool_func(
    t_items: tuple,
    asyn_func: tp.Optional[tp.Callable] = None,
    asyn_func_args: tuple = (),
    asyn_func_kwargs: dict = {},
    cvc_func: tp.Optional[tp.Callable] = None,
    cvc_func_args: tuple = (),
    cvc_func_kwargs: dict = {},
    max_concurency: int = 1,
):
    """Processes a block of items concurrently in a new event loop. Used by :func:`asyn_pmap`.

    Warning: as currently implemented this function does not work. Its keyword argument is spelled
    `max_concurency` while :func:`asyn_pmap` passes `max_concurrency`, which raises a
    :class:`TypeError`, and its body refers to an undefined name and misuses
    :func:`asyncio.wait_for`, so any call that gets that far returns `(False, <exception>)`. Use
    :func:`sequential_pool_func` (that is, `max_concurrency_for_block_items=1`) instead.

    Parameters
    ----------
    t_items : tuple
        the block of items
    asyn_func : function
        an asyn function called as `asyn_func(item, *asyn_func_args, context_vars=context_vars,
        **asyn_func_kwargs)`
    asyn_func_args : tuple
        additional positional arguments for `asyn_func`
    asyn_func_kwargs : dict
        additional keyword arguments for `asyn_func`
    cvc_func : function, optional
        a function returning an asynchronous context manager whose enter-result is the
        `context_vars` dictionary. If None, `context_vars={"async": True}`.
    cvc_func_args : tuple
        positional arguments for `cvc_func`
    cvc_func_kwargs : dict
        keyword arguments for `cvc_func`
    max_concurency : int
        intended maximum number of concurrent items

    Returns
    -------
    ok : bool
        intended to be True if all items were processed, False if an exception was raised
    outputs : list or Exception
        intended to be the list of outputs in the order of the items, or the exception raised
    """
    import asyncio

    async def func(t_items: tuple):
        if cvc_func is None:
            ctx = nullcontext({"async": True})
        else:
            ctx = cvc_func(*cvc_func_args, **cvc_func_kwargs)
        async with ctx as context_vars:
            i = 0
            N = len(t_items)
            l_outputs = [None] * N
            d_tasks = {}

            while i < N:
                # push
                while i < N and len(s_tasks) < max_concurency:
                    coro = asyn_func(
                        t_items[i],
                        *asyn_func_args,
                        context_vars=context_vars,
                        **asyn_func_kwargs,
                    )
                    task = asyncio.create_task(coro)
                    d_tasks[task] = i
                    i += 1

                # pop
                if len(d_tasks) > 0:
                    # await for maximum 10 minutes for at least 1 task to finish
                    s_done, _ = await asyncio.wait_for(
                        d_tasks.keys(),
                        timeout=600,
                        return_when=asyncio.FIRST_COMPLETED,
                    )

                    if len(s_done) == 0:
                        raise TimeoutError("No task has been done for 10 minutes.")

                    for task in s_done:
                        l_outputs[d_tasks.pop(task)] = task.result()

            return l_outputs

    try:
        return True, asyncio.run(func(t_items))
    except Exception as e:
        return False, e


def asyn_pmap(
    asyn_func: tp.Callable,
    input_iterable: tp.Iterable,
    block_size: int = 1024,
    max_concurrency_for_block_items: int = 1,
    batch_size: tp.Optional[int] = None,
    asyn_func_args: tuple = (),
    asyn_func_kwargs: dict = {},
    pool_processes: tp.Optional[int] = None,
    pool_maxtasksperchild: tp.Optional[int] = None,
    pool_ordered: bool = True,
    init_func: tp.Optional[tp.Callable] = None,
    init_func_args: tuple = (),
    init_func_kwargs: dict = {},
    cvc_func: tp.Optional[tp.Callable] = None,
    cvc_func_args: tuple = (),
    cvc_func_kwargs: dict = {},
) -> tp.Iterable:
    """pmap over an iterator with an asyn function.

    Internally, the iterator is batched into a block iterator. Each block is sent to one worker for
    processing. The worker goes through every item of a block and invokes asyn function `asyn_func`
    providing the `context_vars` dictionary. Each block of results is then unbatched. The output
    iterator yield resultant items, which may or may not in order depending on the `pool_ordered`
    argument.

    The function returns immediately with a lazy iterator, backed by a :class:`multiprocessing.Pool`
    that is created at call time and never explicitly closed. If processing a block raises an
    exception, it is re-raised in the caller when the iteration reaches that block. Use a
    `if __name__ == "__main__":` guard in scripts, and define `asyn_func` at module level so that
    it can be pickled.

    Parameters
    ----------
    asyn_func : function
        an asyn, picklable function that returns something. The first positional argument of
        function is the item to be processed. The keyword argument 'context_vars' is provided to
        the function.
    input_iterable : iterable
        any iterable object to act as the input iterator
    block_size : int
        number of items in each block. It should be chosen by the user to balance between the
        benefit of iterating over the items of a block in async mode and the cost of allocated
        memory to store pending transformed items of the block
    max_concurrency_for_block_items : int, optional
        the maximum number of concurrent items in a block to be processed by any worker. If the
        value is greater than 1, :func:`concurrent_pool_func` is used (but it currently does not
        work, see its docstring). Otherwise, the items of a block are awaited one after another
        by :func:`sequential_pool_func`.
    batch_size : int, optional
        same as block_size. For backward compatibility only. Takes priority over block_size due to
        backward compatibility. New invocations should not use this argument.
    asyn_func_args : tuple, optional
        additional positional arguments to be passed to the asyn function as-is
    asyn_func_kwargs : dict, optional
        additional keyword arguments to be passed to the asyn function as-is
    pool_processes : int, optional
        the number of processes to be created. If not specified, it is equal to the number of CPUs.
        Passed as-is to :class:`multiprocessing.Pool`.
    pool_maxtasksperchild : int, optional
        the number of tasks a worker process can complete before it will exit and be replaced with
        a fresh worker process, to enable unused resources to be freed. The default
        `maxtasksperchild` is None, which means worker processes will live as long as the pool.
        Passed as-is to :class:`multiprocessing.Pool`.
    pool_ordered : bool
        whether the output iterator provides the same order as the input iterator or not
    init_func : function, optional
        a function returning None that is invoked after a worker process is created
    init_func_args : tuple, optional
        additional positional arguments to be passed to the init function as-is
    init_func_kwargs : dict, optional
        additional keyword arguments to be passed to the init function as-is
    cvc_func : function, optional
        a function returning the `context_vars` dictionary to be provided to the asyn function as a
        keyword argument. The context is consistent throughout the lifecycle of one block. If not
        provided, `context_vars={"async": True}`.
    cvc_func_args : tuple, optional
        additional positional arguments to be passed to the cvc function as-is
    cvc_func_kwargs : dict, optional
        additional keyword arguments to be passed to the cvc function as-is

    Returns
    -------
    output_iterable : iterable
        the output iterator, yielding one output per input item

    See Also
    --------
    sequential_pool_func : the function that processes each block

    Examples
    --------
    .. code-block:: python

        from mt.concurrency import asyn_pmap

        async def double(x, context_vars={}):
            return 2 * x

        if __name__ == "__main__":
            print(list(asyn_pmap(double, range(5), block_size=2, pool_processes=2)))
            # [0, 2, 4, 6, 8]
    """

    if batch_size is not None:
        block_size = batch_size

    iterable = batched(input_iterable, block_size)

    if max_concurrency_for_block_items > 1:
        pool_func = functools.partial(
            concurrent_pool_func,
            asyn_func=asyn_func,
            asyn_func_args=asyn_func_args,
            asyn_func_kwargs=asyn_func_kwargs,
            cvc_func=cvc_func,
            cvc_func_args=cvc_func_args,
            cvc_func_kwargs=cvc_func_kwargs,
            max_concurrency=max_concurrency_for_block_items,
        )
    else:
        pool_func = functools.partial(
            sequential_pool_func,
            asyn_func=asyn_func,
            asyn_func_args=asyn_func_args,
            asyn_func_kwargs=asyn_func_kwargs,
            cvc_func=cvc_func,
            cvc_func_args=cvc_func_args,
            cvc_func_kwargs=cvc_func_kwargs,
        )

    initargs = (
        pool_func,
        init_func,
        init_func_args,
        init_func_kwargs,
    )
    pool = mp.Pool(
        processes=pool_processes,
        initializer=pool_initializer,
        initargs=initargs,
        maxtasksperchild=pool_maxtasksperchild,
    )

    if pool_ordered:
        iterable = pool.imap(pool_func, iterable)
    else:
        iterable = pool.imap_unordered(pool_func, iterable)

    output_iterable = unbatched(iterable)

    return output_iterable
