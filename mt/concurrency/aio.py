"""Concurrency using asyncio and multiprocessing."""

import typing as tp
import asyncio
import multiprocessing as mp
import queue as _q
import itertools

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
    """Invokes the same asyn function with different work ids concurrently and asynchronously, in a given context.

    Parameters
    ----------
    progress_queue: multiprocessing.Queue
        a shared queue so that the main process can observe the progress inside the context. See
        notes below.
    func : function
        an asyn function that may return something and may raise an Exception. The function must
        have the first argument being the work id. The context variables provided to the function
        are automatically created via invoking :func:`mt.base.s3.create_context_vars`.
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
    asyn : bool
        whether the asyn function is to be invoked asynchronously or synchronously
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`. In asynchronous mode, variable
        'http_session' must exist and hold an enter-result of an async with statement invoking
        :func:`mt.base.http.create_http_session`. You can use
        :func:`mt.base.s3.create_context_vars` to create a dictionary like this.

    Notes
    -----
    This function returns nothing but while it is running, the progress queue regularly receives
    messages. Each message is a tuple `(context_id, message_code, work_id, ...)`. The user should
    process the progress queue.

    If KeyboardInterrupt is raised during the invocation, all remaining unscheduled tasks are
    cancelled. This means for the case that max_concurrency is None, KeyboardInterrupt has no
    effect.
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

    Parameters
    ----------
    func : function
        an asyn function that may return something and may raise an Exception. The function must
        have the first argument being the work id. The context variables provided to the function
        are automatically created via invoking :func:`mt.base.s3.create_context_vars`.
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
        timeout, in `(0.1*second)` unit

    Notes
    -----
    The context ids are zero-based. The number of contexts is equal to the number of processes.
    The work ids are task ids and are zero-based. The messages related to when a task is done are
    yielded.

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
            # debug_logger.debug("asyn_work_generator: {}".format(msg))
            if msg[1] == "context_destroyed":
                num_running_buckets -= 1
            elif msg[1] in ("task_returned", "task_cancelled", "task_raised"):
                num_works_done += 1
                # if num_works_done % 1000 == 0:
                # if debug_logger:
                # debug_logger.debug("asyn_work_generator: {}/{} works done".format(num_works_done, num_works))
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
                "asyn_work_generator: {:.1f}s timeout reached.".format(0.1 * timeout)
            )
        for p in process_list:
            p.terminate()  # ouch!

    if keyboard_interrupted:
        raise KeyboardInterrupt(
            "Keyboard interrupted while asyn_work_generator() is running."
        )


def batched(iterable, n):
    "Batch data into tuples of length n. The last batch may be shorter."
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    it = iter(iterable)
    while batch := tuple(itertools.islice(it, n)):
        yield batch


def unbatched(iterable):
    "Unbatch tuples of data into just data."
    for x in iterable:
        for y in x:
            yield y


def pool_initializer(
    pool_func: tp.Callable,
    asyn_func: tp.Callable,
    asyn_func_args: tuple = (),
    asyn_func_kwargs: dict = {},
    init_func: tp.Optional[tp.Callable] = None,
    init_func_args: tuple = (),
    init_func_kwargs: dict = {},
    cvc_func: tp.Optional[tp.Callable] = None,
    cvc_func_args: tuple = (),
    cvc_func_kwargs: dict = {},
):
    pool_func.asyn_func = asyn_func
    pool_func.asyn_func_args = asyn_func_args
    pool_func.asyn_func_kwargs = asyn_func_kwargs

    pool_func.cvc_func = cvc_func
    pool_func.cvc_func_args = cvc_func_args
    pool_func.cvc_func_kwargs = cvc_func_kwargs

    if init_func is not None:
        init_func(*init_func_args, **init_func_kwargs)


def pool_func(
    t_items: tuple,
):
    import asyncio

    async def func(t_items: tuple):
        if pool_func.cvc_func is None:
            ctx = nullcontext({"async": True})
        else:
            ctx = pool_func.cvc_func(
                *pool_func.cvc_func_args, **pool_func.cvc_func_kwargs
            )
        async with ctx as context_vars:
            l_outputs = []
            for item in t_items:
                output = await pool_func.asyn_func(
                    item,
                    *pool_func.asyn_func_args,
                    context_vars=context_vars,
                    **pool_func.asyn_func_kwargs,
                )
                l_outputs.append(output)

            return l_outputs

    return asyncio.run(func(t_items))


def asyn_pmap(
    asyn_func: tp.Callable,
    input_iterable: tp.Iterable,
    batch_size: int = 1024,
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

    Internally, the iterator is batched into a batch iterator. Each batch is sent to one worker for
    processing. The worker goes through every item of a batch and invokes asyn function `asyn_func`
    providing the `context_vars` dictionary. Each batch of results is then unbatched. The output
    iterator yield resultant items, which may or may not in order depending on the `pool_ordered`
    argument.

    Parameters
    ----------
    asyn_func : function
        an asyn function that returns something. The first positional argument of function is the
        item to be processed. The keyword argument 'context_vars' is provided to the function.
    input_iterable : iterable
        any iterable object to act as the input iterator
    batch_size : int
        number of batch items in each batch. It should be chosen by the user to balance between the
        benefit of iterating over the items of a batch in async mode and the cost of allocated
        memory to store pending transformed items of the batch
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
        the output iterator
    """

    iterable = batched(input_iterable, batch_size)

    initargs = (
        pool_func,
        asyn_func,
        asyn_func_args,
        asyn_func_kwargs,
        init_func,
        init_func_args,
        init_func_kwargs,
        cvc_func,
        cvc_func_args,
        cvc_func_kwargs,
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
