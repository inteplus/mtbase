'''Concurrency in dask way. Needed for streaming workloads.'''

import os
import asyncio
from time import sleep
import multiprocessing as _mp
import threading as _t
import queue as _q
import numpy as np
import psutil

from .bg_invoke import BgInvoke, BgThread, BgException


__all__ = ['Counter', 'used_memory_too_much', 'used_cpu_too_much', 'split_works', 'ProcessParalleliser', 'WorkIterator', 'serial_work_generator',
           'aio_work_generator', 'run_asyn_works_in_context', 'asyn_work_generator']


class Counter(object):

    '''Counter class without the race-condition bug'''

    def __init__(self):
        self.val = _mp.Value('i', 0)

    def increment(self, n=1):
        with self.val.get_lock():
            self.val.value += n

    @property
    def value(self):
        return self.val.value


# ----------------------------------------------------------------------


def used_memory_too_much():
    mem = psutil.virtual_memory()
    return mem.percent > 90 # 90% usage is the limit


def used_cpu_too_much():
    return psutil.cpu_percent() > 90 # 90% usage is the limit


def split_works(num_works, num_buckets = None):
    '''Splits a number of works randomly into a few buckets, returning the work id per bucket.

    Parameters
    ----------
    num_works : int
        number of works

    num_buckets : int, optional
        number of buckets. If not specified, the number of cpus will be used.

    Returns
    -------
    work_id_list_list : list
        a nested list of work id lists. The work ids, in interval [0, num_works), are split
        approximately evenly and randomly into the buckets
    '''

    N = num_works
    K = _mp.cpu_count() if num_buckets is None else num_buckets

    rng = np.random.default_rng()
    vals = np.floor(rng.uniform(0, K, size=N)).astype(int)
    retval = [[] for k in range(K)]
    for n in range(N): # slow but fine
        k = vals[n]
        retval[k].append(n)

    return retval


# ----------------------------------------------------------------------


INTERVAL = 0.5 # 0.5 seconds interval
HEARTBEAT_PERIOD = 5 # send a heart beat every 5 intervals
MAX_MISS_CNT = 240 # number of times before we declare that the other process has died


def worker_process(func, heartbeat_pipe, queue_in, queue_out, logger=None):

    '''
    The worker process.

    The worker process operates in the following way. There is a pipe, named `heartbeat_pipe` to
    communicate between the worker and its parent process about their heartbeats. Each side of the
    pipe constantly sends a heartbeat in every {} seconds. If one side does not hear any heartbeat
    within {} seconds, then it can assume the other side has died for some unexpected reason. After
    detecting this fateful event, the parent process can abruptly kill the worker process, but in
    contrast, the worker can only finish its own fate.

    A heart beat from the parent process is a byte. If the heartbeat is 0, the parent is alive. If
    it is 1, the worker process must do a seppuku.

    A heart beat from the worker process is also a byte. If it is 0, the worker is alive. If it is
    1, the worker signals that it has received keyboard interruption and is about to die in peace.

    There is global pair of queues, `queue_in` and `queue_out`, to distribute workloads. Workloads
    are divided into smaller pieces identified by zero-based work ids. The parent process sends work
    ids to `queue_in`. Any worker process can extract a work id from the queue, do the work, and
    return the outcome to `queue_out`. The data in `queue_in` are pure integers. The data in
    `queue_out` are tuples of the form `(work_id, ...)`.

    The parent process owns all the pipes and queues.

    Parameters
    ----------
    func : function
        a function taking work_id as the only argument and returning something. The function is run
        in a background thread of the worker process
    heartbeat_pipe : multiprocessing.Connection
        the child connection of a parent-child pipe to communicate with the parent about their
        heartbeats
    queue_in : multiprocessing.Queue
        queue containing work ids
    queue_out : multiprocessing.Queue
        queue containing results `(work_id, ...)`
    logger : IndentedLoggerAdapter
        logger for debugging purposes
    '''.format(INTERVAL, MAX_MISS_CNT*INTERVAL)

    to_die = False
    miss_cnt = 0
    hb_cnt = 0

    bg_thread = BgThread(func)
    has_work = False
    work_id = -1
    result_list = []

    while True:

        if to_die: # die as soon as we have the opportunity
            if (not has_work) and (len(result_list) == 0):
                #print("  dying gracefully!", work_id)
                break

        # check the background thread
        if has_work and (not bg_thread.is_running()):
            try:
                result_list.append((work_id, bg_thread.result))
            except BgException: # premature death
                if logger:
                    logger.warn_last_exception()
                    logger.warn("Uncaught exception killed worker pid {}.".format(os.getpid()))
                to_die = True
            has_work = False

        # attempt to put some result to queue_out
        while result_list:
            #print("result_list",result_list)
            try:
                queue_out.put_nowait(result_list[-1])
                result_list.pop()
            except _q.Full:
                break

        # get a work id
        if (not to_die) and (not has_work): # and not used_memory_too_much():
            work_id = -1
            try:
                work_id = queue_in.get_nowait()
            except _q.Empty:
                pass

            if work_id >= 0:
                #print("work_id", work_id)
                has_work = True
                bg_thread.invoke(work_id) # do the work in background

        # heartbeats
        hb_cnt += 1
        if hb_cnt >= HEARTBEAT_PERIOD:
            hb_cnt = 0
            heartbeat_pipe.send_bytes(bytes((0,)))
        if heartbeat_pipe.poll():
            miss_cnt = 0
            buf = heartbeat_pipe.recv_bytes(16384)
            for x in buf:
                if x == 1: # request from parent to die
                    to_die = True
        elif not to_die:
            miss_cnt += 1
            if miss_cnt >= MAX_MISS_CNT: # seppuku
                if logger:
                    logger.warn("Lack of parent's heartbeat killed worker pid {}.".format(os.getpid()))
                to_die = True

        # sleep until next time
        try:
            sleep(INTERVAL)
        except KeyboardInterrupt:
            # the parent process may have died by now, but we want to notify anyway
            heartbeat_pipe.send_bytes(bytes((1,)))
            sleep(INTERVAL)
            to_die = True

    bg_thread.close()
    queue_out.cancel_join_thread()
    queue_in.cancel_join_thread()
    heartbeat_pipe.close()


class ProcessParalleliser(object):

    '''Run a function with different inputs in parallel using multiprocessing.

    Parameters
    ----------
    func : function
        a function to be run in parallel. The function takes as input a non-negative integer
        'work_id' and returns some result.
    max_num_workers : int, optional
        maximum number of concurrent workers or equivalently processes to be allocated
    logger : IndentedLoggerAdapter, optional
        for logging messages
    '''

    def __init__(self, func, max_num_workers=None, logger=None):
        if max_num_workers is None:
            max_num_workers = _mp.cpu_count()

        self.func = func
        self.logger = logger
        self.queue_in = _mp.Queue()
        self.queue_out = _mp.Queue()
        self.num_workers = max_num_workers
        self.miss_cnt_list = [0]*self.num_workers
        self.pipe_list = []
        self.process_list = []
        for i in range(self.num_workers):
            if used_memory_too_much():
                break
            pipe = _mp.Pipe()
            self.pipe_list.append(pipe[0])
            p = _mp.Process(
                    target=worker_process,
                    args=(func, pipe[1], self.queue_in, self.queue_out),
                    kwargs={'logger': logger})
            p.start() # start the process
            self.process_list.append(p)
        self.num_workers = len(self.process_list)

        # launch a background thread to communicate constantly with the worker processes
        self.state = 'living'
        self.bg_thread = BgInvoke(self._process)


    def _process(self):

        '''A background process to communicate with the worker processes.'''

        death_code = 'normal'
        hb_cnt = 0
        while True:
            try:
                all_dead = True
                for i, p in enumerate(self.process_list):
                    try:
                        if not p.is_alive():
                            self.state = 'dying'
                            continue

                        # heartbeats
                        pipe = self.pipe_list[i]
                        val = int(self.state != 'living')
                        if hb_cnt == 0:
                            pipe.send_bytes(bytes((val,)))
                        if pipe.poll():
                            all_dead = False
                            self.miss_cnt_list[i] = 0
                            buf = pipe.recv_bytes(16384) # cleanse the pipe
                            #self.logger.debug("Pipe from {}: {}".format(p.pid, buf))
                            for x in buf:
                                if x == 1:
                                    if self.logger:
                                        self.logger.debug("Worker {} wanted parent to die.".format(p.pid))
                                    if death_code == 'normal':
                                        death_code = 'keyboard_interrupted'
                                    self.state = 'dying'
                                    break
                        else:
                            self.miss_cnt_list[i] += 1
                            if self.miss_cnt_list[i] >= MAX_MISS_CNT: # no heartbeat for too long
                                if death_code == 'normal':
                                    death_code = 'worker_died_prematurely'
                                self.state = 'dying'
                                if p.is_alive():
                                    pipe.send_bytes(bytes((1,)))
                                self.miss_cnt_list[i] = 0
                            else: # assume the current worker still lives
                                all_dead = False
                    except (EOFError, BrokenPipeError, ConnectionResetError):
                        if death_code == 'normal':
                            death_code = 'broken_pipe_or_something'
                        self.state = 'dying'
                    except: # broken pipe or something, assume worker process is dead
                        if self.logger:
                            self.logger.warn_last_exception()
                        if death_code == 'normal':
                            death_code = 'uncaught_exception'
                        self.state = 'dying'

                #self.logger.debug("  -- main process", all_dead)
                if all_dead:
                    break

                hb_cnt += 1
                if hb_cnt >= HEARTBEAT_PERIOD:
                    hb_cnt = 0

                # sleep until next time
                try:
                    sleep(INTERVAL)
                except KeyboardInterrupt:
                    if logger:
                        logger.warn("Process {} interrupted by keyboard.".format(os.getpid()))
                    if death_code == 'normal':
                        death_code = 'keyboard_interrupted'
                    self.state = 'dying'
            except:
                if logger:
                    logger.warn_last_exception()
                    logger.warn("Uncaught exception above killed process.".format(os.getpid()))
                break

        if death_code != 'normal':
            if logger:
                logger.warn("Process {} died with reason: {}.".format(os.getpid(), death_code))

        #self.logger.debug("  -- main process closing")
        self._close()


    def __del__(self):
        self.close()

        
    def _close(self):
        '''Closes the instance when all worker processes have died.'''

        if self.state == 'dead':
            return

        for p in self.process_list:
            p.join()
            
        self.state = 'dead'

    def close(self):
        '''Closes the instance properly.'''
        if self.state == 'living':
            self.state = 'dying'
        #while self.state != 'dead':
            #sleep(INTERVAL)


    def push(self, work_id, timeout=30):
        '''Pushes a work id to the background to run the provided function in parallel.

        Parameters
        ----------
        work_id : int
            non-negative integer to be provided to the function
        timeout : float
            number of seconds to wait to push the id to the queue before bailing out

        Returns
        -------
        bool
            whether or not the work id has been pushed

        Notes
        -----
        You should use :func:`pop` or :func:`empty` to check for the output of each work.
        '''
        if self.state != 'living': # we can't accept more work when we're not living
            return False
        if not isinstance(work_id, int):
            raise ValueError("Work id is not an integer. Got {}.".format(work_id))
        if work_id < 0:
            raise ValueError("Work id is negative. Got {}.".format(work_id))
        try:
            self.queue_in.put(work_id, block=True, timeout=timeout)
            return True
        except _q.Full:
            return False

    def empty(self):
        '''Returns whether the output queue is empty.'''
        return self.queue_out.empty()

    def pop(self, timeout=60*60):
        '''Returns a pair (work_id, result) when at least one such pair is available.

        Parameters
        ----------
        timeout : float
            number of seconds to wait to pop a work result from the queue before bailing output
        
        Returns
        -------
        int
            non-negative integer representing the work id
        object
            work result

        Raises
        ------
        queue.Empty
            if there is no work result after the timeout
        '''
        if self.state == 'dead':
            raise RuntimeError("The process paralleliser has been closed. Please reinstantiate.")
        return self.queue_out.get(block=True, timeout=timeout)


# ----------------------------------------------------------------------


class WorkIterator(object):
    '''Iterates work from id 0 to infinity, returning the work result in each iteration.

    By default, the class invokes ProcessParalleliser to do a few works ahead of time. It switches
    to serial mode if requested.

    Parameters
    ----------
    func : function
        a function representing the work process. The function takes as input a non-negative
        integer 'work_id' and returns some result in the form of `(work_id, ...)` if successful
        else None.
    buffer_size : int, optional
        maximum number of work resultant items to be buffered ahead of time. If not specified,
        default to be twice the number of processes.
    push_timeout : float, optional
        timeout in second for each push to input queue. See :func:`ProcessParalleliser.push`.
    pop_timeout : float, optional
        timeout in second for each pop from output queue. See :func:`ProcessParalleliser.pop`.
    skip_null : bool, optional
        whether or not to skip the iteration that contains None as the work result.
    logger : IndentedLoggerAdapter, optional
        for logging messages
    max_num_workers : int, optional
        maximum number of concurrent workers or equivalently processes to be allocated
    serial_mode : bool
        whether or not to activate the serial mode. Useful when the number of works is small.
    logger : logging.Logger, optional
        logger for debugging purposes

    Notes
    -----
    Instances of the class qualify as a thread-safe Python iterator. Each iteration returns a
    (work_id, result) pair. To avoid a possible deadlock during garbage collection, it is
    recommended to explicitly invoke :func:`close` to clean up background processes.

    As of 2021/2/17, instances of WorkIterator can be used in a with statement. Upon exiting,
    :func:`close` is invoked.

    As of 2021/4/30, you can switch version of paralleliser.

    As of 2021/08/13, the class has been extended to do work in serial if the number of works is
    provided and is less than or equal a provided threshold.
    '''

    def __init__(self, func, buffer_size=None, skip_null: bool = True, push_timeout=30, pop_timeout=60*60, max_num_workers=None, serial_mode=False, logger=None):
        self.skip_null = skip_null
        self.send_counter = 0
        self.recv_counter = 0
        self.retr_counter = 0
        if serial_mode:
            self.serial_mode = True
            self.func = func
        else:
            self.serial_mode = False
            self.paralleliser = ProcessParalleliser(func, max_num_workers=max_num_workers, logger=logger)        
            self.push_timeout = push_timeout
            self.pop_timeout = pop_timeout
            self.push_timeout = push_timeout
            self.pop_timeout = pop_timeout
            self.buffer_size = len(self.paralleliser.process_list)*2 if buffer_size is None else buffer_size
            self.lock = _t.Lock()
            self.alive = True

    # ----- cleaning up resources -----

    def close(self):
        '''Closes the iterator for further use.'''
        if self.serial_mode:
            return
        with self.lock:
            if not self.alive:
                return

            self.paralleliser.close()
            self.alive = False

    def __del__(self):
        self.close()

    # ----- with-compatibility -----

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.close()

    # ----- next ------

    def __next__(self):
        if self.serial_mode:
            while True:
                result = self.func(self.send_counter)
                self.send_counter += 1
                self.recv_counter += 1
                if self.skip_null and result is None:
                    continue
                self.retr_counter += 1
                return result

        with self.lock:
            if not self.alive:
                raise RuntimeError("The instance has been closed. Please reinstantiate.")

            while True:
                if self.paralleliser.state == 'dead':
                    raise RuntimeError("Cannot get the next item because the internal paralleliser has been dead.")

                max_items = max(self.recv_counter + self.buffer_size - self.send_counter, 0)
                for i in range(max_items):
                    if not self.paralleliser.push(self.send_counter, timeout=self.push_timeout):
                        break # can't push, maybe timeout?
                    self.send_counter += 1

                if self.send_counter <= self.recv_counter:
                    raise RuntimeError("Unable to push some work to background processes.")

                work_id, result = self.paralleliser.pop(timeout=self.pop_timeout)
                self.recv_counter += 1

                if self.skip_null and result is None:
                    continue
                
                self.retr_counter += 1
                return result
                

    def next(self):
        return self.__next__()

    def __iter__(self):
        return self

def serial_work_generator(func, num_work_ids):
    '''A generator that serially does some works and yields the work results.

    This function complements the WorkIterator class to deal with cases when the number of works is
    small.

    Parameters
    ----------
    func : function
        a function representing the work process. The function takes as input a non-negative
        integer 'work_id' and returns some result in the form of `(work_id, ...)` if successful
        else None.
    num_work_ids : int
        number of works to iterate over without using multiprocessing or multithreading.

    Returns
    -------
    object
        a Python generator
    '''
    for work_id in range(num_work_ids):
        yield func(work_id)


async def aio_work_generator(func, num_work_ids, skip_null: bool = True, func_kwargs: dict = {}, max_concurrency: int = 1024):
    '''An asynchronous generator that does some works and yields the work results.

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
    func_kwargs : dict
        additional keyword arguments to be passed to the function as-is
    max_concurrency : int
        the maximum number of concurrent works at any time, good for managing memory allocations.
        If None is given, all works will be scheduled to run at once.

    Returns
    -------
    object
        an asynchronous generator yielding each result in the form `(work_id, ...)`
    '''

    coros = [func(work_id, **func_kwargs) for work_id in range(num_work_ids)]

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
                new_task_list = [asyncio.ensure_future(coros[pos+i]) for i in range(spare)]
                cur_task_list.extend(new_task_list)
                pos += spare

            # get some tasks done
            done_task_list, cur_task_list = await asyncio.wait(cur_task_list, return_when=asyncio.FIRST_COMPLETED)
            cur_task_list = list(cur_task_list)

            # yield the results
            for done_task in done_task_list:
                e = done_task.exception()
                if e is not None:
                    raise e
                result = done_task.result()
                if not skip_null or result is not None:
                    yield result


async def run_asyn_works_in_context(progress_queue: _mp.Queue, func, func_args: list = [], func_kwargs: dict = {}, context_id = None, work_id_list: list = [], max_concurrency: int = 1024, context_vars: dict = {}):
    '''Invokes the same asyn function with different work ids concurrently and asynchronously, in a given context.

    Parameters
    ----------
    progress_queue: multiprocessing.Queue
        a shared queue so that the main process can observe the progress inside the context. See
        notes below.
    func : function
        an asyn function that may return something and may raise an Exception. The function must
        have the first argument being the work id. The context variables provided to the function
        are automatically created via invoking :func:`mt.base.s3.create_context_vars`.
    func_args : list
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
    '''

    def get_done_task_result(task, work_id):
        if task.cancelled():
            return (context_id, 'task_cancelled', work_id)
        e = task.exception()
        if e is not None:
            import io
            tracestack = io.StringIO()
            task.print_stack(file=tracestack)
            return (context_id, 'task_raised', work_id, e, tracestack.getvalue())
        result = task.result()
        return (context_id, 'task_returned', work_id, result)

    import asyncio
    keyboard_interrupted = False

    if max_concurrency is None:
        task_map = {}
        for work_id in work_id_list:
            task = asyncio.ensure_future(func(work_id, *func_args, context_vars=context_vars, **func_kwargs))
            progress_queue.put_nowait((context_id, 'task_scheduled', work_id))
            task_map[task] = work_id

        while len(task_map) > 0:
            done_task_set, _ = await asyncio.wait(task_map.keys(), return_when=asyncio.FIRST_COMPLETED)

            for task in done_task_set:
                work_id = task_map[task]
                msg = get_done_task_result(task, work_id)
                progress_queue.put_nowait(msg)
                del task_map[task]
                if msg[1] == 'task_raised' and isinstance(msg[3], KeyboardInterrupt):
                    keyboard_interrupted = True

    else:
        cur_pos = 0
        cur_task_map = {}

        while cur_pos < len(work_id_list) or len(cur_task_map) > 0:
            # add tasks to run concurrently
            spare = min(max_concurrency - len(cur_task_map), len(work_id_list) - cur_pos)
            if spare > 0:
                for i in range(spare):
                    work_id = work_id_list[cur_pos + i]
                    if keyboard_interrupted:
                        progress_queue.put_nowait((context_id, 'task_cancelled', work_id))
                    else:
                        task = asyncio.ensure_future(func(work_id, *func_args, context_vars=context_vars, **func_kwargs))
                        progress_queue.put_nowait((context_id, 'task_scheduled', work_id))
                        cur_task_map[task] = work_id
                cur_pos += spare

            # get some tasks done
            done_task_set, _ = await asyncio.wait(cur_task_map.keys(), return_when=asyncio.FIRST_COMPLETED)
            for task in done_task_set:
                work_id = cur_task_map[task]
                msg = get_done_task_result(task, work_id)
                progress_queue.put_nowait(msg)
                del cur_task_map[task]
                if msg[1] == 'task_raised' and isinstance(msg[3], KeyboardInterrupt):
                    keyboard_interrupted = True


async def asyn_work_generator(func, func_args: list = [], func_kwargs: dict = {}, num_processes = None, num_works: int = 0, max_concurrency: int = 1024, profile = None, debug_logger = None, progress_queue = None):
    '''An asyn generator that does a large number of works concurrently and yields the work results.

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
    func_args : list
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

    Notes
    -----
    The context ids are zero-based. The number of contexts is equal to the number of processes.
    The work ids are task ids and are zero-based. The messages related to when a task is done are
    yielded.

    Asyncio and KeyboardInterrupt are not happy with each other.
    https://bugs.python.org/issue42683
    '''

    if num_works <= 0:
        return

    # for communicating between processes
    queue = _mp.Queue() if progress_queue is None else progress_queue

    num_buckets = _mp.cpu_count() if num_processes is None else num_processes
    work_id_list_list = split_works(num_works, num_buckets)


    def worker_process(progress_queue: _mp.Queue, func, func_args: list = [], func_kwargs: dict = {}, context_id = None, work_id_list: list = [], max_concurrency: int = 1024, profile = None):
        import asyncio
        from .s3 import create_context_vars

        async def asyn_func():
            async with create_context_vars(asyn=True, profile=profile) as context_vars:
                content = await run_asyn_works_in_context(
                    progress_queue, func,
                    func_args=func_args,
                    func_kwargs=func_kwargs,
                    context_id=context_id,
                    work_id_list=work_id_list,
                    max_concurrency=max_concurrency,
                    context_vars=context_vars,
                )
            return content

        progress_queue.put_nowait((context_id, 'context_created'))
        try:
            asyncio.run(asyn_func())
        except KeyboardInterrupt as e: # asyncio sucks
            from .traceback import extract_stack_compact
            progress_queue.put_nowait((context_id, 'context_raised', e, extract_stack_compact()))
        progress_queue.put_nowait((context_id, 'context_destroyed'))

    # launch the concurrency suite
    process_list = []
    for context_id in range(num_buckets):
        p = _mp.Process(
            target=worker_process,
            args=(queue, func),
            kwargs={
                'func_args': func_args,
                'func_kwargs': func_kwargs,
                'context_id': context_id,
                'work_id_list': work_id_list_list[context_id],
                'max_concurrency': max_concurrency,
                'profile': profile,
            })
        p.start()
        process_list.append(p)

    # wait for every task to be done
    num_running_buckets = num_buckets
    wait_cnt = 0
    keyboard_interrupted = False
    while (num_running_buckets > 0) and wait_cnt < 300:
        try:
            msg = queue.get_nowait()
            wait_cnt = 0
            if debug_logger:
                debug_logger.debug("asyn_work_generator: {}".format(msg))
            if msg[1] == 'context_destroyed':
                num_running_buckets -= 1
            elif msg[1] in ('task_returned', 'task_cancelled', 'task_raised'):
                yield msg[1:]
        except _q.Empty:
            try:
                await asyncio.sleep(0.1)
            except KeyboardInterrupt:
                if debug_logger:
                    debug_logger.warn("Keyboard interrupted. Will reraise when the time is right.")
                keyboard_interrupted = True
            wait_cnt += 1

    # clean up
    if wait_cnt < 300: # healthy
        for p in process_list:
            p.join()
    else: # something wrong
        if debug_logger:
            debug_logger.debug("asyn_work_generator: 30s timeout reached.")
        for p in process_list:
            p.terminate() # ouch!

    if keyboard_interrupted:
        raise KeyboardInterrupt("Keyboard interrupted while asyn_work_generator() is running.")
