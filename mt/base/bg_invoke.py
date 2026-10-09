"""Wrapper around threading for running things in background.

:class:`BgInvoke` runs one function call in a background thread, :class:`BgThread` is a
long-lived background thread that can run the same function many times, :func:`parallelise` runs
many jobs over a pool of threads, and :func:`bg_run_proc` queues fire-and-forget procedures. An
exception raised in a background thread is captured and re-raised in the caller as a
:class:`BgException` when the result is read.
"""

from collections import deque
import multiprocessing as _mp
import threading as _t
import sys as _sys
import os as _os

from mt import time, traceback, logg


__all__ = [
    "BgException",
    "BgInvoke",
    "BgThread",
    "parallelise",
    "bg_run_proc",
    "bg_proc_manager",
]


class BgException(Exception):
    """Raised in the calling thread when a function run in a background thread raised an exception.

    The message contains the formatted traceback of the original exception.

    Parameters
    ----------
    message : str
        short description, placed at the top of the message
    exc_info : tuple
        the `(type, value, traceback)` triple of the original exception, as returned by
        :func:`sys.exc_info`
    """

    def __init__(self, message, exc_info):
        lines = traceback.format_exception(*exc_info)
        lines = ["  " + x for x in lines]
        lines = [message, "{"] + lines + ["}"]
        message = "\n".join(lines)
        super().__init__(message)


class BgInvoke:
    """Thin wrapper around threading.Thread to run `target(*args, **kwds)` in background.

    The thread starts immediately at construction and is a daemon thread, so it does not keep the
    interpreter alive. Poll `self.is_running()` until it returns `False`, at which point attribute
    `self.result` returns the output of the invocation.

    If an exception was raised in the target, reading `self.result` raises a :class:`BgException`
    that carries the original traceback. Only subclasses of :class:`Exception` are captured.

    Parameters
    ----------
    target : callable
        callable object to be invoked in the background thread
    *args : tuple
        positional arguments for the target invocation
    **kwds : dict
        keyword arguments for the target invocation

    Attributes
    ----------
    thread : threading.Thread
        the background thread
    result : object
        the return value of the target. Reading it raises :class:`ValueError` if the invocation
        has not finished yet and :class:`BgException` if the target raised an exception.

    Notes
    -----
    BgInvoke differs from BgThread in that the function can be invoked only once.

    Examples
    --------
    >>> def slow_count(nbTimes=1000000):
    ...     cnt = 0
    ...     for i in range(nbTimes):
    ...         cnt = cnt+1
    ...     return cnt
    ...
    >>> from mt.base.bg_invoke import BgInvoke
    >>> from time import sleep
    >>> a = BgInvoke(slow_count, nbTimes=1000000)
    >>> while a.is_running():
    ...     sleep(0.1)
    ...
    >>> print(a.result)
    1000000
    """

    def _wrapper(self, g, *args, **kwds):
        """Runs `g(*args, **kwds)` and stores either its output or its exception info."""

        try:
            self._result = True, g(*args, **kwds)
        except Exception:
            self._result = False, _sys.exc_info()

    @property
    def result(self):
        """The result of invoking the function.

        Raises
        ------
        ValueError
            if the invocation has not finished yet
        BgException
            if the function raised an exception
        """
        if hasattr(self, "_result"):
            if self._result[0]:  # succeeded
                return self._result[1]  # returning object
            else:  # thread generated an exception
                raise BgException(
                    f"Exception raised in background thread {self.thread.ident}",
                    self._result[1],
                )
        else:
            raise ValueError("Result is not available.")

    def __init__(self, target, *args, **kwds):
        """Initialises the invocation of `target(*args, **kwds)` in background.

        Parameters
        ----------
        target : callable
            callable object to be invoked
        *args : tuple
            positional argument tuple for the target invocation
        **kwds : dict
            dictionary of keyword arguments for the target
        """
        self.thread = _t.Thread(
            target=self._wrapper, args=(target,) + args, kwargs=kwds
        )
        self.thread.daemon = True
        self.thread.start()

    def is_running(self):
        """Returns whether the invocation is still running."""
        return self.thread.is_alive()


class BgThread(_t.Thread):
    """Thin wrapper around threading.Thread to run `func(*args, **kwds)` in background.

    The thread starts immediately at construction, as a daemon thread, and then sleeps. Once invoked
    via :func:`invoke`, the thread waits until the last function call has finished, then invokes the
    function with new arguments in background. The user can do other things. They should invoke
    :func:`is_running` to determine when the function invocation is completed. They can also use
    attribute :attr:`result` to wait and get the result. Call :func:`close` when done (it is also
    called when the object is deleted).

    If an exception was raised in the function, reading `self.result` raises a :class:`BgException`.

    Parameters
    ----------
    func : function
        a function to be wrapped

    Attributes
    ----------
    result : object
        the result of the latest invocation. It blocks until that result becomes available and
        consumes it, so reading it twice without a new :func:`invoke` in between blocks forever.

    Notes
    -----
    BgThread differs from BgInvoke in that the function can be invoked many times.

    Examples
    --------
    >>> def slow_count(nbTimes=1000000):
    ...     cnt = 0
    ...     for i in range(nbTimes):
    ...         cnt = cnt+1
    ...     return cnt
    ...
    >>> from mt.base.bg_invoke import BgThread
    >>> from time import sleep
    >>> a = BgThread(slow_count)
    >>> a.invoke(nbTimes=1000000)
    >>> print(a.result)
    1000000
    >>> a.invoke(nbTimes=100000)
    >>> print(a.result)
    100000
    >>> a.close()
    """

    def __init__(self, func):

        """Wraps `func` and starts the background thread."""

        super(BgThread, self).__init__()

        self.func = func

        self.input_cv = _t.Condition()
        self.new_input = False
        self.func_args = []
        self.func_kwds = {}
        self.is_thread_running = True

        self.func_running = False

        self.output_cv = _t.Condition()
        self.new_output = False
        self.func_result = None

        self.daemon = True  # daemon thread
        self.start()

    def close(self):
        """Waits for the current task to be done and closes the thread.

        After closing, :func:`invoke` and the :attr:`result` property raise
        :class:`RuntimeError`. An input that was submitted but not yet picked up by the thread
        is discarded.

        Raises
        ------
        TimeoutError
            if the thread is still alive 30 seconds after the current task has finished
        """
        # print("Bg thread {} closing.".format(self.ident))
        with self.input_cv:
            self.is_thread_running = False
            self.input_cv.notify()
        while self.is_running():
            time.sleep(1)
        self.join(30)
        if self.is_alive():
            raise TimeoutError(
                f"Background thread {self.ident} takes too long to close."
            )

    def __del__(self):
        """Closes the thread."""

        self.close()

    def run(self):
        """The wrapping code to be run in the background thread. Do not call."""

        while True:
            # get input
            with self.input_cv:
                while self.is_thread_running and not self.new_input:
                    self.input_cv.wait()
                if not self.is_thread_running:
                    return  # die happily
                args = self.func_args
                kwds = self.func_kwds
                self.new_input = False  # consume the new input
                self.input_cv.notify()

            # execute
            self.func_running = True
            try:
                result = True, self.func(*args, **kwds)
            except Exception:
                result = False, _sys.exc_info()
            self.func_running = False

            # put output
            with self.output_cv:
                self.func_result = result
                self.new_output = True
                self.output_cv.notify()

    def is_running(self):
        """Returns whether or not the function is running."""
        return self.func_running

    def invoke(self, *args, **kwds):
        """Invokes the function in a graceful way.

        This function blocks only until the previous arguments have been picked up by the thread (so
        it may wait if the thread has just been invoked), then hands over the new arguments. It
        returns without waiting for the function to finish. Use :attr:`result` to get the output.

        Parameters
        ----------
        *args : list
            positional arguments of the function
        **kwds : dict
            keyword arguments of the function

        Raises
        ------
        RuntimeError
            if the thread has been closed
        """

        with self.input_cv:
            while (
                self.is_thread_running and self.new_input
            ):  # current input not yet consumed
                self.input_cv.wait()
            if not self.is_thread_running:
                raise RuntimeError(
                    "Cannot invoke the function when the thread is dead."
                )
            self.func_args = args
            self.func_kwds = kwds
            self.new_input = True
            self.input_cv.notify()

    @property
    def result(self):
        """The result after waiting for the function call to finish.

        The result is consumed by reading it. Reading it again blocks until the function is invoked
        again and finishes.

        Raises
        ------
        BgException
            if the function raised an exception
        RuntimeError
            if the thread has been closed
        """
        with self.output_cv:
            while (
                self.is_thread_running and not self.new_output
            ):  # no output produced yet
                self.output_cv.wait()
            if not self.is_thread_running:
                raise RuntimeError(
                    "Cannot get the function result when the thread is dead."
                )
            func_result = self.func_result
            self.new_output = False  # consume the result
            self.output_cv.notify()

        if func_result[0]:  # succeeded
            return func_result[1]  # returning object
        else:  # thread generated an exception
            raise BgException(
                f"Exception raised in background thread {self.ident}",
                func_result[1],
            )


def parallelise(
    func,
    num_jobs,
    *fn_args,
    num_threads=None,
    bg_exception="raise",
    logger=None,
    pass_logger=False,
    **fn_kwds,
):
    """Embarrassingly parallelises the execution of many jobs with a limited number of threads.

    Job `i`, for `i` in `range(num_jobs)`, calls `func(i, *fn_args, **fn_kwds)` in a background
    thread (see :class:`BgInvoke`), with at most `num_threads` jobs running at the same time.
    Completion is polled about once per second when all threads are busy, so it is meant for
    jobs that take a while, rather than for tiny ones. The call returns when all jobs are done.

    Parameters
    ----------
    func : function
        a function of the form `def func(job_id, *args, **kwds)` that can return something
    num_jobs : int
        number of jobs to execute, must be positive integer
    *fn_args : iterable
        list of positional arguments for the function, after the job id
    num_threads : int, optional
        number of threads to be used. If not specified, then 80% of available CPUs are used
        (at least 1)
    bg_exception : {'raise', 'warn'}
        whether to re-raise a :class:`BgException` raised by a thread or to suppress all of them and
        warn instead, in which case the output of the failed job is None. Warnings are only
        logged if `logger` is provided, otherwise the exception is silently dropped. The value is
        only checked when a job raises.
    logger : mt.logg.IndentedLoggerAdapter, optional
        for logging purposes
    pass_logger : bool
        whether or not to include `logger` to `fn_kwds` if it does not exist in `fn_kwds`
        so the function can use the same logger as `parallelise()`
    **fn_kwds : dict
        list of keyword arguments for the function

    Returns
    -------
    list
        a list of `num_jobs` elements, each corresponding to the returning object of the function
        for a given job id

    Raises
    ------
    ValueError
        if `num_jobs` is not a positive integer, or if `bg_exception` is unknown and a job fails
    BgException
        an exception raised by a background thread, if `bg_exception` is 'raise'

    Notes
    -----
    Use this function instead of joblib if you want to integrate with mt.logg and
    BgException better.

    Examples
    --------
    >>> from mt.base.bg_invoke import parallelise
    >>> def multiply(job_id, factor):
    ...     return job_id * factor
    ...
    >>> parallelise(multiply, 4, 10, num_threads=2)
    [0, 10, 20, 30]
    """
    if not isinstance(num_jobs, int) or num_jobs <= 0:
        raise ValueError(
            f"A positive integer is expected for argument 'num_jobs', but was given {num_jobs}."
        )

    if pass_logger:
        if not "logger" in fn_kwds:
            fn_kwds["logger"] = logger

    max_num_conns = (
        max(1, _os.cpu_count() * 4 // 5) if num_threads is None else num_threads
    )
    threads = {}  # background threads
    thread_outputs = [None] * num_jobs

    with logg.scoped_info(
        f"Parallelise {num_jobs} jobs using {max_num_conns} threads",
        logger=logger,
        curly=False,
    ):
        i = 0
        threads = {}
        while i <= num_jobs:
            if i < num_jobs:  # still has a job to execute
                if len(threads) < max_num_conns:
                    if logger is not None and i % 1000 == 0:
                        logger.info(f"Job {i + 1}/{num_jobs}...")
                    # send the job to the thread
                    threads[i] = BgInvoke(func, i, *fn_args, **fn_kwds)
                    i += 1
                else:
                    time.sleep(1)
            else:  # all jobs sent
                if threads:  # waiting for exising threads to finish
                    time.sleep(1)
                else:  # seems like all jobs are done
                    i += 1

            indices = []  # list of threads that have completed their job
            for i2, v2 in threads.items():
                if not v2.is_running():
                    indices.append(i2)

            for i2 in indices:
                try:
                    thread_outputs[i2] = threads[i2].result
                except BgException:
                    if bg_exception == "raise":
                        raise
                    if bg_exception == "warn":
                        with logg.scoped_warning(
                            f"Caught an exception from job {i2}:", logger=logger
                        ):
                            if logger:
                                logger.warn_last_exception()
                    else:
                        raise ValueError(
                            f"Argument 'bg_exception' has an unknown value '{bg_exception}'."
                        )
                threads.pop(i2)

    return thread_outputs


class BgProcManager:
    """A manager to run procedures in background threads.

    A procedure is a function that takes no input and returns None. Procedures are queued with
    :func:`append_new_proc` and are run, in order, by background threads, at most one per CPU at a
    time. If an error is raised while executing a procedure, it is reported as a warning in the
    logger (optional).

    Parameters
    ----------
    logger : logging.Logger or equivalent
        logger for catching the exceptions as warnings

    See Also
    --------
    bg_run_proc : the user-friendly way to queue a procedure on the default manager
    """

    def __init__(self, logger=None):
        """Creates the manager and starts its manager thread."""

        self.logger = logger

        self.is_alive = True

        self.lock = _t.Lock()
        self.running_threads = []
        self.deque = deque()
        self.max_num_threads = _mp.cpu_count()

        self.manager_thread = BgInvoke(self._run)

    def append_new_proc(self, proc):
        """Appends a new procedure to the queue.

        Parameters
        ----------
        proc : function
            a procedure taking no input. If it an error is raised, it will be reported as a warning
            in the logger.
        """
        if not self.is_alive:
            if self.logger:
                self.logger.warn(
                    "Cannot append a new procedure when the manager is being closed."
                )
            return

        self.deque.append(proc)

    def _run(self):
        """Manager thread. Do not invoke the function externally."""

        while True:
            # clean up finished threads
            new_running_threads = []
            for bg_thread in self.running_threads:
                if bg_thread.is_running():
                    new_running_threads.append(bg_thread)
                else:
                    try:
                        _ = bg_thread.result
                    except BgException:
                        if self.logger:
                            self.logger.warn_last_exception()
            self.running_threads = new_running_threads

            # see if we can invoke a new thread
            new_thread_cnt = max(
                0,
                min(len(self.deque), self.max_num_threads - len(self.running_threads)),
            )
            for i in range(new_thread_cnt):
                proc = self.deque.popleft()
                bg_thread = BgInvoke(proc)
                self.running_threads.append(bg_thread)

            # see if we can quit
            if not self.is_alive and not self.deque and not self.running_threads:
                break

            time.sleep(0.1)

    def is_deque_empty(self):
        """Returns whether no procedure is waiting in the queue."""

        return not self.deque

    def is_no_running_threads(self):
        """Returns whether no procedure is currently running."""

        return not self.running_threads

    def wait_until_empty(self):
        """Blocks until the queue is empty and all running procedures are done."""

        time.sleep_until(self.is_deque_empty, logger=self.logger)
        time.sleep_until(self.is_no_running_threads, logger=self.logger)

    def __del__(self):
        """Stops accepting procedures and waits for the pending ones to finish."""

        self.is_alive = False
        self.wait_until_empty()


bg_proc_manager = BgProcManager(logger=logg.logger)


def bg_run_proc(proc, *args, **kwds):
    """Runs a procedure in a background thread.

    The procedure is queued on the module-level manager `bg_proc_manager`, which runs at most one
    procedure per CPU at a time. To wait for all queued procedures, invoke
    `bg_proc_manager.wait_until_empty()`.

    Parameters
    ----------
    proc : function
        procedure (a function returning None) to be executed in a background thread. Any Exception
        raised during the execution will be logged as a warning
    *args : iterable
        positional arguments to be passed as-is to the procedure
    **kwds : dict
        keyword arguments to be passed as-is to the procedure

    Examples
    --------
    >>> from mt.base.bg_invoke import bg_run_proc, bg_proc_manager
    >>> bg_run_proc(print, "hello", "world", sep=", ")
    >>> bg_proc_manager.wait_until_empty()
    hello, world
    """
    bg_proc_manager.append_new_proc(lambda: proc(*args, **kwds))
