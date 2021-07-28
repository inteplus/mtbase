'''Wrapper around threading for running things in background.'''


from collections import deque
import multiprocessing as _mp
import threading as _t
import sys as _sys
import os as _os
from time import sleep

from .with_utils import dummy_scope
from .traceback import format_exception
from .time import sleep_until
from .logging import logger


__all__ = ['BgException', 'BgInvoke', 'BgThread', 'parallelise', 'bg_run_proc']


class BgException(Exception):

    def __init__(self, message, exc_info):
        lines = format_exception(*exc_info)
        lines = ["  "+x for x in lines]
        lines = [message, "{"] + lines + ["}"]
        message = '\n'.join(lines)
        super().__init__(message)


class BgInvoke:
    '''Thin wrapper around threading.Thread to run `target(*args, **kwargs)` in background.

    Once invoked, the thread keeps running in background until function `self.is_running()` returns `False`, at which point `self.result` holds the output of the invocation.
    
    If an exception was raised, `self.result` would contain the exception and other useful information.

    Notes
    -----
    BgInvoke differs from BgThread in that the function can be invoked only once.

    Examples
    --------
    >>> def slow_count(nbTimes=100000000):
    ...     cnt = 0
    ...     for i in range(nbTimes):
    ...         cnt = cnt+1
    ...     return cnt
    ...
    >>> from mt.base.bg_invoke import BgInvoke
    >>> from time import sleep
    >>> a = BgInvoke(slow_count, nbTimes=100000000)
    >>> while a.is_running():
    ...     sleep(0.5)
    ...
    >>> print(a.result)
    100000000
    '''

    def _wrapper(self, g, *args, **kwargs):
        try:
            self._result = True, g(*args, **kwargs)
        except Exception as e:
            self._result = False, _sys.exc_info()

    @property
    def result(self):
        if hasattr(self, '_result'):
            if self._result[0]: # succeeded
                return self._result[1] # returning object
            else: # thread generated an exception
                raise BgException("Exception raised in background thread {}".format(self.thread.ident), self._result[1])
        else:
            raise ValueError("Result is not available.")

    def __init__(self, target, *args, **kwargs):
        '''Initialises the invocation of `target(*args, **kwargs)` in background.

        Parameters
        ----------
            target : callable
                callable object to be invoked. Default to None, meaning nothing is called.
            args : tuple
                argument tuple for the target invocation. Default to ().
            kwargs : dict
                dictionary of keyword arguments for the target. Default to {}.
        '''
        self.thread = _t.Thread(target=self._wrapper, args=(target,)+args, kwargs=kwargs)
        self.thread.daemon = True
        self.thread.start()

    def is_running(self):
        '''Returns whether the invocation is still running.'''
        return self.thread.is_alive()


class BgThread(_t.Thread):
    '''Thin wrapper around threading.Thread to run `func(*args, **kwargs)` in background.

    Once invoked via :func:`invoke`, the thread waits until the last function call has finished,
    then invokes the function with new arguments in background. The user can do other things.
    They should invoke :func:`is_running` to determine when the function invocation is completed.
    They can also use attribute :attr:`result` to wait and get the result.

    If an exception was raised, `self.result` would contain the exception and other useful
    information.

    Parameters
    ----------
    func : function
        a function to be wrapped

    Attributes
    ----------
    result : object
        the current result if it has not been consumed. Otherwise blocks until the new result
        becomes available.

    Notes
    -----
    BgThread differs from BgInvoke in that the function can be invoked many times.

    Examples
    --------
    >>> def slow_count(nbTimes=100000000):
    ...     cnt = 0
    ...     for i in range(nbTimes):
    ...         cnt = cnt+1
    ...     return cnt
    ...
    >>> from mt.base.bg_invoke import BgThread
    >>> from time import sleep
    >>> a = BgThread(slow_count)
    >>> a.invoke(nbTimes=100000000)
    >>> while a.is_running():
    ...     sleep(0.5)
    ...
    >>> print(a.result)
    100000000
    >>> a.invoke(nbTimes=100000)
    >>> print(a.result)
    100000
    '''

    def __init__(self, func):

        super(BgThread, self).__init__()

        self.func = func

        self.input_cv = _t.Condition()
        self.new_input = False
        self.func_args = []
        self.func_kwargs = {}
        self.is_thread_running = True

        self.func_running = False

        self.output_cv = _t.Condition()
        self.new_output = False
        self.func_result = None

        self.daemon = True # daemon thread
        self.start()


    def close(self):
        '''Waits for the current task to be done and closes the thread.'''
        #print("Bg thread {} closing.".format(self.ident))
        with self.input_cv:
            self.is_thread_running = False
            self.input_cv.notify()
        while self.is_running():
            sleep(1)
        self.join(30)
        if self.is_alive():
            raise TimeoutError("Background thread {} takes too long to close.".format(self.ident))


    def __del__(self):
        self.close()


    def run(self):
        '''The wrapping code to be run in the background thread. Do not call.'''

        while True:
            # get input
            with self.input_cv:
                while self.is_thread_running and not self.new_input:
                    self.input_cv.wait()
                if not self.is_thread_running:
                    return # die happily
                args = self.func_args
                kwargs = self.func_kwargs
                self.new_input = False # consume the new input
                self.input_cv.notify()

            # execute
            self.func_running = True
            try:
                result = True, self.func(*args, **kwargs)
            except:
                result = False, _sys.exc_info()
            self.func_running = False

            # put output
            with self.output_cv:
                self.func_result = result
                self.new_output = True
                self.output_cv.notify()


    def is_running(self):
        '''Returns whether or not the function is running.'''
        return self.func_running


    def invoke(self, *args, **kwargs):
        '''Invokes the function in a graceful way.

        This function blocks until the last function arguments have been consumed and then signals
        the function to be invoked again. It returns immediately.

        Parameters
        ----------
        args : list
            positional arguments of the function
        kwargs : dict
            keyword arguments of the function
        '''

        with self.input_cv:
            while self.is_thread_running and self.new_input: # current input not yet consumed
                self.input_cv.wait()
            if not self.is_thread_running:
                raise RuntimeError("Cannot invoke the function when the thread is dead.")
            self.func_args = args
            self.func_kwargs = kwargs
            self.new_input = True
            self.input_cv.notify()


    @property
    def result(self):
        '''The result after waiting for the function call to finish.'''
        with self.output_cv:
            while self.is_thread_running and not self.new_output: # no output produced yet
                self.output_cv.wait()
            if not self.is_thread_running:
                raise RuntimeError("Cannot get the function result when the thread is dead.")
            func_result = self.func_result
            self.new_output = False # consume the result
            self.output_cv.notify()

        if func_result[0]: # succeeded
            return func_result[1] # returning object
        else: # thread generated an exception
            raise BgException("Exception raised in background thread {}".format(self.ident),
                              func_result[1])


def parallelise(func, num_jobs, *fn_args, num_threads=None, bg_exception='raise', logger=None, pass_logger=False, **fn_kwargs):
    '''Embarrasingly parallelises to excecute many jobs with a limited number of threads.

    Parameters
    ----------
    func : function
        a function of the form `def func(job_id, *args, **kwargs)` that can return something
    num_jobs : int
        number of jobs to execute, must be positive integer
    num_threads : int, optional
        number of threads to be used. If not specified, then 80% of available CPUs are used
    bg_exception : {'raise', 'warn'}
        whether to re-rasise a BgException raised by a thread or to suppress all of them and warn instead, in which case `logger` must not be None
    logger : IndentedLoggerAdaptor, optional
        for logging purposes
    pass_logger : bool
        whether or not to include `logger` to `func_kwargs` if it does not exist in `func_kwargs` so the function can use the same logger as `parallelise()`
    fn_args : list
        list of postional arguments for the function
    fn_kwargs : dict
        list of keyword arguments for the function

    Returns
    -------
    list
        a list of `num_jobs` elements, each corresponding to the returning object of the function for a given job id

    Raises
    ------
    BgException
        an exception raised by a background thread

    Notes
    -----
        use this function instead of joblib if you want to integrate with mt.base.logging and BgException better
    '''
    if not isinstance(num_jobs, int) or num_jobs <= 0:
        raise ValueError("A positive integer is expected for argument 'num_jobs', but was given {}.".format(num_jobs))

    if pass_logger:
        if not 'logger' in fn_kwargs:
            fn_kwargs['logger'] = logger

    max_num_conns = max(1, _os.cpu_count()*4//5) if num_threads is None else num_threads
    threads = {}  # background threads
    thread_outputs = [None]*num_jobs

    with logger.scoped_info("Parallelise {} jobs using {} threads".format(num_jobs, max_num_conns), curly=False) if logger else dummy_scope:
        i = 0
        threads = {}
        while i <= num_jobs:
            if i < num_jobs: # still has a job to execute
                if len(threads) < max_num_conns:
                    if logger is not None and i % 1000 == 0:
                        logger.info("Job {}/{}...".format(i+1, num_jobs))
                    # send the job to the thread
                    threads[i] = BgInvoke(func, i, *fn_args, **fn_kwargs)
                    i += 1
                else:
                    sleep(1)
            else: # all jobs sent
                if threads: # waiting for exising threads to finish
                    sleep(1)
                else: # seems like all jobs are done
                    i += 1

            indices = [] # list of threads that have completed their job
            for i2 in threads:
                if not threads[i2].is_running():
                    indices.append(i2)

            for i2 in indices:
                try:
                    thread_outputs[i2] = threads[i2].result
                except BgException as e:
                    if bg_exception == 'raise':
                        raise
                    elif bg_exception == 'warn':
                        with logger.scoped_warning("Caught an exception from job {}:".format(i2), curly=False) if logger else dummy_scope:
                            logger.warn_last_exception()
                    else:
                        raise ValueError("Argument 'bg_exception' has an unknown value '{}'.".format(bg_exception))
                threads.pop(i2)

    return thread_outputs


class BgProcManager:
    '''A manager to run procedures in background threads.

    A procedure is a function that returns None. If it an error is raised while executing the
    procedure, it will be reported as a warning in the logger (optional).

    Parameters
    ----------
    logger : logging.Logger or equivalent
        logger for catching the exceptions as warnings
    '''

    def __init__(self, logger=None):
        self.running_threads = []
        self.deque = deque()
        self.max_num_threads = _mp.cpu_count()
        self.logger = logger

        self.is_alive = True

        self.lock = _t.Lock()
        self.new_proc = None

        self.manager_thread = BgInvoke(self._run)

    def append_new_proc(self, proc):
        '''Appends a new procedure to the queue.

        Parameters
        ----------
        proc : funcion
            a procedure taking no input. If it an error is raised, it will be reported as a warning in the logger.
        '''
        if not self.is_alive:
            if self.logger:
                self.logger.warn("Cannot append a new procedure when the manager is being closed.")
            return

        while True:
            with self.lock:
                if self.new_proc is not None:
                    sleep(0.01)
                    continue
                self.new_proc = proc
            break

    def _run(self):
        '''Manager thread. Do not invoke the function externally.'''

        while True:
            with self.lock:
                if self.new_proc is not None:
                    self.deque.append(self.new_proc)
                    self.new_proc = None

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
            if bool(self.deque) and (len(self.running_threads) < self.max_num_threads):
                proc = self.deque.popleft()
                bg_thread = BgInvoke(proc)
                self.running_threads.append(bg_thread)

            # see if we can quit
            if not self.is_alive and not self.deque and not self.running_threads:
                break
                
            sleep(0.01)

    def __del__(self):
        self.is_alive = False
        sleep_until(lambda: not self.deque, logger=self.logger)
        sleep_until(lambda: not self.running_threads, logger=self.logger)

bg_proc_manager = BgProcManager(logger=logger)

def bg_run_proc(proc, *args, **kwargs):
    '''Runs a procedure in a background thread.

    Parameters
    ----------
    proc : function
        procedure (a function returning None) to be executed in a background thread. Any Exception
        raised during the execution will be logged as a warning
    args : list
        positional arguments to be passed as-is to the procedure
    kwargs : dict
        keyword arguments to be passed as-is to the procedure
    '''
    bg_proc_manager.append_new_proc(lambda: proc(*args, **kwargs))
