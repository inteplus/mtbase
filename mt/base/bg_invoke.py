'''Wrapper around threading for running things in background.'''


import threading as _t
import sys as _sys
import os as _os
from time import sleep

from .logging import dummy_scope
from .traceback import format_exception


__all__ = ['BgException', 'BgInvoke', 'parallelise']


class BgException(Exception):

    def __init__(self, message, exc_info):
        lines = format_exception(*exc_info)
        lines = ["  "+x for x in lines]
        lines = [message, "{"] + lines + ["}"]
        message = '\n'.join(lines)
        super().__init__(message)


class BgInvoke(object):
    '''Thin wrapper around threading.Thread to run `target(*args, **kwargs)` in background.

    Once invoked, the thread keeps running in background until function `self.is_running()` returns `False`, at which point `self.result` holds the output of the invocation.
    
    If an exception was raised, `self.result` would contain the exception and other useful information.

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
