'''Concurrency in dask way. Needed for streaming workloads.'''

import multiprocessing as _mp
from dask.distributed import Client
from distributed.client import Future
from . import home_dirpath
from .path import join, make_dirs
from .deprecated import deprecated_func
from .logging import logger


__all__ = ['get_dd_client', 'reset_dd_client', 'bg_run', 'is_future', 'max_num_threads', 'Counter', 'ProcessParalleliser']


def get_dd_client():
    '''Gets the dask.distributed client created internally.'''
    if get_dd_client.client is None:
        home_dd_dirpath = join(home_dirpath, 'dask-worker-space')
        make_dirs(home_dd_dirpath)
        get_dd_client.client = Client(local_dir=home_dd_dirpath)
    return get_dd_client.client
get_dd_client.client = None

def reset_dd_client():
    '''Removes the dask.distributed client explicitly.'''
    if get_dd_client.client is not None:
        get_dd_client.client.close()
        get_dd_client.client = None

def max_num_threads(client=None, use_dask=True):
    '''Retrieves the maximum number of threads the client can handle concurrently. 

    Parameters
    ----------
    client : dask.distributed.Client, optional
        If 'use_dask' is True, this argument specifies the dask distributed client. It uses the default dask.distributed client if None is given.
    use_dask : bool
        whether or not we use dask distributed to count the number of threads. If not, we use :func:`multiprocessing.cpu_count`.
    '''
    if not use_dask:
        return _mp.cpu_count()

    if client is None:
        client = get_dd_client()
    return sum(client.nthreads().values())

def bg_run(func, *args, **kwargs):
    '''Runs a function in dask.distributed client's background and return a future object.'''
    return get_dd_client().submit(func, *args, **kwargs)

def is_future(obj):
    '''Checks if an object is a dask Future object.'''
    return isinstance(obj, Future)


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


def _worker_process(func, queue_in, queue_out):
    while True:
        worker_id = queue_in.get()
        if not isinstance(worker_id, int) or worker_id < 0:
            return # stop the process

        try:
            res = func(worker_id)
        except:
            with logger.scoped_warn("Returning None since background worker has caught an exception", curly=False):
                logger.warn_last_exception()
            res = None
        queue_out.put((worker_id, res))


class ProcessParalleliser(object):
    '''Run a function with different inputs in parallel using multiprocessing.

    Parameters
    ----------
    func : function
        a function to be run in parallel. The function takes as input a non-negative integer 'worker_id' and returns something.
    '''

    def __init__(self, func):
        self.func = func
        self.queue_in = _mp.Queue()
        self.queue_out = _mp.Queue()
        self.process_list = [_mp.Process(target=_worker_process, args=(func, self.queue_in, self.queue_out)) for i in range(_mp.cpu_count())]

        # start all background processes
        for p in self.process_list:
            p.start()

    def __del__(self):
        # send commands to terminate the processes
        for n in range(len(self.process_list)*2):
            self.queue_in.put(-1)

        # wait for them to be terminated
        for p in self.process_list:
            p.join()
            p.terminate()
            #p.close()


    def push(self, worker_id):
        '''Pushes a worker id to the background to run the provided function in parallel.

        Parameters
        ----------
        worker_id : int
            non-negative integer to be provided to the function

        Notes
        -----
        This function returns nothing. You should use :func:`pop` or :func:`empty` to check the output.
        '''
        if not isinstance(worker_id, int):
            raise ValueError("Worker id is not an integer. Got {}.".format(worker_id))
        if worker_id < 0:
            raise ValueError("Worker id is negative. Got {}.".format(worker_id))
        self.queue_in.put(worker_id)

    def empty(self):
        '''Returns whether the output queue is empty.'''
        return self.queue_out.empty()

    def pop(self):
        '''Returns a pair (worker_id, result) when at least one such pair is available.'''
        return self.queue_out.get()
