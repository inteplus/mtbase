'''Concurrency in dask way. Needed for streaming workloads.'''

import multiprocessing as _mp
from dask.distributed import Client
from distributed.client import Future
from . import home_dirpath
from .path import join, make_dirs
from .deprecated import deprecated_func


__all__ = ['get_dd_client', 'reset_dd_client', 'bg_run', 'is_future', 'max_num_threads', 'Counter']


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

def max_num_threads(client=None):
    '''Retrieves the maximum number of threads the client can handle concurrently. Uses the default dask.distributed client if None is given.'''
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
