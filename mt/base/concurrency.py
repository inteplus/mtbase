'''Concurrency in dask way. Needed for streaming workloads.'''

from dask.distributed import Client
from distributed.client import Future
from . import home_dirpath
from .path import join, make_dirs
from .deprecated import deprecated_func


__all__ = ['get_dd_client', 'reset_dd_client', 'bg_run', 'is_future']


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

def bg_run(func, *args, **kwargs):
    '''Runs a function in dask.distributed client's background and return a future object.'''
    return get_dd_client().submit(func, *args, **kwargs)

def is_future(obj):
    '''Checks if an object is a dask Future object.'''
    return isinstance(obj, Future)
