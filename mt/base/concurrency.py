from dask.distributed import Client
from distributed.client import Future
import os.path as op
import os
import warnings

def get_dd_client():
    '''Gets the dask.distributed client created internally.'''
    if get_dd_client.client is None:
        home_path = op.join(op.expanduser('~'), '.basemt', 'dask-worker-space')
        os.makedirs(home_path, exist_ok=True)
        get_dd_client.client = Client(local_dir=home_path)
    return get_dd_client.client
get_dd_client.client = None

def reset_dd_client():
    '''Removes the dask.distributed client explicitly.'''
    if get_dd_client.client is not None:
        get_dd_client.client.close()
        get_dd_client.client = None

def bg_run(func, *args, **kwargs):
    '''Runs a function in background and return a future object.'''
    warnings.warn("Warning: this function is deprecated. Use mt.base.bg_invoke.BgInvoke instead.")
    return get_dd_client().submit(func, *args, **kwargs)

def is_future(obj):
    '''Checks if an object is a Future object.'''
    warnings.warn("Warning: this function is deprecated. Use mt.base.bg_invoke.BgInvoke instead.")
    return isinstance(obj, Future)
