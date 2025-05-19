'''In-memory object store that can be used by multiple processes.

MT is tring to make sure that the functions in this module are multiprocessing-safe and thread-safe.
'''


import multiprocessing as _mp
import threading as _t
import psutil as _pu
from time import time


__all__ = ['create', 'valid', 'count', 'keys', 'has', 'get', 'put', 'remove']


# main server process
server_process = None

# thread lock to make sure one thread runs a command at a time
_mp_lock = _mp.RLock()
_thread_lock = _t.RLock()


def create(min_free_mem_pct=0.2, put_policy='rotate'):
    '''Creates an in-memory object store.

    Parameters
    ----------
    min_free_mem_pct : float
        the minimum percentage of free physical memory over total physical memory where an object can be put to the store without restriction. When the free memory percentage drops below the given value, a put policy is activated to decide how to proceed further
    put_policy : {'rotate', 'strict'}
        policy for putting an object to the object store when the free memory percentage drops below the given value in `min_free_mem_pct`. If 'rotate' is given, the object store keeps removing earliest-accessed-time objects from the store until enough memory is available or the store is empty. If 'strict' is given, it does nothing. Then, it stores the object if there is enough memory.

    Returns
    -------
    multiprocessing.managers.DictProxy
        an object store that can be passed to other processes.
    '''
    if not put_policy in ['rotate', 'strict']:
        raise ValueError(f"Unknown put policy: '{put_policy}'.")

    global server_process
    if not server_process:
        server_process = _mp.Manager()

    store = server_process.dict()
    store['type'] = 'object_store'
    store['min_free_mem_pct'] = min_free_mem_pct
    store['put_policy'] = put_policy
    #store['lock'] = server_process.RLock()
    return store

    
def valid(store):
    '''Checks whether an object is a valid object store.
    
    Parameters
    ----------
    store : multiprocessing.managers.DictProxy
        object store
    
    Returns
    -------
    bool
        whether or not the input argument is a valid object store
    '''
    return hasattr(store, 'get') and store.get('type', None) == 'object_store'


def store_exec(func, store, *args, **kwargs):
    '''Executes a function related to a store safely by locking the store, executing the function, then unlocking the store.

    Parameters
    ----------
    func : function
        function to be invoked
    store : multiprocessing.managers.DictProxy
        object store
    args : list
        positional arguments to pass to the function
    kwargs : dict
        keyword arguments to pass to the function
    
    Returns
    -------
    object
        whatever that the function returns

    Raises
    ------
    TimeoutError
        if we cannot lock the object store to proceed
    '''
    #process_lock = store['lock']
    process_lock = _mp_lock
    if not process_lock.acquire(block=True, timeout=30.0): # multiprocessing-safe
        raise TimeoutError("Timeout in acquiring the object store's process lock.")
    try:
        if not _thread_lock.acquire(blocking=True, timeout=30.0): # thread-safe
            raise TimeoutError("Timeout in acquiring the object store's thread lock.")
        try:
            return func(store, *args, **kwargs)
        finally:
            _thread_lock.release()
    finally:
        process_lock.release()


def count(store):
    '''Returns the number of objects in the store.

    Parameters
    ----------
    store : multiprocessing.managers.DictProxy
        object store
    
    Returns
    -------
    int
        number of objects in the store
    '''
    return store_exec(lambda x: len(x)-3, store)


def has(store, key):
    '''Checks if an object exists the store given its key.

    Parameters
    ----------
    store : multiprocessing.managers.DictProxy
        object store
    key : str
        the key to identify the object
    
    Returns
    -------
    bool
        whether or not the key exists in the store
    '''
    return store_exec((lambda store, key: 'item_'+key in store), store, key)


def keys(store):
    '''Gets the list of keys the store contains.

    Parameters
    ----------
    store : multiprocessing.managers.DictProxy
        object store
    
    Returns
    -------
    list
        list of keys
    '''
    return store_exec(lambda store: [key[5:] for key in store.keys() if key.startswith('item_')], store)


def get(store, key, default_value=None):
    '''Gets an object in the store given its key.

    Parameters
    ----------
    store : multiprocessing.managers.DictProxy
        object store
    key : str
        the key to identify the object
    
    Returns
    -------
    object
        the object associated with the key, or default value if not found
    '''
    def func(store, key, default_value=None):
        if not has(store, key):
            return default_value

        pair = store['item_'+key]
        obj = pair[0] # actual object
        pair[1] = time() # access time
        return obj
    return store_exec(func, store, key, default_value=default_value)


def remove(store, key):
    '''Attempts to remove an object in the store based on its key.

    Parameters
    ----------
    store : multiprocessing.managers.DictProxy
        object store
    key : str
        the key to identify the object
    
    Returns
    -------
    bool
        whether or not the object has been removed.

    Raises
    ------
    TimeoutError
        if we cannot lock the object store to proceed
    '''
    def func(store, key):
        if not has(store, key):
            return False

        try:
            del store['item_'+key]
            return True
        except:
            return False
    return store_exec(func, store, key)


def put(store, key, value):
    '''Puts an object in the store.

    Parameters
    ----------
    store : multiprocessing.managers.DictProxy
        object store
    key : str
        the key to identify the object
    value : object
        the object itself
    
    Returns
    -------
    bool
        whether or not the object has been stored. Check :func:`create` for more details about the put policy.

    Raises
    ------
    TimeoutError
        if we cannot lock the object store to proceed
    '''
    def func(store, key, value):
        # delete if the key exists
        remove(store, key)

        while True:
            stats = _pu.virtual_memory()
            free_mem_pct = stats.available / stats.total
            if free_mem_pct >= store['min_free_mem_pct']:
                outcome = True
                break
            if count(store) == 0:
                outcome = False
                break
            if store['put_policy'] == 'strict':
                outcome = False
                break

            # find the key corresponding to the oldest object
            old_key = None
            old_time = None
            try:
                for key2 in store.keys():
                    if not key2.startswith('item_'):
                        continue
                    pair = store[key2]
                    if not old_time or old_time > pair[1]:
                        old_key = key2[5:]
                        old_time = pair[1]
            except:
                pass # MT-NOTE: Can't go through all keys. Something went wrong. Just bail out.
            if not old_key:
                outcome = False
                break
            remove(store, old_key)

        if not outcome:
            return False

        store['item_'+key] = [value, time()]
        return True
    return store_exec(func, store, key, value)
    
