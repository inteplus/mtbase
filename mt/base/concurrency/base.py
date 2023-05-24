"""Common tools for concurrency."""

import multiprocessing as _mp
import numpy as np
import psutil

from mt import tp


class Counter(object):

    """Counter class without the race-condition bug"""

    def __init__(self):
        self.val = _mp.Value("i", 0)

    def increment(self, n=1):
        with self.val.get_lock():
            self.val.value += n

    @property
    def value(self):
        return self.val.value


# ----------------------------------------------------------------------


def used_memory_too_much():
    """Checks whether memory usage exceeeds 90%."""
    mem = psutil.virtual_memory()
    return mem.percent > 90  # 90% usage is the limit


def used_cpu_too_much():
    """Checks whether CPU usage exceeds 90%."""
    return psutil.cpu_percent() > 90  # 90% usage is the limit


def split_works(num_works: int, num_buckets: tp.Optional[int] = None):
    """Splits a number of works randomly into a few buckets, returning the work id per bucket.

    Parameters
    ----------
    num_works : int
        number of works

    num_buckets : int, optional
        number of buckets. If not specified, the number of cpus will be used.

    Returns
    -------
    ll_workIds : list
        a nested list of work id lists. The work ids, in interval [0, num_works), are split
        approximately evenly and randomly into the buckets
    """

    N = num_works
    K = _mp.cpu_count() if num_buckets is None else num_buckets

    rng = np.random.default_rng()
    vals = np.floor(rng.uniform(0, K, size=N)).astype(int)
    retval = [[] for k in range(K)]
    for n in range(N):  # slow but fine
        k = vals[n]
        retval[k].append(n)

    return retval


def serial_work_generator(func, num_work_ids: int):
    """A generator that serially does some works and yields the work results.

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
    """
    for work_id in range(num_work_ids):
        yield func(work_id)
