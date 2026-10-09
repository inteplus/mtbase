"""Common tools for concurrency."""

import multiprocessing as _mp
import numpy as np
import psutil

from mt import tp


class Counter(object):

    """An integer counter that can be safely incremented from several processes or threads.

    The counter is backed by a shared-memory C int (:func:`multiprocessing.Value`) and every
    increment takes the value's lock, so concurrent increments are not lost. It starts at 0.

    Attributes
    ----------
    val : multiprocessing.sharedctypes.Synchronized
        the underlying shared integer

    Examples
    --------
    >>> from mt.concurrency import Counter
    >>> c = Counter()
    >>> c.increment()
    >>> c.increment(4)
    >>> c.value
    5
    """

    def __init__(self):
        self.val = _mp.Value("i", 0)

    def increment(self, n=1):
        """Atomically adds `n` to the counter.

        Parameters
        ----------
        n : int
            the amount to add. Defaults to 1. Negative values decrement the counter.
        """
        with self.val.get_lock():
            self.val.value += n

    @property
    def value(self):
        """int: the current value of the counter. Reading it does not take the lock."""
        return self.val.value


# ----------------------------------------------------------------------


def used_memory_too_much():
    """Checks whether the system-wide memory usage exceeds 90%.

    Returns
    -------
    bool
        True if :func:`psutil.virtual_memory` reports more than 90% of the memory in use. The
        result depends on the current state of the machine.

    See Also
    --------
    used_cpu_too_much : the CPU counterpart
    """
    mem = psutil.virtual_memory()
    return mem.percent > 90  # 90% usage is the limit


def used_cpu_too_much():
    """Checks whether the system-wide CPU usage exceeds 90%.

    Returns
    -------
    bool
        True if :func:`psutil.cpu_percent` reports more than 90%. With the default arguments of
        psutil, the usage is measured since the previous call (the very first call compares over
        a meaningless interval), so the result depends on the machine state and call history.

    See Also
    --------
    used_memory_too_much : the memory counterpart
    """
    return psutil.cpu_percent() > 90  # 90% usage is the limit


def split_works(num_works: int, num_buckets: tp.Optional[int] = None):
    """Splits a number of works randomly into a few buckets, returning the work id per bucket.

    Each work id is assigned independently to a uniformly random bucket, so bucket sizes are only
    approximately equal and a bucket may be empty. The assignment is not seeded, hence not
    reproducible. Within each bucket, work ids are in increasing order.

    Parameters
    ----------
    num_works : int
        number of works

    num_buckets : int, optional
        number of buckets. If not specified, the number of cpus (:func:`multiprocessing.cpu_count`)
        will be used.

    Returns
    -------
    ll_workIds : list
        a list of `num_buckets` lists of work ids. The work ids, in interval [0, num_works), are
        split approximately evenly and randomly into the buckets. Every id appears exactly once.

    Examples
    --------
    >>> from mt.concurrency import split_works
    >>> buckets = split_works(6, 3)
    >>> len(buckets)
    3
    >>> sorted(i for bucket in buckets for i in bucket)
    [0, 1, 2, 3, 4, 5]
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

    This function complements the :class:`WorkIterator` class to deal with cases when the number
    of works is small. The works run lazily, one per iteration step, in the calling process and
    thread.

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
        a Python generator yielding `func(work_id)` for `work_id` in `range(num_work_ids)`, in
        order

    Examples
    --------
    >>> from mt.concurrency import serial_work_generator
    >>> list(serial_work_generator(lambda i: (i, i * i), 3))
    [(0, 0), (1, 1), (2, 4)]
    """
    for work_id in range(num_work_ids):
        yield func(work_id)
