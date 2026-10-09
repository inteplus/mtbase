"""Additional utitlities dealing with threading.

Instead of:

.. code-block:: python

   import threading

You do:

.. code-block:: python

   from mt import threading

It will import the threading package plus the additional stuff implemented here:
:class:`ReadWriteLock` (with the context-manager helpers :class:`ReadRWLock` and
:class:`WriteRWLock`) and :class:`LockedIterator`.

Please see Python package `threading`_ for more details.

.. _threading:
   https://docs.python.org/3/library/threading.html
"""

# From O'Reilly Python Cookbook by David Ascher, Alex Martelli
# With changes to cover the starvation situation where a continuous
#   stream of readers may starve a writer, Lock Promotion and Context Managers

from threading import *


__all__ = ["ReadWriteLock", "ReadRWLock", "WriteRWLock", "LockedIterator"]


class ReadWriteLock:
    """A lock object that allows many simultaneous "read locks", but only one "write lock".

    Readers block only while a writer holds the lock; a writer waits until all readers have
    released. Writers are counted as soon as they request the lock, so new readers cannot starve
    a waiting writer. The write lock is re-entrant for the same thread.

    Prefer the context managers :class:`ReadRWLock` and :class:`WriteRWLock` (each constructed
    with the :class:`ReadWriteLock` as its only argument, acquiring on entry and releasing on exit),
    which guarantee the release, over calling the ``acquire_*``/``release_*`` methods directly.

    Parameters
    ----------
    withPromotion : bool
        whether a thread that holds a read lock may request a write lock ("lock promotion"). It is
        allowed only if every current reader is itself requesting the write lock. Otherwise the
        writer waits for the other readers to finish, which can deadlock if the promoting thread
        still holds its own read lock.

    Examples
    --------
    >>> from mt import threading
    >>> lock = threading.ReadWriteLock()
    >>> with threading.ReadRWLock(lock):
    ...     with threading.ReadRWLock(lock):  # many readers are fine
    ...         print(lock.is_free())
    False
    >>> with threading.WriteRWLock(lock):
    ...     print(lock.is_free())
    False
    >>> lock.is_free()
    True
    """

    def __init__(self, withPromotion=False):
        self._read_ready = Condition(RLock())
        self._readers = 0
        self._writers = 0
        self._promote = withPromotion
        self._readerList = []  # List of Reader thread IDs
        self._writerList = []  # List of Writer thread IDs

    def acquire_read(self):
        # logging.debug("RWL : acquire_read()")
        """Acquires a read lock. Blocks only if a thread has acquired the write lock.

        Every call must be paired with a call to :meth:`release_read` from the same thread.
        """
        self._read_ready.acquire()
        try:
            while self._writers > 0:
                self._read_ready.wait()
            self._readers += 1
        finally:
            from threading import get_ident

            self._readerList.append(get_ident())
            self._read_ready.release()

    def release_read(self):
        # logging.debug("RWL : release_read()")
        """Releases a read lock previously acquired by the calling thread.

        Wakes up waiting writers when the last reader leaves.
        """
        self._read_ready.acquire()
        try:
            self._readers -= 1
            if not self._readers:
                self._read_ready.notifyAll()
        finally:
            from threading import get_ident

            self._readerList.remove(get_ident())
            self._read_ready.release()

    def acquire_write(self):
        # logging.debug("RWL : acquire_write()")
        """Acquires a write lock. Blocks until there are no acquired read or write locks.

        The calling thread holds the lock until it calls :meth:`release_write`. With lock
        promotion enabled (see the constructor), a thread that currently holds a read lock may
        call this method too.
        """
        from threading import get_ident

        self._read_ready.acquire()  # A re-entrant lock lets a thread re-acquire the lock
        self._writers += 1
        self._writerList.append(get_ident())
        while self._readers > 0:
            # promote to write lock, only if all the readers are trying to promote to writer
            # If there are other reader threads, then wait till they complete reading
            if (
                self._promote
                and get_ident() in self._readerList
                and set(self._readerList).issubset(set(self._writerList))
            ):
                break
            else:
                self._read_ready.wait()

    def release_write(self):
        # logging.debug("RWL : release_write()")
        """Releases a write lock previously acquired by the calling thread.

        Wakes up all waiting threads.
        """
        from threading import get_ident

        self._writers -= 1
        self._writerList.remove(get_ident())
        self._read_ready.notifyAll()
        self._read_ready.release()

    def is_free(self):
        """Returns whether there is no reader and no writer.

        Returns
        -------
        bool
            True if no thread currently holds or requests a read or write lock
        """
        return not self._readers and not self._writers


# --------------------------------------------------------------------------------------------------


class ReadRWLock:
    # Context Manager class for ReadWriteLock
    def __init__(self, rwLock):
        self.rwLock = rwLock

    def __enter__(self):
        self.rwLock.acquire_read()
        return self  # Not mandatory, but returning to be safe

    def __exit__(self, exc_type, exc_value, traceback_obj):
        self.rwLock.release_read()
        return False  # Raise the exception, if exited due to an exception


# --------------------------------------------------------------------------------------------------


class WriteRWLock:
    # Context Manager class for ReadWriteLock
    def __init__(self, rwLock):
        self.rwLock = rwLock

    def __enter__(self):
        self.rwLock.acquire_write()
        return self  # Not mandatory, but returning to be safe

    def __exit__(self, exc_type, exc_value, traceback_obj):
        self.rwLock.release_write()
        return False  # Raise the exception, if exited due to an exception


# --------------------------------------------------------------------------------------------------


class LockedIterator(object):
    """A wrapper that locks the input iterator to make sure it is thread-safe.

    Each call to ``next`` is serialised by a lock, so several threads can safely consume the same
    iterator (each item is delivered to exactly one thread).

    Parameters
    ----------
    it : iterable
        the iterable to wrap; ``iter(it)`` is taken once at construction

    Examples
    --------
    >>> from mt import threading
    >>> it = threading.LockedIterator([1, 2, 3])
    >>> list(it)
    [1, 2, 3]
    """

    def __init__(self, it):
        self.lock = Lock()
        self.it = it.__iter__()

    def __iter__(self):
        return self

    def __next__(self):
        with self.lock:
            return next(self.it)

    def next(self):
        return self.__next__()


# --------------------------------------------------------------------------------------------------
