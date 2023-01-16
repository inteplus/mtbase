"""Additional utitlities dealing with threading.

Instead of:

.. code-block:: python

   import threading

You do:

.. code-block:: python

   from mt import threading

It will import the threading package plus the additional stuff implemented here.

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
    """A lock object that allows many simultaneous "read locks", but
    only one "write lock." """

    def __init__(self, withPromotion=False):
        self._read_ready = Condition(RLock())
        self._readers = 0
        self._writers = 0
        self._promote = withPromotion
        self._readerList = []  # List of Reader thread IDs
        self._writerList = []  # List of Writer thread IDs

    def acquire_read(self):
        # logging.debug("RWL : acquire_read()")
        """Acquire a read lock. Blocks only if a thread has
        acquired the write lock."""
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
        """Release a read lock."""
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
        """Acquire a write lock. Blocks until there are no
        acquired read or write locks."""
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
        """Release a write lock."""
        from threading import get_ident

        self._writers -= 1
        self._writerList.remove(get_ident())
        self._read_ready.notifyAll()
        self._read_ready.release()

    def is_free(self):
        """Returns whether there is no reader and no writer."""
        return not self._readers and not self._writers


# ----------------------------------------------------------------------------------------------------------


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


# ----------------------------------------------------------------------------------------------------------


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


# ----------------------------------------------------------------------------------------------------------


class LockedIterator(object):
    """An wrapper that locks the input iterator to make sure it is thread-safe."""

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


# ----------------------------------------------------------------------------------------------------------
