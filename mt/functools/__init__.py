"""Additional `functools`_ stuff.

Instead of:

.. code-block:: python

   import functools

You do:

.. code-block:: python

   from mt import functools

It will import functools plus the additional stuff implemented here.

Please see Python package `functools`_ for more details.

.. _functools:
   https://docs.python.org/3/library/functools.html
"""

from functools import *


################################################################################
### rpartial() argument application
################################################################################


# Purely functional, no descriptor behaviour
class rpartial(partial):
    """New function with rpartial application of the given arguments
    and keywords.
    """

    __slots__ = "func", "args", "keywords", "__dict__", "__weakref__"

    def __new__(cls, func, /, *args, **keywords):
        if not callable(func):
            raise TypeError("the first argument must be callable")

        if hasattr(func, "func"):
            args = args + func.args
            keywords = {**func.keywords, **keywords}
            func = func.func

        self = super(rpartial, cls).__new__(cls)

        self.func = func
        self.args = args
        self.keywords = keywords
        return self

    def __call__(self, /, *args, **keywords):
        keywords = {**self.keywords, **keywords}
        return self.func(*args, *self.args, **keywords)


# Descriptor version
class rpartialmethod(partialmethod):
    """Method descriptor with rpartial application of the given arguments
    and keywords.

    Supports wrapping existing descriptors and handles non-descriptor
    callables as instance methods.
    """

    def __init__(self, func, /, *args, **keywords):
        if not callable(func) and not hasattr(func, "__get__"):
            raise TypeError("{!r} is not callable or a descriptor".format(func))

        # func could be a descriptor like classmethod which isn't callable,
        # so we can't inherit from rpartial (it verifies func is callable)
        if isinstance(func, rpartialmethod):
            # flattening is mandatory in order to place cls/self before all
            # other arguments
            # it's also more efficient since only one function will be called
            self.func = func.func
            self.args = args + func.args
            self.keywords = {**func.keywords, **keywords}
        else:
            self.func = func
            self.args = args
            self.keywords = keywords

    def _make_unbound_method(self):
        def _method(cls_or_self, /, *args, **keywords):
            keywords = {**self.keywords, **keywords}
            return self.func(cls_or_self, *self.args, *args, **keywords)

        _method.__isabstractmethod__ = self.__isabstractmethod__
        _method._rpartialmethod = self
        return _method

    def __get__(self, obj, cls=None):
        get = getattr(self.func, "__get__", None)
        result = None
        if get is not None:
            new_func = get(obj, cls)
            if new_func is not self.func:
                # Assume __get__ returning something new indicates the
                # creation of an appropriate callable
                result = rpartial(new_func, *self.args, **self.keywords)
                try:
                    result.__self__ = new_func.__self__
                except AttributeError:
                    pass
        if result is None:
            # If the underlying descriptor didn't do anything, treat this
            # like an instance method
            result = self._make_unbound_method().__get__(obj, cls)
        return result
