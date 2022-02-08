"""Utilities to work with the with statement."""


from .contextlib import nullcontext

from .deprecated import deprecated_func


__all__ = ['DummyScopeForWithStatement', 'dummy_scope', 'join_scopes']


class DummyScopeForWithStatement(object):
    '''Dummy scope for the with statement.

    Warning
    -------
    This class is deprecated as of 2021/09/08. Please do not use.

    >>> with dummy_scope:
    ...     a = 1

    '''
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

dummy_scope = nullcontext()


@deprecated_func(deprecated_version="2021.1", suggested_func="(use built-in compound with)", docstring_prefix="    ")
class join_scopes(object):
    '''Joins two or more with statements into one.

    If you run into a situation in which you have nested with statements, for example::

    ```
    with a:
        with b:
            with c:
                statements
    ```

    you can do::

    ```
    with join_scopes(a,b,c):
        statements
    ```

    All positional arguments passed to the function are treated as a separate object to be used with the 'with' statement. The return value of the `__enter__`  method of the function is the list of return values of each argument's `__enter__` method.

    Notes
    -----
    As of 2021/08/01, MT has realised that the with statement in Python 3 has long supported compound withs. So you can do `with a, b, c:` without any issue. This class has become redundant. Duh!
    '''

    def __init__(self, *with_objs):
        self.with_objs = with_objs

    def __enter__(self):
        return [with_obj.__enter__() for with_obj in self.with_objs]

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None: # normal case
            for with_obj in reversed(self.with_objs): 
                with_obj.__exit__(exc_type, exc_value, traceback)
            return

        reraise = True
        for with_obj in reversed(self.with_objs): 
            if with_obj.__exit__(exc_type, exc_value, traceback):
                reraise = False
                exc_type, exc_value, traceback = None, None, None

        return None if reraise else True
