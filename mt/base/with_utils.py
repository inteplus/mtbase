"""Utilities to work with the with statement."""


__all__ = ['DummyScopeForWithStatement', 'dummy_scope', 'join_scopes']


class DummyScopeForWithStatement(object):
    '''Dummy scope for the with statement.

    >>> with dummy_scope:
    ...     a = 1

    '''
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        pass

dummy_scope = DummyScopeForWithStatement()


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
    '''

    def __init__(self, *with_objs): 
        self.with_objs = with_objs 

    def __enter__(self): 
        for with_obj in self.with_objs: 
            with_obj.__enter__() 

    def __exit__(self, type, value, traceback): 
        for with_obj in reversed(self.with_objs): 
            with_obj.__exit__(type, value, traceback) 
