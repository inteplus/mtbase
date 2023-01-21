"""Utilities to work with the with statement."""


from mt import ctx

from .deprecated import deprecated_func


__all__ = ["DummyScopeForWithStatement", "dummy_scope"]


class DummyScopeForWithStatement(object):
    """Dummy scope for the with statement.

    Warning
    -------
    This class is deprecated as of 2021/09/08. Please do not use.

    >>> with dummy_scope:
    ...     a = 1

    """

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback_obj):
        pass


dummy_scope = ctx.nullcontext()
