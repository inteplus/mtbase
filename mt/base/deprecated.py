"""Utilities to deprecate a function or a class."""

from functools import wraps

from mt import traceback
from mt.logg import logger


__all__ = ["deprecated_func", "deprecated_class"]


def deprecated_func(
    deprecated_version, suggested_func=None, removed_version=None, docstring_prefix=""
):
    """A decorator to warn the user that the function has been deprecated and will be removed
    in future.

    The first time the decorated function is called in a process, a warning is logged via
    :mod:`mt.logg` showing where it was called from, the deprecation version, and, if provided, the
    removal version and the suggested replacement. Later calls are silent. In addition, a Sphinx
    `.. deprecated::` directive (with the removal version and the suggested replacement, if any) is
    appended to the docstring of the function.

    Parameters
    ----------
    deprecated_version : str
        the version since which the function has been deprecated
    suggested_func : str or list of strings, optional
        the function (or functions) to be used in replacement of the deprecated function
    removed_version : str, optional
        the future version from which the deprecated function will be removed
    docstring_prefix : str
        prefix string to be inserted at the beginning of each line that the decorator appends to the
        docstring. Use it to indent the directive when the function is a method inside a class.

    Returns
    -------
    function
        a decorator to be applied on the function to deprecate

    See Also
    --------
    deprecated_class : the equivalent decorator for a class

    Examples
    --------
    >>> from mt.base.deprecated import deprecated_func
    >>> @deprecated_func("1.0", suggested_func="new_func", removed_version="2.0")
    ... def old_func(x):
    ...     '''Does something.'''
    ...     return x + 1
    ...
    >>> print(old_func.__doc__)
    Does something.
    <BLANKLINE>
    .. deprecated:: 1.0
       It will be removed in version 2.0.
       Use :func:`new_func` instead.
    <BLANKLINE>
    """

    def deprecated_decorator(func):
        """Wraps `func` to warn on first use and amends its docstring."""

        @wraps(func)
        def func_wrapper(*args, **kwds):
            """Warns once (see :func:`deprecated_func`) and then calls the function."""
            if not deprecated_func_warned[func]:
                lines = traceback.extract_stack_compact()
                if len(lines) > 7:
                    logger.warn(
                        f"IMPORT: Deprecated function '{func.__name__}' invoked at:"
                    )
                    for x in lines[-7:-5]:
                        logger.warn(x)
                    logger.warn(
                        f"  It has been deprecated since version {deprecated_version}."
                    )
                else:
                    logger.warn(
                        f"IMPORT: Function {func.__name__} has been deprecated since version "
                        f"{deprecated_version}."
                    )
                if removed_version:
                    logger.warn(f"  It will be removed in version {removed_version}.")
                if suggested_func:
                    if isinstance(suggested_func, str):
                        logger.warn(f"  Use function '{suggested_func}' instead.")
                    else:
                        logger.warn(f"  Use a function in {suggested_func} instead.")
                deprecated_func_warned[func] = True
            return func(*args, **kwds)

        deprecated_func_warned[func] = False  # register the function

        the_doc = func_wrapper.__doc__

        msg = f"{docstring_prefix}.. deprecated:: {deprecated_version}\n"
        if not the_doc or len(the_doc) == 0:
            the_doc = msg
        else:
            if the_doc[-1] != "\n":
                the_doc += "\n"
            the_doc += "\n" + msg

        if removed_version:
            the_doc += f"{docstring_prefix}   It will be removed in version {removed_version}.\n"

        if suggested_func:
            if isinstance(suggested_func, str):
                msg = f":func:`{suggested_func}`"
            else:
                msg = " or ".join([f":func:`{x}`" for x in suggested_func])
            the_doc += f"{docstring_prefix}   Use {msg} instead.\n"

        func_wrapper.__doc__ = the_doc
        return func_wrapper

    return deprecated_decorator


# map: deprecated function -> warned
deprecated_func_warned = {}


def deprecated_class(
    deprecated_version, suggested_class=None, removed_version=None, docstring_prefix=""
):
    """A decorator to warn the user that the class has been deprecated and will be removed
    in future.

    The first time an instance of the decorated class is created in a process, a warning is logged
    via :mod:`mt.logg` showing where it was created from, the deprecation version, and, if provided,
    the removal version and the suggested replacement. Later instantiations are silent. In
    addition, a Sphinx `.. deprecated::` directive (with the removal version and the suggested
    replacement, if any) is appended to the docstring of the class.

    Parameters
    ----------
    deprecated_version : str
        the version since which the class has been deprecated
    suggested_class : str or list of strings, optional
        the class (or classes) to be used in replacement of the deprecated class
    removed_version : str, optional
        the future version from which the class will be removed
    docstring_prefix : str
        prefix string to be inserted at the beginning of each line that the decorator appends to the
        docstring

    Returns
    -------
    function
        a decorator to be applied on the class to deprecate. The class is modified in place (its
        `__init__` and `__doc__` are replaced) and returned.

    See Also
    --------
    deprecated_func : the equivalent decorator for a function

    Examples
    --------
    >>> from mt.base.deprecated import deprecated_class
    >>> @deprecated_class("1.0", suggested_class=["NewA", "NewB"])
    ... class Old:
    ...     '''An old class.'''
    ...
    >>> print(Old.__doc__)
    An old class.
    <BLANKLINE>
    .. deprecated:: 1.0
       Use :class:`NewA` or :class:`NewB` instead.
    <BLANKLINE>
    """

    def deprecated_decorator(cls):
        """Patches `cls` to warn on first instantiation and amends its docstring."""
        cls._warn_of_class_deprecation = False
        the_init = cls.__init__

        @wraps(the_init)
        def new_init(self, *args, **kwds):
            """Warns once (see :func:`deprecated_class`) and then runs the original `__init__`."""
            if not self.__class__._warn_of_class_deprecation:
                self.__class__._warn_of_class_deprecation = True

                lines = traceback.extract_stack_compact()
                if len(lines) > 7:
                    logger.warn(
                        f"IMPORT: Deprecated class '{cls.__name__}' invoked at:"
                    )
                    for x in lines[-7:-5]:
                        logger.warn(x)
                    logger.warn(
                        f"  It has been deprecated since version {deprecated_version}."
                    )
                else:
                    logger.warn(
                        f"IMPORT: Class {cls.__name__} has been deprecated since version "
                        f"{deprecated_version}."
                    )
                if removed_version:
                    logger.warn(f"  It will be removed in version {removed_version}.")
                if suggested_class:
                    if isinstance(suggested_class, str):
                        logger.warn(f"  Use class '{suggested_class}' instead.")
                    else:
                        logger.warn(f"  Use a class in {suggested_class} instead.")

            return the_init(self, *args, **kwds)

        cls.__init__ = new_init

        the_doc = cls.__doc__

        msg = f"{docstring_prefix}.. deprecated:: {deprecated_version}\n"
        if not the_doc or len(the_doc) == 0:
            the_doc = msg
        else:
            if the_doc[-1] != "\n":
                the_doc += "\n"
            the_doc += "\n" + msg

        if removed_version:
            the_doc += f"{docstring_prefix}   It will be removed in version {removed_version}.\n"

        if suggested_class:
            if isinstance(suggested_class, str):
                msg = f":class:`{suggested_class}`"
            else:
                msg = " or ".join([f":class:`{x}`" for x in suggested_class])
            the_doc += f"{docstring_prefix}   Use {msg} instead.\n"

        cls.__doc__ = the_doc
        return cls

    return deprecated_decorator
