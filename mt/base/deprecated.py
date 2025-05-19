"""Utilities to deprecate a function or a module."""

from functools import wraps

from mt import traceback
from mt.logg import logger


__all__ = ["deprecated_func", "deprecated_class"]


def deprecated_func(
    deprecated_version, suggested_func=None, removed_version=None, docstring_prefix=""
):
    """A decorator to warn the user that the function has been deprecated and will be removed in future.

    Parameters
    ----------
    deprecated_version : str
        the version since which the function has been deprecated
    suggested_func : str or list of strings, optional
        the function to be used in replacement of the deprecated function
    removed_version : str, optional
        the future version from which the deprecated function will be removed
    docstring_prefix : str
        prefix string to be inserted at the beginning of every new line in the docstring
    """

    def deprecated_decorator(func):
        @wraps(func)
        def func_wrapper(*args, **kwargs):
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
                        f"IMPORT: Function {func.__name__} has been deprecated since version {deprecated_version}."
                    )
                if removed_version:
                    logger.warn(
                        f"  It will be removed in version {removed_version}."
                    )
                if suggested_func:
                    if isinstance(suggested_func, str):
                        logger.warn(
                            f"  Use function '{suggested_func}' instead."
                        )
                    else:
                        logger.warn(
                            f"  Use a function in {suggested_func} instead."
                        )
                deprecated_func_warned[func] = True
            return func(*args, **kwargs)

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
    """A decorator to warn the user that the class has been deprecated and will be removed in future.

    Parameters
    ----------
    deprecated_version : str
        the version since which the class has been deprecated
    suggested_class : str or list of strings, optional
        the class to be used in replacement of the deprecated class
    removed_version : str, optional
        the future version from which the class will be removed
    docstring_prefix : str
        prefix string to be inserted at the beginning of every new line in the docstring
    """

    def deprecated_decorator(cls):
        cls._warn_of_class_deprecation = False
        the_init = cls.__init__

        @wraps(the_init)
        def new_init(self, *args, **kwargs):
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
                        f"IMPORT: Class {cls.__name__} has been deprecated since version {deprecated_version}."
                    )
                if removed_version:
                    logger.warn(
                        f"  It will be removed in version {removed_version}."
                    )
                if suggested_class:
                    if isinstance(suggested_class, str):
                        logger.warn(
                            f"  Use class '{suggested_class}' instead."
                        )
                    else:
                        logger.warn(
                            f"  Use a class in {suggested_class} instead."
                        )

            return the_init(self, *args, **kwargs)

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
