'''Utilities to deprecate a function or a module.'''

from functools import wraps

from .logging import logger
from .traceback import extract_stack_compact


__all__ = ['deprecated_func']


def deprecated_func(deprecated_version, suggested_func=None, removed_version=None, docstring_prefix=""):
    '''A decorator to warn the user that the function has been deprecated and will be removed in future.

    Parameters
    ----------
    deprecated_version : str
        the version since which the function has been deprecated
    suggested_func : str, optional
        the function to be used in replacement of the deprecated function
    removed_version : str, optional
        the future version from which the function will be removed
    docstring_prefix : str
        prefix string to be inserted at the beginning of every new line in the docstring
    '''

    def deprecated_decorator(func):
        @wraps(func)
        def func_wrapper(*args, **kwargs):
            if not deprecated_func_warned[func]:
                lines = extract_stack_compact()
                if len(lines) > 7:
                    logger.warn("IMPORT: Deprecated function '{}' invoked at:".format(func.__name__))
                    for x in lines[-7:-5]:
                        logger.warn(x)
                    logger.warn("  It has been deprecated since version {}.".format(deprecated_version))
                else:
                    logger.warn("IMPORT: Function {} has been deprecated since version {}.".format(func.__name__, deprecated_version))
                if removed_version:
                    logger.warn("  It will be removed in version {}.".format(removed_version))
                if suggested_func:
                    logger.warn("  Use function '{}' instead.".format(suggested_func))
                deprecated_func_warned[func] = True
            return func(*args, **kwargs)

        deprecated_func_warned[func] = False # register the function

        the_doc = func_wrapper.__doc__
        if not the_doc:
            the_doc = ""

        the_doc += "\n{}.. deprecated:: {}\n".format(docstring_prefix, deprecated_version)

        if removed_version:
            the_doc += "{}   It will be removed in version {}.\n".format(docstring_prefix, removed_version)

        if suggested_func:
            the_doc += "{}   Use :func:`{}` instead.\n".format(docstring_prefix, suggested_func)

        func_wrapper.__doc__ = the_doc
        return func_wrapper

    return deprecated_decorator


# map: deprecated function -> warned
deprecated_func_warned = {}
