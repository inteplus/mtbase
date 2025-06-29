"""Exec utils."""

import sys
import pdb
import functools
import threading as _t
from mt.logg import logger


def debug_exec(func, *args, **kwds):
    """Executes a function with trials.

    This function executes a function. However, when an exception is raised in the function, asks
    the user to invoke an IPython interactive shell or execute a Python command and retry. If the
    user declines, it passes the exception up in the call stack.

    Parameters
    ----------
    func : function
        function to be executed
    *args : list
        positional arguments of the function
    **kwds : dict
        keyword arguments of the function

    Returns
    -------
    whatever the function returns
    """
    user_data = {
        "func": func,
        "args": args,
        "kwds": kwds,
    }
    while True:
        try:
            return (user_data["func"])(*(user_data["args"]), **(user_data["kwds"]))
            break
        except:
            if _t.current_thread().__class__.__name__ != "_MainThread":
                raise  #  we only deal with main thread

            logger.warn_last_exception()
            try:
                from IPython.terminal.embed import InteractiveShellEmbed
            except:
                InteractiveShellEmbed = None

            if InteractiveShellEmbed:
                cmd = input(
                    "To debug in IPython shell, enter 'y'. Otherwise, press ENTER to continue: "
                )
                if len(cmd) == 0:
                    raise
                InteractiveShellEmbed()()
            else:
                cmd = input(
                    "Enter a Python command to be executed before we retry, or press ENTER to continue: "
                )
                if len(cmd) == 0:
                    raise
                exec(cmd)


def debug_on(*exceptions):
    """A decorator that uses pdb to inspect the cause of an unhandled exception.

    >>> @debug_on(TypeError)
    ... def buggy_function():
    ...    raise TypeError
    """

    if not exceptions:
        exceptions = (AssertionError,)

    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwds):
            try:
                return f(*args, **kwds)
            except exceptions:
                pdb.post_mortem(sys.exc_info()[2])

        return wrapper

    return decorator
