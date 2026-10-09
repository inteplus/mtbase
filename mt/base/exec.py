"""Exec utils: helpers to retry or post-mortem debug a function that raises."""

import sys
import pdb
import functools
import threading as _t
from mt.logg import logger


def debug_exec(func, *args, **kwds):
    """Executes a function with trials.

    This function executes a function. However, when an exception is raised in the function, it
    logs the exception and asks the user, via the terminal, to invoke an IPython interactive shell
    (if IPython is installed) or to execute a Python command, and then retries the function. If the
    user declines by pressing ENTER, it passes the exception up in the call stack. Outside the main
    thread, no interaction is possible and the exception is simply re-raised.

    Since it blocks on `input()`, do not use it in non-interactive jobs.

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
    object
        whatever the function returns, from the first successful attempt

    See Also
    --------
    debug_on : a decorator that opens a pdb post-mortem session instead of retrying
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
                    "Enter a Python command to be executed before we retry, "
                    "or press ENTER to continue: "
                )
                if len(cmd) == 0:
                    raise
                exec(cmd)


def debug_on(*exceptions):
    """A decorator that uses pdb to inspect the cause of an unhandled exception.

    When the decorated function raises one of the given exception types, a :mod:`pdb` post-mortem
    session is started at the point of the exception. After the session ends the exception is
    swallowed and the decorated function returns None. Other exceptions propagate unchanged.

    Parameters
    ----------
    *exceptions : type
        exception classes to catch. If none is given, only :class:`AssertionError` is caught.

    Returns
    -------
    function
        a decorator that wraps a function (keeping its name and docstring)

    Examples
    --------
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
