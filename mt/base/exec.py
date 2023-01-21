"""Exec utils."""


import threading as _t
from mt.logg import logger


__all__ = ["debug_exec"]


def debug_exec(func, *args, **kwargs):
    """Executes a function with trials.

    This function executes a function. However, when an exception is raised in the function, asks the user to invoke an IPython interactive shell or execute a Python command and retry. If the user declines, it passes the exception up in the call stack.

    Parameters
    ----------
    func : function
        function to be executed
    args : list
        positional arguments of the function
    kwargs : dict
        keyword arguments of the function

    Returns
    -------
    whatever the function returns
    """
    user_data = {
        "func": func,
        "args": args,
        "kwargs": kwargs,
    }
    while True:
        try:
            return (user_data["func"])(*(user_data["args"]), **(user_data["kwargs"]))
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
