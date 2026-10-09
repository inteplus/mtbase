"""Additional utitlities dealing with traceback.

Instead of:

.. code-block:: python

   import traceback

You do:

.. code-block:: python

   from mt import traceback

It will import the traceback package plus the additional stuff implemented here:
:func:`format_exc_info`, :func:`extract_stack_compact`, :class:`LogicError` and
:func:`extract_task_stacktrace`.

Please see Python package `traceback`_ for more details.

.. _traceback:
   https://docs.python.org/3/library/traceback.html
"""

import traceback as _tb
import asyncio
import linecache
from traceback import *


__all__ = [
    "format_exc_info",
    "extract_stack_compact",
    "LogicError",
    "extract_task_stacktrace",
]


def format_exc_info(exc_type, exc_value, exc_traceback):
    """Formats (exception type, exception value, traceback) into multiple lines.

    Parameters
    ----------
    exc_type : type
        the exception class
    exc_value : BaseException
        the exception instance
    exc_traceback : types.TracebackType
        the traceback of the exception

    Returns
    -------
    list of str
        the lines of the formatted exception, without line terminators (the last element is an
        empty string)

    Examples
    --------
    .. code-block:: python

        import sys
        try:
            1 / 0
        except Exception:
            lines = format_exc_info(*sys.exc_info())
        # lines[-2] == 'ZeroDivisionError: division by zero'
    """
    statements = _tb.format_exception(exc_type, exc_value, exc_traceback)
    statements = "".join(statements)
    return statements.split("\n")


def extract_stack_compact():
    """Returns the current callstack in a compact format.

    Returns
    -------
    list of str
        the lines of the formatted stack of the caller, like :func:`traceback.format_stack` split
        by newlines, with the lines mentioning "frozen importlib" removed
    """
    lines = _tb.format_list(_tb.extract_stack())
    lines = "".join(lines).split("\n")
    lines = [line for line in lines if "frozen importlib" not in line]
    return lines


class LogicError(RuntimeError):
    """An error in the logic, defined by a message and a debugging dictionary.

    As of 2024/07/07, the user can optionally provide the error that caused this error together
    with optionally the corresponding traceback (as a list of strings, each string respresenting
    one line without the carriage return symbol).

    The error is meant to be raised with a message and a dictionary of debugging values. Converting
    it to a string gives a multi-line report: the causing error with its traceback (if any), then
    the message, then a ``Where:`` section listing the debugging values.

    Parameters
    ----------
    msg : str
        the error message
    debug : dict
        a dictionary of debugging variables to show under ``Where:``
    causing_error : Exception, optional
        the error that caused this one. If provided, it is shown before the message.
    causing_traceback : list of str, optional
        the traceback of the causing error, one string per line. If not provided, the one attached
        to `causing_error` (if any) is used.

    Examples
    --------
    >>> from mt import traceback
    >>> e = traceback.LogicError("bad value", {"x": 1})
    >>> print(e)
    bad value
    Where:
      x: 1
    >>> e = traceback.LogicError("failed", causing_error=KeyError("k"), causing_traceback=["tb"])
    >>> print(e)
    With KeyError {
      Traceback:
      tb
      'k'
    } KeyError
    failed
    """

    def __init__(self, msg, debug={}, causing_error=None, causing_traceback=None):
        super().__init__(msg, debug, causing_error, causing_traceback)

    def __str__(self):
        l_lines = []

        causing_error = self.args[2]
        if causing_error:
            msg = f"With {type(causing_error).__name__}" + " {"
            l_lines.append(msg)

            causing_traceback = self.args[3]
            if causing_traceback is None:
                causing_traceback = causing_error.__traceback__
                if causing_traceback:
                    causing_traceback = _tb.format_tb(causing_traceback)
                    causing_traceback = "".join(causing_traceback).split("\n")
            if causing_traceback:
                l_lines.append("  Traceback:")
                for line in causing_traceback:
                    l_lines.append("  " + line)

            for line in str(causing_error).split("\n"):
                l_lines.append("  " + line)

            msg = "} " + f"{type(causing_error).__name__}"
            l_lines.append(msg)

        l_lines.append(f"{self.args[0]}")

        debug = self.args[1]
        if debug:
            l_lines.append("Where:")
            for k, v in debug.items():
                l_lines.append(f"  {k}: {v}")

        return "\n".join(l_lines)


def extract_task_stacktrace(task: asyncio.Task) -> list:
    """Extracts the stacktrace of an asyncio task.

    Parameters
    ----------
    task : asyncio.Task
       the task to extract from

    Returns
    -------
    list
        list of strings, each representing one row of the stacktrace of the task (as given by
        :meth:`asyncio.Task.get_stack`), formatted like :func:`traceback.format_list`
    """
    stacktrace = []
    checked = set()
    for f in task.get_stack():
        lineno = f.f_lineno
        co = f.f_code
        filename = co.co_filename
        name = co.co_name
        if filename not in checked:
            checked.add(filename)
            linecache.checkcache(filename)
        line = linecache.getline(filename, lineno, f.f_globals)
        stacktrace.append((filename, lineno, name, line))
    stacktrace = format_list(stacktrace)
    stacktrace = "".join(stacktrace).split("\n")
    return stacktrace
