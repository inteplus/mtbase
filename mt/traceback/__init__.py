"""Additional utitlities dealing with traceback.

Instead of:

.. code-block:: python

   import traceback

You do:

.. code-block:: python

   from mt import traceback

It will import the traceback package plus the additional stuff implemented here.

Please see Python package `traceback`_ for more details.

.. _traceback:
   https://docs.python.org/3/library/traceback.html
"""

import traceback as _tb
import asyncio
from traceback import *


__all__ = [
    "format_exc_info",
    "extract_stack_compact",
    "LogicError",
    "extract_task_stacktrace",
]


def format_exc_info(exc_type, exc_value, exc_traceback):
    """Formats (exception type, exception value, traceback) into multiple lines."""
    statements = _tb.format_exception(exc_type, exc_value, exc_traceback)
    statements = "".join(statements)
    return statements.split("\n")


def extract_stack_compact():
    """Returns the current callstack in a compact format."""
    lines = _tb.format_list(_tb.extract_stack())
    lines = "".join(lines).split("\n")
    lines = [line for line in lines if "frozen importlib" not in line]
    return lines


class LogicError(RuntimeError):
    """An error in the logic, defined by a message and a debugging dictionary.

    As of 2024/07/07, the user can optionally provide the error that caused this error together
    with optionally the corresponding traceback (as a list of strings, each string respresenting
    one line without the carriage return symbol).
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
        list or strings representing multiple rows representing the stacktrace
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
