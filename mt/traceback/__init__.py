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
from traceback import *


__all__ = ["format_exc_info", "extract_stack_compact", "LogicError"]


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
    with optionally the corresponding traceback.
    """

    def __init__(self, msg, debug={}, causing_error=None, causing_traceback=None):
        super().__init__(msg, debug, causing_error)

    def __str__(self):
        l_lines = []

        causing_error = self.args[2]
        if causing_error:
            msg = f"With the following {type(causing_error).__name__}:"
            l_lines.append(msg)
            if causing_traceback is None:
                causing_stacktrace = causing_error.__traceback__
                if causing_stacktrace:
                    causing_stacktrace = _tb.format_tb(causing_stacktrace)
            else:
                causing_stacktrace = causing_traceback
                if causing_stacktrace:
                    causing_stacktrace = _tb.format_list(causing_stacktrace)
            if causing_stacktrace:
                causing_stacktrace = "".join(causing_stacktrace).split("\n")
                l_lines.append("  Stack trace:")
                for line in causing_stacktrace:
                    l_lines.append("  " + line)
            for line in str(causing_error).split("\n"):
                l_lines.append("  " + line)

        l_lines.append(f"{self.args[0]}")
        debug = self.args[1]
        if debug:
            l_lines.append("Debugging dictionary:")
            for k, v in debug.items():
                l_lines.append(f"  {k}: {v}")
        return "\n".join(l_lines)
