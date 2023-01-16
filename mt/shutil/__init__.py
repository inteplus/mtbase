"""Additional `shutil`_ stuff related to the terminal.

Instead of:

.. code-block:: python

   import shutil

You do:

.. code-block:: python

   from mt import shutil

It will import shutil plus the additional stuff implemented here.

Please see Python package `shutil`_ for more details.

.. _shutil:
   https://docs.python.org/3/library/shutil.html
"""


from shutil import *


__all__ = ["stty_imgres", "stty_size"]


def stty_size():
    """Gets the terminal size.

    Returns the Linux-compatible console's number of rows and number of characters per
    row. If the information does not exist, returns (72, 128)."""

    res = get_terminal_size(fallback=(128, 72))
    return res[1], res[0]


def stty_imgres():
    """Gets the terminal resolution.

    Returns the Linux-compatible console's number of letters per row and the number of
    rows. If the information does not exist, returns (128, 72)."""

    res = get_terminal_size(fallback=(128, 72))
    return [res[0], res[1]]
