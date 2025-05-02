"""Monkey-patching contextlib.

For python 3.6 or earlier, this module corresponds to package `contextlib2`_. Otherwise, it
corresponds to package `contextlib`_.

Instead of:

.. code-block:: python

   import contextlib

or

.. code-block:: python

   import contextlib2

depending on which Python version you are using, you do:

.. code-block:: python

   from mt import ctx

It will import the right contextlib package.

Please see Python packages `contextlib`_ for more details.

.. _contextlib:
   https://docs.python.org/3/library/contextlib.html
.. _contextlib2:
   https://contextlib2.readthedocs.io/en/stable/
"""

from packaging.version import Version
import platform

if Version(platform.python_version()) < Version("3.7"):
    from contextlib2 import *
else:
    from contextlib import *

if Version(platform.python_version()) < Version("3.10"):
    _nullcontext = nullcontext

    class nullcontext(_nullcontext):
        """Context manager that does no additional processing.

        Used as a stand-in for a normal context manager, when a particular
        block of code is only sometimes used with a normal context manager:

        cm = optional_cm if condition else nullcontext()
        with cm:
            # Perform operation, using optional_cm if condition is True
        """

        async def __aenter__(self):
            return self.enter_result

        async def __aexit__(self, *excinfo):
            pass
