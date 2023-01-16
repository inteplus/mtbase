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

from packaging import version
import platform

if version.parse(platform.python_version()) < version.parse("3.7"):
    from contextlib2 import *
else:
    from contextlib import *
