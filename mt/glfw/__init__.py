"""Alias module of `glfw`_.

Instead of:

.. code-block:: python

   import glfw

You do:

.. code-block:: python

   from mt import glfw

It will import the glfw package and make sure you never have to invoke :func:`glfw.terminate`.

Please see Python package `pyglfw`_ for more details.

.. _glfw:
   https://pypi.org/project/pyglfw/
"""


import atexit as _ex


try:
    import glfw
    from glfw import *
except ImportError:
    raise ImportError("Alias 'mt.glfw' requires package 'pyglfw' be installed.")


# exit function
def __exit_module():
    glfw.terminate()


_ex.register(__exit_module)
