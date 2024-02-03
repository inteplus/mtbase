"""Alias module of `glfw`_.

Instead of:

.. code-block:: python

   import glfw

You do:

.. code-block:: python

   from mt import glfw

It will import the glfw package and make sure that you never have to invoke :func:`glfw.terminate`
and that windows and cursors can be created in a pythonic way using functions with the 'scoped_'
prefix.

Please see Python package `pyglfw`_ for more details.

.. _glfw:
   https://pypi.org/project/pyglfw/
"""


import atexit as _ex


try:
    import glfw
except ImportError:
    raise ImportError("Alias 'mt.glfw' requires package 'pyglfw' be installed.")

from glfw import *


# ----- initialisation -----


if not glfw.init():
    raise ImportError("Unable to initialise glfw.")


class Base:
    def __init__(self, pointer, del_func):
        self.pointer = pointer
        self.del_func = del_func

    def __enter__(self):
        return self.pointer

    def __exit__(self, type, value, traceback_obj):
        if self.pointer:
            self.del_func(self.pointer)


# ----- scoped window -----


class Window(Base):
    def __init__(self, pointer):
        super().__init__(pointer, glfw.destroy_window)


def scoped_create_window(width: int, height: int, title: str, monitor, share) -> Window:
    return Window(glfw.create_window(width, height, title, monitor, share))


scoped_create_window.__doc__ = "Scoped-" + glfw.create_window.__doc__


# ----- scoped cursor -----


class Cursor(Base):
    def __init__(self, pointer):
        super().__init__(pointer, glfw.destroy_cursor)


def scoped_create_cursor(image, xhot: int, yhot: int) -> Cursor:
    return Cursor(glfw.create_cursor(image, xhot, yhot))


scoped_create_cursor.__doc__ = "Scoped-" + glfw.create_cursor.__doc__


def scoped_create_standard_cursor(shape: int) -> Cursor:
    return Cursor(glfw.create_standard_cursor(shape))


scoped_create_standard_cursor.__doc__ = "Scoped-" + glfw.create_standard_cursor.__doc__


# ----- cleaning up -----


# exit function
def __exit_module():
    glfw.terminate()


_ex.register(__exit_module)
