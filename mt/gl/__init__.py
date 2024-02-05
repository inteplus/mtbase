"""Alias module of `OpenGL.GL`_ of `pyopengl`.

Instead of:

.. code-block:: python

   import OpenGL.GL as gl

You do:

.. code-block:: python

   from mt import gl

It will import the OpenGL.GL package of `pyopengl` and provide some useful functions.

Please see Python package `pyopengl`_ for more details.

.. _pyopengl:
   https://pyopengl.sourceforge.net/
"""


try:
    import OpenGL.GL as gl
except ImportError:
    raise ImportError("Alias 'mt.gl' requires package 'pyglfw' be installed.")

import abc

from OpenGL.GL import *


# ----- utilities -----


class Base(abc.ABC):
    def __init__(self, obj):
        self.obj = obj

    @abc.abstractmethod
    def bind(self):
        pass

    @abc.abstractclassmethod
    def unbind(self):
        pass

    def __enter__(self):
        self.bind()
        return self.obj

    def __exit__(self, type, value, traceback_obj):
        self.unbind()


class VAO(Base):
    """A single vertex array object (VAO).

    The class owns and manages the VAO.

    Examples
    --------
    >>> from mt import gl
    >>> vao = gl.VAO()
    >>> with vao:
    >>>     pass # do something while the VAO is binded
    """

    def __init__(self):
        super().__init__(gl.glGenVertexArrays(1))

    def bind(self):
        gl.glBindVertexArray(self.obj)

    def unbind(self):
        gl.glBindVertexArray(0)

    def __del__(self):
        gl.glDeleteVertexArrays(1, [self.obj])


class VBO(Base):
    """A single vertex buffer object (VBO).

    The class owns and manages the VBO.

    Examples
    --------
    >>> from mt import gl
    >>> vbo = gl.VBO()
    >>> with vbo:
    >>>     pass # do something while the VBO is binded
    """

    def __init__(self):
        super().__init__(gl.glGenBuffers(1))

    def bind(self):
        gl.glBindBuffer(gl.GL_ARRAY_BUFFER, self.obj)

    def unbind(self):
        gl.glBindBuffer(gl.GL_ARRAY_BUFFER, 0)

    def __del__(self):
        gl.glDeleteBuffers(1, [self.obj])
