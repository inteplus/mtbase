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
    import OpenGL.GL.shaders
    import OpenGL.GL.ARB
except ImportError:
    raise ImportError("Alias 'mt.gl' requires package 'pyopengl' be installed.")

import abc
import glm
import numpy as np
from OpenGL.GL import *


# ----- utilities -----


class Base(abc.ABC):
    def __init__(self, obj):
        self.obj = obj
        self._context_level = 0

    @abc.abstractmethod
    def bind(self):
        pass

    @abc.abstractclassmethod
    def unbind(self):
        pass

    def __enter__(self):
        if self._context_level == 0:
            self.bind()
        self._context_level += 1
        return self.obj

    def __exit__(self, type, value, traceback_obj):
        self._context_level -= 1
        if self._context_level == 0:
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


def glElementType(cls):
    if cls in (
        np.float32,
        glm.float32,
        glm.vec1,
        glm.vec2,
        glm.vec3,
        glm.vec4,
        glm.mat2,
        glm.mat3,
        glm.mat4,
    ):
        return gl.GL_FLOAT
    if cls in (
        np.float64,
        glm.float64,
        glm.dvec1,
        glm.dvec2,
        glm.dvec3,
        glm.dvec4,
        glm.dmat2,
        glm.dmat3,
        glm.dmat4,
    ):
        return gl.GL_DOUBLE
    if cls in (np.int8, glm.i8vec1, glm.i8vec2, glm.i8vec3, glm.i8vec4):
        return gl.GL_BYTE
    if cls in (np.uint8, glm.u8vec1, glm.u8vec2, glm.u8vec3, glm.u8vec4):
        return gl.GL_UNSIGNED_BYTE
    if cls in (np.int16, glm.i16vec1, glm.i16vec2, glm.i16vec3, glm.i16vec4):
        return gl.GL_SHORT
    if cls in (np.uint16, glm.u16vec1, glm.u16vec2, glm.u16vec3, glm.u16vec4):
        return gl.GL_UNSIGNED_SHORT
    if cls in (
        np.int32,
        glm.ivec1,
        glm.ivec2,
        glm.ivec3,
        glm.ivec4,
        glm.imat2,
        glm.imat3,
        glm.imat4,
    ):
        return gl.GL_INT
    if cls in (
        np.uint32,
        glm.uvec1,
        glm.uvec2,
        glm.uvec3,
        glm.uvec4,
        glm.umat2,
        glm.umat3,
        glm.umat4,
    ):
        return gl.GL_UNSIGNED_INT
    raise NotImplementedError


def glElementSize(cls):
    if cls in (
        np.int8,
        np.uint8,
        np.int16,
        np.uint16,
        np.float32,
        np.float64,
        glm.int8,
        glm.uint8,
        glm.int16,
        glm.uint16,
        glm.int32,
        glm.uint32,
        glm.float32,
        glm.float64,
        glm.vec1,
        glm.dvec1,
        glm.i8vec1,
        glm.u8vec1,
        glm.i16vec1,
        glm.u16vec1,
        glm.ivec1,
        glm.uvec1,
    ):
        return 1
    if cls in (
        glm.vec2,
        glm.dvec2,
        glm.ivec2,
        glm.uvec2,
        glm.i8vec2,
        glm.u8vec2,
        glm.i16vec2,
        glm.u16vec2,
    ):
        return 2
    if cls in (
        glm.vec3,
        glm.dvec3,
        glm.ivec3,
        glm.uvec3,
        glm.i8vec3,
        glm.u8vec3,
        glm.i16vec3,
        glm.u16vec3,
    ):
        return 3
    if cls in (
        glm.vec4,
        glm.dvec4,
        glm.ivec4,
        glm.uvec4,
        glm.i8vec4,
        glm.u8vec4,
        glm.i16vec4,
        glm.u16vec4,
        glm.mat2,
        glm.dmat2,
        glm.imat2,
        glm.umat2,
    ):
        return 4
    if cls in (
        glm.mat3,
        glm.dmat3,
        glm.imat3,
        glm.umat3,
    ):
        return 9
    if cls in (
        glm.mat4,
        glm.dmat4,
        glm.imat4,
        glm.umat4,
    ):
        return 16
    raise NotImplementedError


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

    def set_data(self, va_id, data: glm.array, usage: int = gl.GL_STATIC_DRAW):
        """Sets the data to the VBO and binds it to a vertex attribute.

        Parameters
        ----------
        va_id : int
            the vertex attribute index to be binded to
        data : glm.array
            the array of vertices
        usage : int
            the `usage` parameter of :func:`OpenGL.GL.glBufferData`
        """
        with self:
            gl.glBufferData(gl.GL_ARRAY_BUFFER, data.nbytes, data.ptr, usage)
            esize = glElementSize(data.element_type)
            gl.glVertexAttribPointer(
                va_id,
                esize,
                glElementType(data.element_type),
                gl.GL_FALSE,
                data.dt_size * esize,
                None,
            )
            gl.glEnableVertexAttribArray(va_id)

    def __del__(self):
        gl.glDeleteBuffers(1, [self.obj])


class ShaderProgram(gl.shaders.ShaderProgram):
    def set_uniform(self, name: str, value):
        loc = gl.glGetUniformLocation(self, name)
        if isinstance(value, bool):
            gl.glUniform1i(loc, int(value))
        elif isinstance(
            value, (int, np.int8, np.int16, np.int32, glm.int8, glm.int16, glm.int32)
        ):
            gl.glUniform1i(loc, int(value))
        elif isinstance(value, (glm.i8vec1, glm.i16vec1, glm.ivec1)):
            gl.glUniform1i(loc, value.x)
        elif isinstance(
            value, (np.uint8, np.uint16, np.uint32, glm.uint8, glm.uint16, glm.uint32)
        ):
            gl.glUniform1ui(loc, np.uint32(value))
        elif isinstance(value, (glm.u8vec1, glm.u16vec1, glm.uvec1)):
            gl.glUniform1ui(loc, value.x)
        elif isinstance(value, (float, np.float32, glm.float32)):
            gl.glUniform1f(loc, float(value))
        elif isinstance(value, glm.vec1):
            gl.glUniform1f(loc, value.x)
        elif isinstance(value, (glm.i8vec2, glm.i16vec2, glm.ivec2)):
            gl.glUniform2i(loc, value.x, value.y)
        elif isinstance(value, (glm.u8vec2, glm.u16vec2, glm.uvec2)):
            gl.glUniform2ui(loc, value.x, value.y)
        elif isinstance(value, glm.vec2):
            gl.glUniform2f(loc, value.x, value.y)
        elif isinstance(value, (glm.i8vec3, glm.i16vec3, glm.ivec3)):
            gl.glUniform3i(loc, value.x, value.y, value.z)
        elif isinstance(value, (glm.u8vec3, glm.u16vec3, glm.uvec3)):
            gl.glUniform3ui(loc, value.x, value.y, value.z)
        elif isinstance(value, glm.vec3):
            gl.glUniform3f(loc, value.x, value.y, value.z)
        elif isinstance(value, (glm.i8vec4, glm.i16vec4, glm.ivec4)):
            gl.glUniform4i(loc, value.x, value.y, value.z, value.w)
        elif isinstance(value, (glm.u8vec4, glm.u16vec4, glm.uvec4)):
            gl.glUniform4ui(loc, value.x, value.y, value.z, value.w)
        elif isinstance(value, glm.vec4):
            gl.glUniform4f(loc, value.x, value.y, value.z, value.w)
        elif isinstance(value, glm.mat2):
            gl.glUniformMatrix2fv(loc, 1, gl.GL_TRUE, np.array(value))
        elif isinstance(value, glm.mat3):
            gl.glUniformMatrix3fv(loc, 1, gl.GL_TRUE, np.array(value))
        elif isinstance(value, glm.mat4):
            gl.glUniformMatrix4fv(loc, 1, gl.GL_TRUE, np.array(value))
        else:
            raise NotImplementedError

    def set_uniform_texture_unit(self, name: str, loc: int):
        self.set_uniform(name, int(loc))


compileShader = gl.shaders.compileShader


def compileProgram(*shaders, **named):
    program = gl.glCreateProgram()
    if named.get("separable"):
        gl.glProgramParameteri(
            program, gl.ARB.separate_shader_objects.GL_PROGRAM_SEPARABLE, gl.GL_TRUE
        )
    if named.get("retrievable"):
        gl.glProgramParameteri(
            program,
            gl.ARB.get_program_binary.GL_PROGRAM_BINARY_RETRIEVABLE_HINT,
            gl.GL_TRUE,
        )
    for shader in shaders:
        gl.glAttachShader(program, shader)
    program = ShaderProgram(program)
    gl.glLinkProgram(program)
    if named.get("validate", True):
        program.check_validate()
    program.check_linked()
    for shader in shaders:
        gl.glDeleteShader(shader)
    return program


compileProgram.__doc__ = gl.shaders.compileProgram.__doc__
