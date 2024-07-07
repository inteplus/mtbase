"""Submodules dealing with datatypes."""

import sys


def is_ndarray(a) -> bool:
    """Checks if the object is a numpy ndarray or not."""
    if "numpy" not in sys.modules:
        return False
    import numpy as np

    return isinstance(a, np.ndarray)


def is_jaxarray(a) -> bool:
    """Checks if the object is a jax ndarray or not."""
    if "jax" not in sys.modules:
        return False
    import jax.numpy as jnp

    return isinstance(a, jnp.ndarray)


def is_tftensor(a) -> bool:
    """Checks if the object is a tensorflow tensor or not."""
    if "tensorflow" not in sys.modules:
        return False
    import tensorflow as tf

    return tf.is_tensor(a)


def is_h5group(a) -> bool:
    """Checks if the object is a :class:`h5py.Group` instance or not."""
    if "h5py" not in sys.modules:
        return False
    import h5py

    return isinstance(a, h5py.Group)


class LogicError(RuntimeError):
    """An error in the logic, defined by a message and a debugging dictionary.

    As of 2024/07/07, the user can optionally provide the error that caused this error and
    optionally the callstack (in list of lines) of the causing error.
    """

    def __init__(self, msg, debug={}, causing_error=None, causing_callstack=[]):
        super().__init__(msg, debug, causing_error, causing_callstack)

    def __str__(self):
        l_lines = []

        causing_error = self.args[2]
        if causing_error:
            msg = f"With the following {type(causing_error).__name__}:"
            l_lines.append(msg)
            causing_callstack = self.args[3]
            if causing_callstack:
                l_lines.append("  Callstack:")
                for line in causing_callstack:
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
