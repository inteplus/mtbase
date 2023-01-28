"""Submodules dealing with datatypes."""


import sys


def is_ndarray(a) -> bool:
    """Checks if the object is a numpy ndarray or not."""
    if "numpy" not in sys.modules:
        return False
    import numpy as np

    return isinstance(a, np.ndarray)


def is_jaxarray(a) -> bool:
    """Checks if the object is a jax Array or not."""
    if "jax" not in sys.modules:
        return False
    import jax

    return isinstance(a, jax.Array)


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
