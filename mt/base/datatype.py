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


def is_torchtensor(a) -> bool:
    """Checks if the object is a torch tensor or not."""
    if "torch" not in sys.modules:
        return False
    import torch

    return isinstance(a, torch.Tensor)


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
