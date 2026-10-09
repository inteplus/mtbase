"""Submodules dealing with datatypes.

Each `is_*` function tells whether an object is of a given array/tensor type without forcing
the corresponding heavy library to be imported: if the library has not already been imported
elsewhere in the process, the object cannot be one of its instances and the function returns
False immediately.
"""

import sys


def is_ndarray(a) -> bool:
    """Checks if the object is a numpy ndarray or not.

    Parameters
    ----------
    a : object
        the object to check

    Returns
    -------
    bool
        True if numpy has been imported and `a` is a :class:`numpy.ndarray`, False otherwise

    Examples
    --------
    >>> import numpy as np
    >>> from mt.base import is_ndarray
    >>> is_ndarray(np.zeros(3))
    True
    >>> is_ndarray([0, 0, 0])
    False
    """
    if "numpy" not in sys.modules:
        return False
    import numpy as np

    return isinstance(a, np.ndarray)


def is_jaxarray(a) -> bool:
    """Checks if the object is a jax ndarray or not.

    Parameters
    ----------
    a : object
        the object to check

    Returns
    -------
    bool
        True if jax has been imported and `a` is a :class:`jax.numpy.ndarray`, False otherwise
    """
    if "jax" not in sys.modules:
        return False
    import jax.numpy as jnp

    return isinstance(a, jnp.ndarray)


def is_torchtensor(a) -> bool:
    """Checks if the object is a torch tensor or not.

    Parameters
    ----------
    a : object
        the object to check

    Returns
    -------
    bool
        True if torch has been imported and `a` is a :class:`torch.Tensor`, False otherwise
    """
    if "torch" not in sys.modules:
        return False
    import torch

    return isinstance(a, torch.Tensor)


def is_tftensor(a) -> bool:
    """Checks if the object is a tensorflow tensor or not.

    Parameters
    ----------
    a : object
        the object to check

    Returns
    -------
    bool
        True if tensorflow has been imported and :func:`tensorflow.is_tensor` accepts `a`, False
        otherwise
    """
    if "tensorflow" not in sys.modules:
        return False
    import tensorflow as tf

    return tf.is_tensor(a)


def is_keras_variable(a) -> bool:
    """Checks if the object is a keras Variable or not.

    Parameters
    ----------
    a : object
        the object to check

    Returns
    -------
    bool
        True if keras has been imported and `a` is a `keras.Variable` (Keras 3), False otherwise
    """
    if "keras" not in sys.modules:
        return False
    import keras

    return isinstance(a, keras.Variable)


def is_h5group(a) -> bool:
    """Checks if the object is a :class:`h5py.Group` instance or not.

    Parameters
    ----------
    a : object
        the object to check

    Returns
    -------
    bool
        True if h5py has been imported and `a` is a :class:`h5py.Group` (which includes
        :class:`h5py.File`), False otherwise
    """
    if "h5py" not in sys.modules:
        return False
    import h5py

    return isinstance(a, h5py.Group)
