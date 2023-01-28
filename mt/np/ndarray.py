"""Useful functions dealing with numpy array."""


import math
import numpy as np


def ndarray_repr(a: np.ndarray) -> str:
    """Gets a one-line representation string for a numpy array.

    Parameters
    ----------
    a : numpy.ndarray
        input numpy array

    Returns
    -------
    str
        a short representation string for the array
    """
    if not isinstance(a, np.ndarray):
        raise TypeError("An ndarray expected. Got '{}'.".format(type(a)))

    if a.size > 20:
        return "ndarray(shape={}, dtype={}, min={}, max={}, mean={}, std={})".format(
            a.shape, a.dtype, a.min(), a.max(), a.mean(), a.std()
        )

    return "ndarray({}, dtype={})".format(repr(a.tolist()), a.dtype)


def sigmoid(x) -> np.ndarray:
    """Returns `sigmoid(x) = 1/(1+exp(-x))`."""

    if not isinstance(x, np.ndarray):
        return 1 / (1 + math.exp(-x))

    cutoff = -20 if x.dtype == np.float64 else -9
    return np.where(x < cutoff, np.exp(x), 1 / (1 + np.exp(-x)))


def asigmoid(y) -> np.ndarray:
    """Inverse of sigmoid. See :func:`sigmoid`."""

    if not isinstance(y, np.ndarray):
        return math.log(y) - math.log1p(-y)

    return np.log(y) - np.log1p(-y)


def frombytes(data: bytes) -> np.ndarray:
    """Converts a `bytes` instance into a 1D uint8 array."""
    return np.frombuffer(data, dtype=np.uint8)
