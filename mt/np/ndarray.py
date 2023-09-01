"""Useful functions dealing with numpy array."""

import typing as tp
import math
import numpy as np
import base64
from io import BytesIO


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


def divide_no_nan(
    x: tp.Union[np.ndarray, float], y: tp.Union[np.ndarray, float]
) -> tp.Union[np.ndarray, float]:
    """Computes a safe divide which returns 0 if y (denominator) is zero.

    The returning object is scalar if and only if both `x` and `y` are scalar.
    """
    cond = y != 0
    res = np.where(cond, np.divide(x, y, where=cond), 0)
    if np.isscalar(x) and np.isscalar(y):
        res = float(res)
    return res


def to_b85(x: np.ndarray) -> str:
    """Converts a primitve numpy array into a b85-encoded string."""
    f = BytesIO()
    np.save(f, x, allow_pickle=False)
    encoded = base64.b85encode(f.getbuffer())
    return encoded.decode("ascii")


def from_b85(b85: str) -> np.ndarray:
    """Converts a b85-encoded string back to a numpy array."""
    x = base64.b85decode(b85)
    f = BytesIO(x)
    return np.load(f, allow_pickle=False)
