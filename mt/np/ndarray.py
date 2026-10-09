"""Useful functions dealing with numpy array."""

import typing as tp
import math
import numpy as np
import base64
from io import BytesIO


def ndarray_repr(a: np.ndarray) -> str:
    """Gets a one-line representation string for a numpy array.

    Arrays of up to 20 elements are shown in full (as a nested list). Larger arrays are summarised
    by their shape, dtype and basic statistics, so that the string stays short.

    Parameters
    ----------
    a : numpy.ndarray
        input numpy array

    Returns
    -------
    str
        a short representation string for the array

    Raises
    ------
    TypeError
        if `a` is not a numpy array

    Examples
    --------
    >>> from mt import np
    >>> np.ndarray_repr(np.array([1, 2, 3], dtype=np.int32))
    'ndarray([1, 2, 3], dtype=int32)'
    >>> np.ndarray_repr(np.zeros((5, 5), dtype=np.float32))
    'ndarray(shape=(5, 5), dtype=float32, min=0.0, max=0.0, mean=0.0, std=0.0)'
    """
    if not isinstance(a, np.ndarray):
        raise TypeError(f"An ndarray expected. Got '{type(a)}'.")

    if a.size > 20:
        return (
            f"ndarray(shape={a.shape}, dtype={a.dtype}, min={a.min()}, max={a.max()}, "
            f"mean={a.mean()}, std={a.std()})"
        )

    return f"ndarray({a.tolist()!r}, dtype={a.dtype})"


def sigmoid(x) -> np.ndarray:
    """Returns `sigmoid(x) = 1/(1+exp(-x))`.

    Parameters
    ----------
    x : float or numpy.ndarray
        input scalar or array. For arrays, very negative values (below -20 for float64, below -9
        for other dtypes) are evaluated as `exp(x)` to avoid overflow in `exp(-x)`.

    Returns
    -------
    float or numpy.ndarray
        the sigmoid of the input, of the same kind (scalar or array) as the input

    See Also
    --------
    asigmoid : the inverse function

    Examples
    --------
    >>> from mt import np
    >>> np.sigmoid(0.0)
    0.5
    >>> np.sigmoid(np.array([0.0, 2.0])).round(4)
    array([0.5   , 0.8808])
    """

    if not isinstance(x, np.ndarray):
        return 1 / (1 + math.exp(-x))

    cutoff = -20 if x.dtype == np.float64 else -9
    return np.where(x < cutoff, np.exp(x), 1 / (1 + np.exp(-x)))


def asigmoid(y) -> np.ndarray:
    """Inverse of sigmoid, that is `asigmoid(y) = log(y/(1-y))`. See :func:`sigmoid`.

    Parameters
    ----------
    y : float or numpy.ndarray
        input scalar or array with values in the open interval (0, 1)

    Returns
    -------
    float or numpy.ndarray
        the logit of the input, of the same kind (scalar or array) as the input

    Examples
    --------
    >>> from mt import np
    >>> np.asigmoid(0.5)
    0.0
    >>> np.asigmoid(np.sigmoid(np.array([-1.0, 2.0]))).round(6)
    array([-1.,  2.])
    """

    if not isinstance(y, np.ndarray):
        return math.log(y) - math.log1p(-y)

    return np.log(y) - np.log1p(-y)


def frombytes(data: bytes) -> np.ndarray:
    """Converts a `bytes` instance into a 1D uint8 array.

    The array shares memory with `data` and is therefore read-only.

    Examples
    --------
    >>> from mt import np
    >>> np.frombytes(b"abc")
    array([97, 98, 99], dtype=uint8)
    """
    return np.frombuffer(data, dtype=np.uint8)


def divide_no_nan(
    x: tp.Union[np.ndarray, float], y: tp.Union[np.ndarray, float]
) -> tp.Union[np.ndarray, float]:
    """Computes a safe divide which returns 0 if y (denominator) is zero.

    The returning object is scalar if and only if both `x` and `y` are scalar.

    Parameters
    ----------
    x : float or numpy.ndarray
        numerator
    y : float or numpy.ndarray
        denominator; entries equal to zero give 0 in the result. `x` and `y` must be broadcastable.

    Returns
    -------
    float or numpy.ndarray
        the element-wise quotient `x/y`, with 0 wherever `y` is 0

    Examples
    --------
    >>> from mt import np
    >>> np.divide_no_nan(1.0, 0.0)
    0.0
    >>> np.divide_no_nan(np.array([1.0, 2.0]), np.array([2.0, 0.0]))
    array([0.5, 0. ])
    """
    cond = y != 0
    res = np.where(cond, np.divide(x, y, out=None, where=cond), 0)
    if np.isscalar(x) and np.isscalar(y):
        res = float(res)
    return res


def to_b85(x: np.ndarray) -> str:
    """Converts a primitive numpy array into a b85-encoded string.

    The array is serialised in the ``.npy`` format (so shape and dtype are preserved) and then
    base85-encoded into an ASCII string. Object arrays are not supported because pickling is
    disabled.

    Parameters
    ----------
    x : numpy.ndarray
        a numpy array of a primitive dtype

    Returns
    -------
    str
        an ASCII string that :func:`from_b85` can decode back to the array

    Examples
    --------
    >>> from mt import np
    >>> s = np.to_b85(np.array([[1.0, 2.0], [3.0, 4.0]]))
    >>> isinstance(s, str)
    True
    >>> np.from_b85(s)
    array([[1., 2.],
           [3., 4.]])
    """
    f = BytesIO()
    np.save(f, x, allow_pickle=False)
    encoded = base64.b85encode(f.getbuffer())
    return encoded.decode("ascii")


def from_b85(b85: str) -> np.ndarray:
    """Converts a b85-encoded string back to a numpy array.

    Parameters
    ----------
    b85 : str
        a string produced by :func:`to_b85`

    Returns
    -------
    numpy.ndarray
        the decoded array, with its original shape and dtype

    See Also
    --------
    to_b85 : the encoding function
    """
    x = base64.b85decode(b85)
    f = BytesIO(x)
    return np.load(f, allow_pickle=False)
