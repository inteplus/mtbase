"""Useful functions dealing with images."""

import numpy as np

from mt import tp


def quantise_images(a: np.ndarray, clip: bool = True) -> np.ndarray:
    """Quantises a tensor of images.

    It takes a tensor of images of dtype float32 where every pixel value is in range [0., 256.)
    and converts them to dtype uint8 by truncating the fractional part. It is the inverse of
    :func:`dequantise_images` up to the noise added by the latter.

    Parameters
    ----------
    a : numpy.ndarray
        input tensor of images of dtype float32 and each value is in range [0., 256.)
    clip : bool
        whether or not to clip overflow or underflow values. That is, values less than 0 are set to
        0 and values at 256 or above are set to 255. If False, out-of-range values are converted
        with numpy's ``astype`` semantics, which is undefined for out-of-range floats.

    Returns
    -------
    numpy.ndarray
        output tensor of images of dtype uint8 and each value is in range [0, 256)

    See Also
    --------
    dequantise_images : the opposite operation

    Examples
    --------
    >>> from mt import np
    >>> a = np.array([-1.0, 0.5, 3.9, 255.9, 300.0], dtype=np.float32)
    >>> np.quantise_images(a)
    array([  0,   0,   3, 255, 255], dtype=uint8)
    """
    if clip:
        a = np.clip(a, 0.0, 255.999)
    return a.astype(np.uint8)


def dequantise_images(
    a: np.ndarray, rng: tp.Optional[np.random.RandomState] = None
) -> np.ndarray:
    """Dequantises a tensor of images.

    It takes a tensor of images of dtype uint8, converts the tensor into dtype float32, and adds a
    uniform noise in range [0,1) to every pixel value.

    Parameters
    ----------
    a : numpy.ndarray
        input tensor of images of dtype uint8 and each value is in integer range [0, 256)
    rng : numpy.random.RandomState, optional
        the random number generator to make uniform noise values. If not provided, a freshly
        seeded one is created, so the output is not reproducible.

    Returns
    -------
    numpy.ndarray
        output tensor of images of dtype float32 and each value is in float32 range [0., 256.]

    Raises
    ------
    TypeError
        if `a` is not a numpy array or its dtype is not uint8

    See Also
    --------
    quantise_images : the opposite operation

    Examples
    --------
    >>> from mt import np
    >>> a = np.array([0, 10, 255], dtype=np.uint8)
    >>> b = np.dequantise_images(a, rng=np.random.RandomState(0))
    >>> b.dtype
    dtype('float32')
    >>> bool(np.all(np.floor(b) == a))
    True
    """
    if not isinstance(a, np.ndarray):
        raise TypeError(f"An ndarray is expected. Got '{type(a)}'.")

    if not np.issubdtype(a.dtype, np.uint8):
        raise TypeError(
            f"An ndarray of dtype uint8 is expected. Got '{a.dtype}'."
        )

    if rng is None:
        rng = np.random.RandomState()
    return (a + rng.uniform(size=a.shape)).astype(np.float32)
