"""Useful functions dealing with images."""

import numpy as np

from mt import tp


def quantise_images(a: np.ndarray, clip: bool = True) -> np.ndarray:
    """Quantises a tensor of images.

    It takes a tensor of images of dtype float32 where every pixel value is in range (0., 256.)
    and converts them to dtype uint8.

    Parameters
    ----------
    a : numpy.ndarray
        input tensor of images of dtype float32 and each value is in integer range (0., 256.)
    clip : bool
        whether or not to clip overflow or underflow values. That is, values less than 0 are set to
        0 and values at 256 or above are set to 255.

    Returns
    -------
    numpy.ndarray
        output tensor of images of dtype uint8 and each value is in range [0, 256)
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
        the random number generator to make uniform noise values

    Returns
    -------
    numpy.ndarray
        output tensor of images of dtype float32 and each value is in float32 range (0., 256.)
    """
    if not isinstance(a, np.ndarray):
        raise TypeError("An ndarray is expected. Got '{}'.".format(type(a)))

    if not np.issubdtype(a.dtype, np.uint8):
        raise TypeError(
            "An ndarray of dtype uint8 is expected. Got '{}'.".format(a.dtype)
        )

    if rng is None:
        rng = np.random.RandomState()
    return (a + rng.uniform(size=a.shape)).astype(np.float32)
