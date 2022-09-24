"""Useful functions dealing with images."""

import typing as tp
import numpy as np


__all__ = ["dequantise_images"]


def dequantise_images(
    a: np.ndarray, rng: tp.Optional[np.random.RandomState] = None
) -> np.ndarray:
    """Dequantises an image or a high-rank tensor of images.

    It takes a tensor of images of dtype uint8, converts the tensor into dtype float32 and adds a
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
        output tensor of images of dtype float32 and each value is in float32 range [0, 256), plus
        uniform noise in range [0,1)
    """
    if not isinstance(a, np.ndarray):
        raise TypeError("An ndarray is expected. Got '{}'.".format(type(a)))

    if not np.issubdtype(a.dtype, np.uint8):
        raise TypeError(
            "An ndarray of dtype uint8 is expected. Got '{}'.".format(a.dtype)
        )

    if rng is None:
        rng = np.random.RandomState()
    a = a.astype(np.float32) + rng.uniform(size=a.shape)
    return a
