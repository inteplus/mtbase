# pylint: disable=invalid-name
# pylint: disable=missing-function-docstring

"""Useful functions making class colors."""

import numpy as np


initial_colors = [
    (255, 0, 0),
    (0, 255, 0),
    (0, 0, 255),
    (255, 255, 0),
    (255, 0, 255),
    (0, 255, 255),
]


def get_class_color(class_idx: int) -> np.ndarray:
    """Gets a distinct color for a given class index.

    The first 6 indices map to fixed saturated colors (blue, green, red, cyan, magenta, yellow, in
    that order, as BGR triplets). Larger indices get a pseudo-random but reproducible color
    obtained by seeding numpy's global random generator with the index.

    Parameters
    ----------
    class_idx : int
        non-negative class index

    Returns
    -------
    numpy.ndarray
        color as a uint8 array of shape (3,) in BGR (OpenCV) channel order

    Notes
    -----
    For `class_idx` of 6 or above, the function calls ``numpy.random.seed(class_idx)``, hence
    resets the state of numpy's global random generator as a side effect.

    Examples
    --------
    >>> from mt import np
    >>> np.get_class_color(0)
    array([255,   0,   0], dtype=uint8)
    >>> np.get_class_color(2)
    array([  0,   0, 255], dtype=uint8)
    """
    if class_idx < len(initial_colors):
        return np.array(initial_colors[class_idx], dtype=np.uint8)

    # Use a simple hashing function to generate distinct colors
    np.random.seed(class_idx)
    color = np.random.randint(0, 256, size=2)
    color = np.array([255, color[0], color[1] // 2], dtype=np.uint8)
    np.random.shuffle(color)
    return color
