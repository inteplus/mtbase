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
    """Get a distinct color for a given class index.

    Parameters
    ----------
    class_idx : int
        Class index.

    Returns
    -------
    numpy.ndarray
        Color as a numpy array of shape (3,) in BGR format.
    """
    if class_idx < len(initial_colors):
        return np.array(initial_colors[class_idx], dtype=np.uint8)

    # Use a simple hashing function to generate distinct colors
    np.random.seed(class_idx)
    color = np.random.randint(0, 256, size=2)
    color = np.array([255, color[0], color[1] // 2], dtype=np.uint8)
    np.random.shuffle(color)
    return color
