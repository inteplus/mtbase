"""Alias module of `v4l2`_.

Instead of:

.. code-block:: python

   import v4l2py

You do:

.. code-block:: python

   from mt import v4l2

It will import the v4l2py package. In addition, it adds an ``as_image()`` method to
``Frame`` (see below) which views a YUYV frame as a numpy array. It requires a Linux machine with
the package `v4l2py` installed.

Please see Python package `v4l2`_ for more details.

.. _v4l2:
   https://github.com/tiagocoutinho/v4l2py
"""

import numpy as np


try:
    import v4l2py as v4l2

    for key in v4l2.__dict__:
        if key == "__doc__":
            continue
        globals()[key] = v4l2.__dict__[key]
except ImportError:
    raise ImportError("Alias 'mt.v4l2' requires package 'v4l2py' be installed.")


def _Frame_as_image(self):
    """Views the frame as a numpy image.

    It is attached to ``Frame`` as method ``as_image()``.

    Returns
    -------
    numpy.ndarray
        a uint16 array of shape (height, width) sharing the frame's data. Each element holds one
        YUYV pixel's two bytes.

    Raises
    ------
    NotImplementedError
        if the pixel format of the frame is not YUYV
    """

    if self.pixel_format == PixelFormat.YUYV:
        dtype = np.uint16
    else:
        raise NotImplementedError

    return np.frombuffer(self.data, dtype).reshape((self.height, self.width))


Frame.as_image = _Frame_as_image
