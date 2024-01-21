"""Alias module of `v4l2`_.

Instead of:

.. code-block:: python

   import v4l2py

You do:

.. code-block:: python

   from mt import v4l2

It will import the v4l2py package.

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
    """Views the frame as a numpy image."""

    if self.pixel_format == PixelFormat.YUYV:
        dtype = np.uint16
    else:
        raise NotImplementedError

    return np.frombuffer(self.data, dtype).reshape((self.height, self.width))


Frame.as_image = _Frame_as_image
