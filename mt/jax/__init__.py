"""Alias module of `jax`_.

Instead of:

.. code-block:: python

   import jax
   import jax.numpy as jnp
   import jax.scipy as jsp

You do:

.. code-block:: python

   from mt import jax, jnp, jsp

It will import the jax package and its numpy and scipy packages. While doing so, it tries to avoid
preallocation, accepting potential memory fragmentation.

Please see Python package `jax`_ for more details.

.. _jax:
   https://jax.readthedocs.io/en/latest/index.html
"""

import sys

if "jax" not in sys.modules:
    import os

    if "XLA_PYTHON_CLIENT_PREALLOCATE" not in os.environ:
        os.environ["XLA_PYTHON_CLIENT_PREALLOCATE"] = "false"

try:
    import jax as _jax

    for key in _jax.__dict__:
        if key == "__doc__":
            continue
        globals()[key] = _jax.__dict__[key]
except ImportError:
    raise ImportError("Alias 'mt.jax' requires package 'jax' be installed.")
