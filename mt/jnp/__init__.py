"""Alias module of `jax`_.

Instead of:

.. code-block:: python

   import jax
   import jax.numpy as jnp

You do:

.. code-block:: python

   from mt import jax, jnp

It will import the jax package and its numpy package. While doing so, it tries to avoid
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
    import jax.numpy as _jnp

    for k, v in _jnp.__dict__.items():
        globals()[k] = v
except ImportError:
    raise ImportError("Alias 'mt.jax' requires package 'jax' be installed.")
