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
preallocation, accepting potential memory fragmentation: unless the environment variable
``XLA_PYTHON_CLIENT_PREALLOCATE`` is already set, or jax was imported before, it sets it to
``"false"`` so that jax does not grab most of the GPU memory at start-up.

This particular module, ``mt.jnp``, re-exports the contents of ``jax.numpy``. It raises ImportError
if package `jax` is not installed.

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
