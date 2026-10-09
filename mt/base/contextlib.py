"""Monkey-patching contextlib.

This module re-exports everything in :mod:`mt.ctx`, which provides the same names as the
standard :mod:`contextlib` (``contextmanager``, ``asynccontextmanager``, ``nullcontext``, etc.)
and backports for older Python versions. Import from :mod:`mt.ctx` in new code. This module is
planned to be deprecated starting from v4.0.
"""

from mt.ctx import *

# MT-TODO: make the module deprecated starting from v4.0.
