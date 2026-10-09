"""Threading utilities, a thin alias of :mod:`threading` plus :mod:`mt.threading`.

Everything public in the standard :mod:`threading` and in :mod:`mt.threading` is available here.
Import from :mod:`mt.threading` in new code. This module is planned to be deprecated starting
from v4.0.
"""

from threading import *
from mt.threading import *

# MT-TODO: make the module deprecated starting from v4.0.
