"""Terminal utilities, a thin alias of :mod:`shutil` plus :mod:`mt.shutil`.

Everything public in :mod:`shutil` and :mod:`mt.shutil` is available here. Import from
:mod:`mt.shutil` in new code. This module is planned to be deprecated starting from v4.0.
"""

from shutil import *
from mt.shutil import *

# MT-TODO: make the module deprecated starting from v4.0.
