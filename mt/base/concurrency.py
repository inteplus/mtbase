"""Useful functions dealing with concurrency.

2023/05/24
==========

Soon this module will be replaced by :module:`mt.concurrency`.
"""

from mt.concurrency import *

from mt import logg

logg.logger.warn_module_move("mt.base.concurrency", "mt.concurrency")
