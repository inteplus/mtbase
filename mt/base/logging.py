from logging import *
from mt.logging import *
from mt.logg import logger

# MT-TODO: make the module deprecated starting from v4.0.
logger.warn_module_move("mt.base.logging", "mt.logg")
