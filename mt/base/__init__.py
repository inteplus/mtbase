from .logging import dummy_scope, make_logger, logger, init as _log_init
from .deprecated import deprecated_func
from .casting import cast, castable

home_dirpath = _log_init._home_dirpath
temp_dirpath = _log_init._temp_dirpath

__all__ = ['logger', 'home_dirpath', 'temp_dirpath', 'deprecated_func', 'cast', 'castable']


from .const import *


# backdoor to debug the process
from .debug_process import listen as _listen
_listen()

