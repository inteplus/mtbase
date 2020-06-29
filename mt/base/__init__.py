from .logging import dummy_scope, make_logger, init as _log_init

logger = make_logger("basemt")

home_dirpath = _log_init._home_dirpath
temp_dirpath = _log_init._temp_dirpath


__all__ = ['logger', 'home_dirpath', 'temp_dirpath']
