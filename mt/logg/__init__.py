"""Customised logging.

This module extends Python's package `logging`_ with some customisation made specifically for MT's
code. Instead of:

.. code-block:: python

   import logging

You do:

.. code-block:: python

   from mt import logg

It will import the logging package plus the additional stuff implemented here.

We use acronym `logg` instead of `log` to avoid naming conflict with the mathematical `log`
function. In addition, the logging functions `critical`, `error`, `warning`, `warn`, `info` and
`debug` have their API extended a bit.

Please see Python package `logging`_ for more details.

.. _logging:
   https://docs.python.org/3/library/logging.html
"""

from logging import *
import logging.handlers as _lh
import functools
import sys as _sys
import os as _os
import os.path as _op
import tempfile as _tf
import getpass as _gp
from colorama import Fore, Style
from colorama import init as _colorama_init

_colorama_init()

from mt import traceback, shutil, ctx, tp


__all__ = [
    "IndentedLoggerAdapter",
    "make_logger",
    "prepare_file_handler",
    "init",
    "logger",
    "with_logger",
    "log",
    "critical",
    "error",
    "info",
    "warning",
    "warn",
    "debug",
    "ScopedLog",
    "scoped_log",
    "scoped_critical",
    "scoped_error",
    "scoped_info",
    "scoped_warn",
    "scoped_warning",
    "scoped_debug",
    "scoped_log_if",
]


# -----------------------------------------------------------------------------
# base implementation
# -----------------------------------------------------------------------------


class IndentedFilter(Filter):
    """Indented filter for indented logger adapter."""

    def __init__(self, indented_logger_adapter):
        self.parent = indented_logger_adapter

    def filter(self, record):
        record.indent = self.parent.indent
        return True


class IndentedLoggerAdapter(LoggerAdapter):
    """Logger with indenting capability."""

    def __init__(self, logger, extra):
        super().__init__(logger, extra)
        self.indent = 0
        self.last_exception = None

        # add a filter that adds 'indent' to every log record
        f = IndentedFilter(self)
        self.logger.addFilter(f)

    def process(self, msg: tp.Union[str, bytes], kwargs):
        return (
            ("  " * self.indent + msg.decode(), kwargs)
            if isinstance(msg, bytes)
            else ("  " * self.indent + msg, kwargs)
        )

    def inc(self):
        self.indent += 1

    def dec(self):
        self.indent -= 1

    def setLastException(self, value):
        self.last_exception = value

    def getLastException(self):
        return self.last_exception

    # ----- break mutli-line messages -----

    def critical(self, msg: tp.Union[str, bytes], *args, **kwargs):
        if not isinstance(msg, (str, bytes)):
            msg = str(msg)
        for m in msg.split(b"\n") if isinstance(msg, bytes) else msg.split("\n"):
            super(IndentedLoggerAdapter, self).critical(
                Fore.LIGHTRED_EX + m, *args, **kwargs
            )

    def error(self, msg: tp.Union[str, bytes], *args, **kwargs):
        if not isinstance(msg, (str, bytes)):
            msg = str(msg)
        for m in msg.split(b"\n") if isinstance(msg, bytes) else msg.split("\n"):
            super(IndentedLoggerAdapter, self).error(
                Fore.LIGHTMAGENTA_EX + m, *args, **kwargs
            )

    def warning(self, msg: tp.Union[str, bytes], *args, **kwargs):
        if not isinstance(msg, (str, bytes)):
            msg = str(msg)
        for m in msg.split(b"\n") if isinstance(msg, bytes) else msg.split("\n"):
            super(IndentedLoggerAdapter, self).warning(
                Fore.LIGHTYELLOW_EX + m, *args, **kwargs
            )

    warn = warning

    def info(self, msg: tp.Union[str, bytes], *args, **kwargs):
        if not isinstance(msg, (str, bytes)):
            msg = str(msg)
        for m in msg.split(b"\n") if isinstance(msg, bytes) else msg.split("\n"):
            super(IndentedLoggerAdapter, self).info(
                Fore.LIGHTWHITE_EX + m, *args, **kwargs
            )

    def debug(self, msg: tp.Union[str, bytes], *args, **kwargs):
        if not isinstance(msg, (str, bytes)):
            msg = str(msg)
        for m in msg.split(b"\n") if isinstance(msg, bytes) else msg.split("\n"):
            super(IndentedLoggerAdapter, self).debug(
                Fore.LIGHTBLUE_EX + m, *args, **kwargs
            )

    # ----- scoped logging -----

    def scoped_critical(self, msg: tp.Union[str, bytes], curly: bool = False):
        return ScopedLog(self, CRITICAL, msg=msg, curly=curly)

    def scoped_error(self, msg: tp.Union[str, bytes], curly: bool = False):
        return ScopedLog(self, ERROR, msg=msg, curly=curly)

    def scoped_warning(self, msg: tp.Union[str, bytes], curly: bool = False):
        return ScopedLog(self, WARNING, msg=msg, curly=curly)

    scoped_warn = scoped_warning

    def scoped_info(self, msg: tp.Union[str, bytes], curly: bool = False):
        return ScopedLog(self, INFO, msg=msg, curly=curly)

    def scoped_debug(self, msg: tp.Union[str, bytes], curly: bool = False):
        return ScopedLog(self, DEBUG, msg=msg, curly=curly)

    # ----- useful warning messages -----

    def warn_last_exception(self):
        lines = traceback.format_exc_info(*_sys.exc_info())
        for x in lines:
            self.warning(x)

    def warn_module_move(self, old_module, new_module):
        """Warns that an old module has been moved to a new module.

        Parameters
        ----------
        old_module : str
            short string representing the old module
        new_module : str
            short string representing the new module
        """
        self.warn(
            "IMPORT: module '{}' has been deprecated. Please use module '{}' instead.".format(
                old_module, new_module
            )
        )
        lines = traceback.extract_stack_compact()
        if len(lines) > 9:
            selected_lines = lines[-9:-7]
        else:
            selected_lines = lines
        for x in selected_lines:
            self.warn(x)

    def warn_func_move(self, old_func, new_func):
        """Warns that an old function has been moved to a new func.

        Parameters
        ----------
        old_func : str
            short string representing the old function
        new_func : str
            short string representing the new function
        """
        self.warn(
            "IMPORT: function '{}()' has been deprecated. Please use function '{}()' instead.".format(
                old_func, new_func
            )
        )
        lines = traceback.extract_stack_compact()
        if len(lines) > 9:
            selected_lines = lines[-9:-7]
        else:
            selected_lines = lines
        for x in selected_lines:
            self.warn(x)

    def debug_call_stack(self):
        lines = traceback.format_list(traceback.extract_stack())
        lines = "".join(lines).split("\n")
        lines.pop(-2)
        lines.pop(-2)
        for x in lines:
            self.debug(x)


_format_str = (
    Fore.CYAN
    + "%(asctime)s "
    + Fore.LIGHTGREEN_EX
    + "%(levelname)8s"
    + Fore.WHITE
    + ": ["
    + Fore.LIGHTMAGENTA_EX
    + "%(name)s"
    + Fore.LIGHTWHITE_EX
    + "] %(message)s"
    + Fore.RESET
)
# _format_str = f'{Fore.CYAN}%(asctime)s {Fore.LIGHTGREEN_EX}%(levelname)8s{Fore.WHITE}: [{Fore.LIGHTMAGENTA_EX}%(name)s{Fore.LIGHTWHITE_EX}] %(message)s{Fore.RESET}'
# _format_str = '%(asctime)s %(levelname)8s: [%(name)s] %(message)s'


old_factory = getLogRecordFactory()


def record_factory(*args, **kwargs):
    record = old_factory(*args, **kwargs)

    frames = traceback.extract_stack()
    for frame in reversed(frames):
        if not "logging" in frame.filename:
            record.pathname = frame.filename
            record.lineno = frame.lineno
            record.funcName = frame.name
            filename = _op.basename(frame.filename)
            record.filename = filename
            record.module = filename[:-3] if filename.endswith(".py") else filename
            break

    return record


setLogRecordFactory(record_factory)


def make_logger(logger_name, max_indent=10):
    """Make a singleton logger.

    :Parameters:
        logger_name : str
            name of the logger
        max_indent : int
            max number of indents. Default to 10.

    The generated logger has 2 handlers, one standard output and one watched file handler. Both
    handlers threshold at DEBUG. The standard handler further thresholds at `max_indent`. The debug
    handler thresholds at DEBUG level. The standard handler is brief, not showing thread id. The
    debug level is verbose, showing everything.

    """
    init()

    logger = getLogger(logger_name)
    logger.setLevel(1)  # capture everything but let the handlers decide
    logger = IndentedLoggerAdapter(logger, extra=None)

    # standard handler
    if True:

        class StdFilter(Filter):
            def __init__(self, max_indent=None, name=""):
                super().__init__(name=name)
                self.max_indent = max_indent

            def filter(self, record):
                if self.max_indent is None:
                    return True
                return record.indent <= self.max_indent

        std_handler = StreamHandler()
        std_handler.setLevel(DEBUG)

        std_filter = StdFilter(max_indent=max_indent)
        std_handler.addFilter(std_filter)

        # determine some max string lengths
        column_length = shutil.stty_size()[1] - 13
        log_lvl_length = min(max(int(column_length * 0.03), 1), 8)
        s1 = "{}.{}s ".format(log_lvl_length, log_lvl_length)
        column_length -= log_lvl_length
        s5 = "-{}.{}s".format(column_length, column_length)

        fmt_str = (
            Fore.CYAN
            + "%(asctime)s "
            + Fore.LIGHTGREEN_EX
            + "%(levelname)"
            + s1
            + Fore.LIGHTWHITE_EX
            + "%(message)"
            + s5
            + Fore.RESET
        )
        formatter = Formatter(fmt_str)
        formatter.default_time_format = (
            "%a %H:%M:%S"  # stupid Python 3.8 implementation of Formatter
        )
        std_handler.setFormatter(formatter)

        logger.logger.addHandler(std_handler)

    return logger


def prepare_file_handler(prefix="ml", filepath=None):
    """Prepares a file handler for logging."""
    init()
    if filepath is None:
        filepath = _op.join(init._temp_dirpath, prefix + ".debug.log")
    if _op.exists(filepath):  # remove previous file
        _os.remove(filepath)
    file_handler = FileHandler(filepath)

    file_handler.setLevel(DEBUG)

    file_formatter = Formatter(
        "%(asctime)s {pid=%(process)5d log_level=%(levelno)2d} %(message)s"
    )
    file_handler.setFormatter(file_formatter)

    return file_handler


# -----------------------------------------------------------------------------
# module initialisation
# -----------------------------------------------------------------------------


def init():
    """Initialises the module if it has not been initialised."""
    if init._completed:
        return

    setattr(init, "_home_dirpath", _op.join(_op.expanduser("~"), ".mtbase"))
    _os.makedirs(init._home_dirpath, exist_ok=True)

    temp_dirpath = _op.join(_tf.gettempdir(), _gp.getuser(), ".mtbase")
    _os.makedirs(temp_dirpath, exist_ok=True)
    setattr(init, "_temp_dirpath", temp_dirpath)

    debuglog_filepath = _op.join(temp_dirpath, "debug.log")
    if _op.exists(debuglog_filepath):
        _os.remove(debuglog_filepath)
    setattr(init, "_debuglog_filepath", debuglog_filepath)

    init._completed = True


init._completed = False

logger = make_logger("mtbase")


def with_logger(func, logger: tp.Optional[IndentedLoggerAdapter] = None):
    """Wrapper that adds keyword 'logger=loger' to the input function."""
    return functools.partial(func, logger=logger)


# -----------------------------------------------------------------------------
# convenient log functions
# -----------------------------------------------------------------------------


# convenient log functions
def log(
    level: int,
    msg: tp.Union[str, bytes],
    logger: tp.Optional[Logger] = logger,
    *args,
    **kwargs
):
    """Wraps :func:`logging.log` with additional logger keyword.

    Parameters
    ----------
    level : int
        level. Passed as-is to :func:`logging.log`.
    msg : str or bytes
        message. Passed as-is to :func:`logging.log`.
    logger : logging.Logger, optional
        which logger to process the message. Default is the default logger of mtbase. If None is
        provided, no message will be logged.
    *args : tuple
        positional arguments passed as-is to :func:`logging.log`.
    *kwargs : dict
        keyword arguments passed as-is to :func:`logging.log`.
    """
    if logger:
        logger.log(level, msg, *args, **kwargs)


def critical(
    msg: tp.Union[str, bytes], logger: tp.Optional[Logger] = logger, *args, **kwargs
):
    """Wraps :func:`logging.critical` with additional logger keyword.

    Parameters
    ----------
    msg : str or bytes
        message. Passed as-is to :func:`logging.critical`.
    logger : logging.Logger, optional
        which logger to process the message. Default is the default logger of mtbase. If None is
        provided, no message will be logged.
    *args : tuple
        positional arguments passed as-is to :func:`logging.critical`.
    *kwargs : dict
        keyword arguments passed as-is to :func:`logging.critical`.
    """
    if logger:
        logger.critical(msg, *args, **kwargs)


def error(
    msg: tp.Union[str, bytes], logger: tp.Optional[Logger] = logger, *args, **kwargs
):
    """Wraps :func:`logging.error` with additional logger keyword.

    Parameters
    ----------
    msg : str or bytes
        message. Passed as-is to :func:`logging.error`.
    logger : logging.Logger, optional
        which logger to process the message. Default is the default logger of mtbase. If None is
        provided, no message will be logged.
    *args : tuple
        positional arguments passed as-is to :func:`logging.error`.
    *kwargs : dict
        keyword arguments passed as-is to :func:`logging.error`.
    """
    if logger:
        logger.error(msg, *args, **kwargs)


def warning(
    msg: tp.Union[str, bytes], logger: tp.Optional[Logger] = logger, *args, **kwargs
):
    """Wraps :func:`logging.warning` with additional logger keyword.

    Parameters
    ----------
    msg : str or bytes
        message. Passed as-is to :func:`logging.warning`.
    logger : logging.Logger, optional
        which logger to process the message. Default is the default logger of mtbase. If None is
        provided, no message will be logged.
    *args : tuple
        positional arguments passed as-is to :func:`logging.warning`.
    *kwargs : dict
        keyword arguments passed as-is to :func:`logging.warning`.
    """
    if logger:
        logger.warning(msg, *args, **kwargs)


warn = warning


def info(
    msg: tp.Union[str, bytes], logger: tp.Optional[Logger] = logger, *args, **kwargs
):
    """Wraps :func:`logging.info` with additional logger keyword.

    Parameters
    ----------
    msg : str or bytes
        message. Passed as-is to :func:`logging.info`.
    logger : logging.Logger, optional
        which logger to process the message. Default is the default logger of mtbase. If None is
        provided, no message will be logged.
    *args : tuple
        positional arguments passed as-is to :func:`logging.info`.
    *kwargs : dict
        keyword arguments passed as-is to :func:`logging.info`.
    """
    if logger:
        logger.info(msg, *args, **kwargs)


def debug(
    msg: tp.Union[str, bytes], logger: tp.Optional[Logger] = logger, *args, **kwargs
):
    """Wraps :func:`logging.debug` with additional logger keyword.

    Parameters
    ----------
    msg : str or bytes
        message. Passed as-is to :func:`logging.debug`.
    logger : logging.Logger, optional
        which logger to process the message. Default is the default logger of mtbase. If None is
        provided, no message will be logged.
    *args : tuple
        positional arguments passed as-is to :func:`logging.debug`.
    *kwargs : dict
        keyword arguments passed as-is to :func:`logging.debug`.
    """
    if logger:
        logger.debug(msg, *args, **kwargs)


class ScopedLog:

    """Scoped-log a message.

    >>> from mt import logg
    >>> with logg.ScopedLog(logg.logger, logg.DEBUG, 'hello world'):
    ...     a = 1
    ...     logg.logger.info("Hi there")
    hello world:
      Hi there

    """

    def __init__(
        self,
        indented_logger_adapter,
        level,
        msg: tp.Union[str, bytes],
        curly: bool = False,
    ):
        """Scope a log message.

        # Arguments
        indented_logger_adapter : IndentedLoggerAdapter or None
            the logger. If None then does nothing, just passing things through.
        level : obj
            logging level (e.g. logging.CRITICAL, logging.INFO, logging.DEBUG, etc)
        msg : str
            message to log
        curly : bool
            whether or not to print a curly bracket
        """
        self.logger = indented_logger_adapter
        if self.logger is not None and not isinstance(
            self.logger, IndentedLoggerAdapter
        ):
            raise ValueError(
                "Argument `indented_logger_adapter` is expected to be an IndentedLoggerAdapter or None only."
            )

        self.msg = msg
        if self.logger is None:
            self.func = None
        elif level == CRITICAL:
            self.func = self.logger.critical
        elif level == ERROR:
            self.func = self.logger.error
        elif level == WARNING:
            self.func = self.logger.warning
        elif level == INFO:
            self.func = self.logger.info
        elif level == DEBUG:
            self.func = self.logger.debug
        else:
            raise ValueError("Unknown debugging level {}.".format(level))

        self.curly = curly

    def __enter__(self):
        if self.logger:
            self.func("{ " + self.msg if self.curly else self.msg + ":")
            self.logger.inc()
        return self

    def __exit__(self, type, value, traceback_obj):
        if self.logger:
            if self.logger.getLastException() != value:
                self.logger.setLastException(value)

                if type is not None:
                    lines = traceback.format_exc_info(type, value, traceback_obj)
                    for line in lines:
                        self.logger.debug("##{}".format(line))

            self.logger.dec()
            if self.curly:
                self.func("} " + self.msg)


# convenient scoped-log functions
def scoped_log(
    level,
    msg: tp.Union[str, bytes],
    logger: tp.Optional[IndentedLoggerAdapter] = logger,
    curly: bool = False,
):
    """Scoped log function that can be used in a with statement.

    Partial functions derived from the function include: `scoped_critical`, `scoped_error`,
    `scoped_warning`, `scoped_warn`, `scoped_info` and `scoped_debug`.

    Parameters
    ----------
    level : int
        level. Passed as-is to :class:`ScopeLog`.
    msg : str or bytes
        message. Passed as-is to :class:`ScopeLog`.
    logger : IndentedLoggerAdapter, optional
        which logger to process the message. Default is the default logger of mtbase. If None is
        provided, a null context is returned.
    curly : bool
        whether or not to print a curly bracket. Passed as-is to :class:`ScopeLog`.
    """

    if logger is None:
        return ctx.nullcontext()
    return ScopedLog(logger, level, msg=msg, curly=curly)


scoped_critical = functools.partial(scoped_log, CRITICAL)
scoped_error = functools.partial(scoped_log, ERROR)
scoped_warning = functools.partial(scoped_log, WARNING)
scoped_warn = scoped_warning
scoped_info = functools.partial(scoped_log, INFO)
scoped_debug = functools.partial(scoped_log, DEBUG)


def scoped_log_if(
    cond,
    func,
    indented_logger_adapter,
    level,
    msg: tp.Union[str, bytes],
    curly: bool = False,
    func_args: tuple = (),
    func_kwargs: dict = {},
    return_value_if_false=None,
):
    if not cond:
        return return_value_if_false
    with scoped_log(indented_logger_adapter, level, msg=msg, curly=curly):
        return func(*func_args, **func_kwargs)
