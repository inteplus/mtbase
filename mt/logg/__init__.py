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
`debug` have their API extended a bit: they take an optional `logger` argument (defaulting to the
module-level :data:`logger`), and do nothing if it is None.

Main components:

- :data:`logger`, the default :class:`IndentedLoggerAdapter` named "mtbase", which writes colored,
  one-line-per-message output to standard error (importing the module creates directories
  ``~/.mtbase`` and ``<tempdir>/<user>/.mtbase``);
- :func:`log`, :func:`critical`, :func:`error`, :func:`warning` (alias :func:`warn`), :func:`info`
  and :func:`debug`, which log a message with a given (or the default) logger;
- :class:`ScopedLog` and its helpers :func:`scoped_log`, :func:`scoped_critical`,
  :func:`scoped_error`, :func:`scoped_warning` (alias :func:`scoped_warn`), :func:`scoped_info` and
  :func:`scoped_debug`, which log a message and indent everything logged inside a ``with`` block;
  :func:`scoped_log_if` runs a function inside such a scope only if a condition is true;
- :func:`make_logger` and :func:`prepare_file_handler` to build other loggers and log files;
- decorators :func:`warn_uncaught_exception`, :func:`awarn_uncaught_exception`,
  :func:`return_exception` and :func:`areturn_exception`, and :func:`with_logger`.

Examples
--------
.. code-block:: python

   from mt import logg

   logg.info("starting")                       # uses the default logger
   with logg.scoped_info("loading data"):     # logs "loading data:" then indents what follows
       logg.debug("reading file")
   logg.warning("careful", logger=None)       # does nothing

Please see Python package `logging`_ for more details.

.. _logging:
   https://docs.python.org/3/library/logging.html
"""

from logging import *
import functools
import sys as _sys
import os as _os
import os.path as _op
import tempfile as _tf
import getpass as _gp
from colorama import Fore
from colorama import init as _colorama_init

from mt import traceback, shutil, ctx, tp

_colorama_init()


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
    "warn_uncaught_exception",
    "awarn_uncaught_exception",
    "return_exception",
    "areturn_exception",
]


# -----------------------------------------------------------------------------
# base implementation
# -----------------------------------------------------------------------------


class IndentedFilter(Filter):
    """Indented filter for indented logger adapter.

    A logging filter that never rejects a record but stamps it with attribute `indent`, copied
    from the parent :class:`IndentedLoggerAdapter`. Handlers can use it to filter by depth.

    Parameters
    ----------
    indented_logger_adapter : IndentedLoggerAdapter
        the adapter whose current `indent` is stamped on each record
    """

    def __init__(self, indented_logger_adapter):
        self.parent = indented_logger_adapter

    def filter(self, record):
        record.indent = self.parent.indent
        return True


class IndentedLoggerAdapter(LoggerAdapter):
    r"""Logger with indenting capability.

    A :class:`logging.LoggerAdapter` that prefixes every message with two spaces per indent level
    (see :class:`ScopedLog`, which raises and lowers the level around a ``with`` block), splits
    multi-line messages into one log record per line, and colors each message according to its
    level using `colorama` escape codes (so messages contain ANSI codes unless stripped by the
    formatter). Messages of a type other than `str` or `bytes` are converted with ``str()``. The
    methods `critical`, `error`, `warning` (alias `warn`), `info` and `debug` follow the interface
    of :class:`logging.Logger`; `scoped_critical`, `scoped_error`, `scoped_warning` (alias
    `scoped_warn`), `scoped_info` and `scoped_debug` return a :class:`ScopedLog` context manager.

    Creating an adapter attaches a filter to the wrapped logger, so create it once per logger, for
    instance with :func:`make_logger`.

    Parameters
    ----------
    logger : logging.Logger
        the logger to wrap
    extra : dict or None
        extra contextual information, as in :class:`logging.LoggerAdapter`

    Attributes
    ----------
    indent : int
        the current indent level, starting at 0
    last_exception : Exception or None
        the last exception processed by :class:`ScopedLog`, used to avoid logging the traceback of
        one exception several times as it propagates out of nested scopes

    Examples
    --------
    >>> import logging, re, sys
    >>> from mt import logg
    >>> class Plain(logging.Formatter):  # strips the ANSI color codes for display
    ...     def format(self, record):
    ...         return re.sub(r"\x1b\[\d+m", "", super().format(record))
    >>> lg = logg.IndentedLoggerAdapter(logging.getLogger("doc.indented"), None)
    >>> lg.logger.setLevel(logging.DEBUG)
    >>> handler = logging.StreamHandler(sys.stdout)
    >>> handler.setFormatter(Plain("%(levelname)s: %(message)s"))
    >>> lg.logger.addHandler(handler)
    >>> lg.info("line one\nline two")
    INFO: line one
    INFO: line two
    >>> lg.inc()
    >>> lg.warning("nested")
    WARNING:   nested
    >>> lg.dec()
    >>> with lg.scoped_debug("scope"):
    ...     lg.error("inside")
    DEBUG: scope:
    ERROR:   inside
    """

    def __init__(self, logger, extra):
        super().__init__(logger, extra)
        self.indent = 0
        self.last_exception = None

        # add a filter that adds 'indent' to every log record
        f = IndentedFilter(self)
        self.logger.addFilter(f)

    def process(self, msg: tp.Union[str, bytes], kwds):
        return (
            ("  " * self.indent + msg.decode(), kwds)
            if isinstance(msg, bytes)
            else ("  " * self.indent + msg, kwds)
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

    def critical(self, msg: tp.Union[str, bytes], *args, **kwds):
        if not isinstance(msg, (str, bytes)):
            msg = str(msg)
        for m in msg.split(b"\n") if isinstance(msg, bytes) else msg.split("\n"):
            super(IndentedLoggerAdapter, self).critical(
                Fore.LIGHTRED_EX + m, *args, **kwds
            )

    def error(self, msg: tp.Union[str, bytes], *args, **kwds):
        if not isinstance(msg, (str, bytes)):
            msg = str(msg)
        for m in msg.split(b"\n") if isinstance(msg, bytes) else msg.split("\n"):
            super(IndentedLoggerAdapter, self).error(
                Fore.LIGHTMAGENTA_EX + m, *args, **kwds
            )

    def warning(self, msg: tp.Union[str, bytes], *args, **kwds):
        if not isinstance(msg, (str, bytes)):
            msg = str(msg)
        for m in msg.split(b"\n") if isinstance(msg, bytes) else msg.split("\n"):
            super(IndentedLoggerAdapter, self).warning(
                Fore.LIGHTYELLOW_EX + m, *args, **kwds
            )

    warn = warning

    def info(self, msg: tp.Union[str, bytes], *args, **kwds):
        if not isinstance(msg, (str, bytes)):
            msg = str(msg)
        for m in msg.split(b"\n") if isinstance(msg, bytes) else msg.split("\n"):
            super(IndentedLoggerAdapter, self).info(
                Fore.LIGHTWHITE_EX + m, *args, **kwds
            )

    def debug(self, msg: tp.Union[str, bytes], *args, **kwds):
        if not isinstance(msg, (str, bytes)):
            msg = str(msg)
        for m in msg.split(b"\n") if isinstance(msg, bytes) else msg.split("\n"):
            super(IndentedLoggerAdapter, self).debug(
                Fore.LIGHTBLUE_EX + m, *args, **kwds
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

    def warn_last_exception(self, **kwds):
        """Warns the exception currently being handled, printing out any keyword arguments.

        It must be called from within an ``except`` block. The traceback is logged line by line at
        WARNING level, followed by each keyword argument in its own scope.

        Parameters
        ----------
        **kwds : dict
            additional keyword arguments to be included in the warning message
        """

        lines = traceback.format_exc_info(*_sys.exc_info())
        for x in lines:
            self.warning(x)

        if kwds:
            self.warning("Keyword arguments")
            self.warning("-----------------")
            for key, value in kwds.items():
                with self.scoped_warning(f"{key}"):
                    self.warning(value)

    def warn_exception(self, e: Exception, **kwds):
        """Warns an exception by raising and catching it, printing out any keyword arguments.

        Unlike :meth:`warn_last_exception`, it can be called outside of an ``except`` block.

        Parameters
        ----------
        e : Exception
            exception to print out as warning message(s)
        **kwds : dict
            additional keyword arguments to be included in the warning message
        """

        try:
            raise e
        except:
            self.warn_last_exception(**kwds)

    def warn_module_move(self, old_module: str, new_module: str):
        """Warns that an old module has been moved to a new module.

        It logs a deprecation message followed by two lines of the compact call stack (the code
        that imported the old module).

        Parameters
        ----------
        old_module : str
            short string representing the old module
        new_module : str
            short string representing the new module
        """
        self.warn(
            f"IMPORT: module '{old_module}' has been deprecated. "
            f"Please use module '{new_module}' instead."
        )
        lines = traceback.extract_stack_compact()
        if len(lines) > 9:
            selected_lines = lines[-9:-7]
        else:
            selected_lines = lines
        for x in selected_lines:
            self.warn(x)

    def warn_func_move(self, old_func: str, new_func: str):
        """Warns that an old function has been moved to a new function.

        It logs a deprecation message followed by two lines of the compact call stack (the code
        that called the old function).

        Parameters
        ----------
        old_func : str
            short string representing the old function
        new_func : str
            short string representing the new function
        """
        msg = (
            f"IMPORT: function '{old_func}()' has been deprecated. "
            f"Please use function '{new_func}()' instead."
        )
        self.warn(msg)
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


def record_factory(*args, **kwds):
    record = old_factory(*args, **kwds)

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
    """Makes a logger with a colored stream handler.

    The logger (a :class:`logging.Logger` of the given name, wrapped in an
    :class:`IndentedLoggerAdapter`) captures every level, and has one stream handler (writing to
    standard error) at DEBUG level. That handler shows the timestamp, a truncated level name and
    the message truncated to the width of the terminal, and drops messages whose indent level is
    larger than `max_indent`. No file handler is attached; see :func:`prepare_file_handler`.

    Calling it twice with the same name wraps the same underlying logger and adds a second stream
    handler, so every message would be printed twice; keep the returned object instead.

    Parameters
    ----------
    logger_name : str
        name of the logger
    max_indent : int
        max number of indents shown by the handler. Default to 10.

    Returns
    -------
    IndentedLoggerAdapter
        the logger

    See Also
    --------
    prepare_file_handler : makes a file handler that can be added to ``logger.logger``
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
        s1 = f"{log_lvl_length}.{log_lvl_length}s "
        column_length -= log_lvl_length
        s5 = f"-{column_length}.{column_length}s"

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
    """Prepares a file handler for logging.

    Any existing file at the destination is deleted first. The handler thresholds at DEBUG level
    and writes lines made of a timestamp, process id, numeric log level and the message.

    Parameters
    ----------
    prefix : str
        used to name the default log file ``<prefix>.debug.log`` in the directory
        ``<tempdir>/<user>/.mtbase``. Ignored if `filepath` is provided.
    filepath : str, optional
        path of the log file. If None, the default one is used.

    Returns
    -------
    logging.FileHandler
        the handler. Add it to a logger with ``logger.logger.addHandler(handler)``, where `logger`
        is an :class:`IndentedLoggerAdapter`.
    """
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
    """Initialises the module if it has not been initialised.

    It creates directories ``~/.mtbase`` and ``<tempdir>/<user>/.mtbase`` and deletes the file
    ``debug.log`` in the latter, if any. It is called by :func:`make_logger` and
    :func:`prepare_file_handler`, so users rarely need to call it. It does nothing the second
    time it is called.
    """
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
    """Wrapper that adds keyword 'logger=logger' to the input function.

    Parameters
    ----------
    func : callable
        a function accepting keyword argument `logger`
    logger : IndentedLoggerAdapter, optional
        the logger to bind. Note that the default is None, not the module-level logger.

    Returns
    -------
    functools.partial
        ``func`` with keyword argument `logger` bound to the given logger

    Examples
    --------
    >>> from mt import logg
    >>> def f(x, logger=None):
    ...     return (x, logger)
    >>> logg.with_logger(f)(1)
    (1, None)
    """
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
    **kwds,
):
    """Wraps :func:`logging.log` with additional logger keyword.

    Note that `logger` is the third positional parameter, so extra `*args` for message formatting
    can only be passed after it. Do not pass an unrelated positional value before it.

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
    **kwds : dict
        keyword arguments passed as-is to :func:`logging.log`.

    Examples
    --------
    >>> from mt import logg
    >>> logg.log(logg.INFO, "not logged anywhere", logger=None)
    """
    if logger:
        logger.log(level, msg, *args, **kwds)


def critical(
    msg: tp.Union[str, bytes], logger: tp.Optional[Logger] = logger, *args, **kwds
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
    **kwds : dict
        keyword arguments passed as-is to :func:`logging.critical`.
    """
    if logger:
        logger.critical(msg, *args, **kwds)


def error(
    msg: tp.Union[str, bytes], logger: tp.Optional[Logger] = logger, *args, **kwds
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
    **kwds : dict
        keyword arguments passed as-is to :func:`logging.error`.
    """
    if logger:
        logger.error(msg, *args, **kwds)


def warning(
    msg: tp.Union[str, bytes], logger: tp.Optional[Logger] = logger, *args, **kwds
):
    """Wraps :func:`logging.warning` with additional logger keyword.

    Parameters
    ----------
    msg : str or bytes
        message. Passed as-is to :func:`logging.warning`.
    logger : logging.Logger, optional
        which logger to process the message. Default is the default logger of mtbase. If None is
        provided, no message will be logged. `warn` is an alias of this function.
    *args : tuple
        positional arguments passed as-is to :func:`logging.warning`.
    **kwds : dict
        keyword arguments passed as-is to :func:`logging.warning`.
    """
    if logger:
        logger.warning(msg, *args, **kwds)


warn = warning


def info(
    msg: tp.Union[str, bytes], logger: tp.Optional[Logger] = logger, *args, **kwds
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
    **kwds : dict
        keyword arguments passed as-is to :func:`logging.info`.
    """
    if logger:
        logger.info(msg, *args, **kwds)


def debug(
    msg: tp.Union[str, bytes], logger: tp.Optional[Logger] = logger, *args, **kwds
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
    **kwds : dict
        keyword arguments passed as-is to :func:`logging.debug`.
    """
    if logger:
        logger.debug(msg, *args, **kwds)


class ScopedLog:
    r"""Scoped-log a message.

    A context manager that logs a message when entering a ``with`` block and increases the indent
    level of the logger, so everything logged inside the block is indented by one more level. On
    exit the indent level is restored. With ``curly=True`` the message is logged as ``{ msg``
    on entering and ``} msg`` on exit; otherwise it is logged as ``msg:`` on entering only.

    If an exception leaves the block, its traceback is logged at DEBUG level, one line at a time
    prefixed with ``##``, once per exception even if it passes through several nested scopes. The
    exception itself is not suppressed.

    The functions :func:`scoped_log`, :func:`scoped_info` etc. are shortcuts for it.

    Examples
    --------
    >>> import logging, re, sys
    >>> from mt import logg
    >>> class Plain(logging.Formatter):  # strips the ANSI color codes for display
    ...     def format(self, record):
    ...         return re.sub(r"\x1b\[\d+m", "", super().format(record))
    >>> lg = logg.IndentedLoggerAdapter(logging.getLogger("doc.scoped"), None)
    >>> lg.logger.setLevel(logging.DEBUG)
    >>> handler = logging.StreamHandler(sys.stdout)
    >>> handler.setFormatter(Plain("%(message)s"))
    >>> lg.logger.addHandler(handler)
    >>> with logg.ScopedLog(lg, logg.DEBUG, 'hello world'):
    ...     lg.info("Hi there")
    hello world:
      Hi there
    >>> with logg.ScopedLog(lg, logg.DEBUG, 'hello world', curly=True):
    ...     lg.info("Hi there")
    { hello world
      Hi there
    } hello world
    """

    def __init__(
        self,
        indented_logger_adapter,
        level,
        msg: tp.Union[str, bytes],
        curly: bool = False,
    ):
        """Scopes a log message.

        Parameters
        ----------
        indented_logger_adapter : IndentedLoggerAdapter or None
            the logger. If None then does nothing, just passing things through.
        level : int
            logging level, one of `logging.CRITICAL`, `ERROR`, `WARNING`, `INFO` or `DEBUG`
        msg : str
            message to log
        curly : bool
            whether or not to print a curly bracket

        Raises
        ------
        ValueError
            if the logger is neither None nor an :class:`IndentedLoggerAdapter`, or if `level` is
            not one of the five levels above
        """
        self.logger = indented_logger_adapter
        if self.logger is not None and not isinstance(
            self.logger, IndentedLoggerAdapter
        ):
            raise ValueError(
                "Argument `indented_logger_adapter` is expected to be an IndentedLoggerAdapter "
                "or None only."
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
            raise ValueError(f"Unknown debugging level {level}.")

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
                        self.logger.debug(f"##{line}")

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
    `scoped_warning`, `scoped_warn`, `scoped_info` and `scoped_debug`; they have `level` already
    bound, so ``logg.scoped_info("msg")`` equals ``logg.scoped_log(logg.INFO, "msg")``.

    Parameters
    ----------
    level : int
        level. Passed as-is to :class:`ScopedLog`.
    msg : str or bytes
        message. Passed as-is to :class:`ScopedLog`.
    logger : IndentedLoggerAdapter, optional
        which logger to process the message. Default is the default logger of mtbase. If None is
        provided, a null context is returned.
    curly : bool
        whether or not to print a curly bracket. Passed as-is to :class:`ScopedLog`.

    Returns
    -------
    ScopedLog or contextlib.nullcontext
        a context manager to be used in a ``with`` statement

    Examples
    --------
    >>> from mt import logg
    >>> with logg.scoped_info("this is skipped", logger=None):
    ...     print("body runs")
    body runs
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
    func_kwds: dict = {},
    return_value_if_false=None,
):
    if not cond:
        return return_value_if_false
    with scoped_log(indented_logger_adapter, level, msg=msg, curly=curly):
        return func(*func_args, **func_kwds)


def warn_uncaught_exception(func):
    """A decorator that warns any uncaught exception when invoking a function accepting keyword
    argument 'logger'.

    The wrapped function is called with the `logger` keyword argument (None if the caller did not
    provide it). If it raises an exception and the logger is not None, the exception is logged with
    :meth:`IndentedLoggerAdapter.warn_last_exception`. The exception is always re-raised.

    See Also
    --------
    awarn_uncaught_exception : the version for async functions

    Examples
    --------
    >>> from mt import logg
    >>> @logg.warn_uncaught_exception
    ... def f(x, logger=None):
    ...     return x + 1
    >>> f(1)
    2
    """

    @functools.wraps(func)
    def func_wrapper(*args, logger=None, **kwds):
        try:
            return func(*args, **kwds, logger=logger)
        except Exception:
            if logger:
                logger.warn_last_exception()
            raise

    return func_wrapper


def awarn_uncaught_exception(func):
    """A decorator that warns any uncaught exception when invoking an async function accepting
    keyword argument 'logger'.

    The async counterpart of :func:`warn_uncaught_exception`: the exception is logged (if a logger
    is provided) and re-raised.
    """

    @functools.wraps(func)
    async def func_wrapper(*args, logger=None, **kwds):
        try:
            return await func(*args, **kwds, logger=logger)
        except Exception:
            if logger:
                logger.warn_last_exception()
            raise

    return func_wrapper


def return_exception(func):
    """A decorator that catches any raised exception from the function and returns it.

    Only subclasses of :class:`Exception` are caught. The caller can tell success from failure by
    checking whether the result is an exception instance.

    See Also
    --------
    areturn_exception : the version for async functions

    Examples
    --------
    >>> from mt import logg
    >>> @logg.return_exception
    ... def f(x):
    ...     return 1 / x
    >>> f(2)
    0.5
    >>> f(0)
    ZeroDivisionError('division by zero')
    """

    @functools.wraps(func)
    def func_wrapper(*args, **kwds):
        try:
            return func(*args, **kwds)
        except Exception as e:
            return e

    return func_wrapper


def areturn_exception(func):
    """A decorator that catches any raised exception from the async function and returns it.

    The async counterpart of :func:`return_exception`.
    """

    @functools.wraps(func)
    async def func_wrapper(*args, **kwds):
        try:
            return await func(*args, **kwds)
        except Exception as e:
            return e

    return func_wrapper
