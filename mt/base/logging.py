"""Customised logging"""
from logging import *
import logging.handlers as _lh
import sys as _sys
import os as _os
import os.path as _op
import tempfile as _tf
import getpass as _gp
from colorama import Fore, Style
from colorama import init as _colorama_init
_colorama_init()

from .terminal import stty_size # for backward compatibility
from .traceback import format_exc_info as _tb_format_exc_info, format_list as _tb_format_list, extract_stack as _tb_extract_stack, extract_stack_compact as _tb_extract_stack_compact


__all__ = ['ScopedLog', 'scoped_critical', 'scoped_debug', 'scoped_error', 'scoped_info',
           'scoped_warn', 'scoped_warning', 'IndentedLoggerAdapter', 'make_logger',
           'prepare_file_handler', 'init', 'logger']


class ScopedLog:

    '''Scoped-log a message.

    >>> with ScopedLog(logger, logging.DEBUG, 'hello world'):
    ...     a = 1
    ...     logger.info("Hi there")
    { hello world
      Hi there
    } hello world

    '''

    def __init__(self, indented_logger_adapter, level, msg='', curly=True):
        '''Scope a log message.

        # Arguments
        indented_logger_adapter : IndentedLoggerAdapter or None
            the logger. If None then does nothing, just passing things through.
        level : obj
            logging level (e.g. logging.CRITICAL, logging.INFO, logging.DEBUG, etc)
        msg : str
            message to log
        curly : bool
            whether or not to print a curly bracket
        '''
        self.logger = indented_logger_adapter
        if self.logger is not None and not isinstance(self.logger, IndentedLoggerAdapter):
            raise ValueError("Argument `indented_logger_adapter` is expected to be an IndentedLoggerAdapter or None only.")

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
            self.func('{ '+self.msg if self.curly else self.msg+':')
            self.logger.inc()
        return self

    def __exit__(self, type, value, traceback):
        if self.logger:
            if self.logger.getLastException() != value:
                self.logger.setLastException(value)

                if type is not None:
                    lines = _tb_format_exc_info(type, value, traceback)
                    for line in lines:
                        self.logger.debug("##{}".format(line))

            self.logger.dec()
            if self.curly:
                self.func('} '+self.msg)


# convenient scoped-log functions
def scoped_critical(indented_logger_adapter, msg='', curly=True):
    return ScopedLog(indented_logger_adapter, CRITICAL, msg=msg, curly=curly)
def scoped_error(indented_logger_adapter, msg='', curly=True):
    return ScopedLog(indented_logger_adapter, ERROR, msg=msg, curly=curly)
def scoped_warning(indented_logger_adapter, msg='', curly=True):
    return ScopedLog(indented_logger_adapter, WARNING, msg=msg, curly=curly)
scoped_warn = scoped_warning
def scoped_info(indented_logger_adapter, msg='', curly=True):
    return ScopedLog(indented_logger_adapter, INFO, msg=msg, curly=curly)
def scoped_debug(indented_logger_adapter, msg='', curly=True):
    return ScopedLog(indented_logger_adapter, DEBUG, msg=msg, curly=curly)


class _IndentedFilter(Filter):
    '''Indented filter for indented logger adapter.'''

    def __init__(self, indented_logger_adapter):
        self.parent = indented_logger_adapter

    def filter(self, record):
        record.indent = self.parent.indent
        return True


class IndentedLoggerAdapter(LoggerAdapter):
    '''Logger with indenting capability.'''

    def __init__(self, logger, extra):
        super().__init__(logger, extra)
        self.indent = 0
        self.last_exception = None

        # add a filter that adds 'indent' to every log record
        f = _IndentedFilter(self)
        self.logger.addFilter(f)

    def process(self, msg, kwargs):
        return ("  "*self.indent + msg.decode(), kwargs) if isinstance(msg, bytes) else ("  "*self.indent + msg, kwargs)

    def inc(self):
        self.indent += 1

    def dec(self):
        self.indent -= 1

    def setLastException(self, value):
        self.last_exception = value

    def getLastException(self):
        return self.last_exception


    # ----- break mutli-line messages -----

    def critical(self, msg, *args, **kwargs):
        if not isinstance(msg, (str, bytes)):
            msg = str(msg)
        for m in msg.split(b'\n') if isinstance(msg, bytes) else msg.split('\n'):
            super(IndentedLoggerAdapter, self).critical(Fore.LIGHTRED_EX + m, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        if not isinstance(msg, (str, bytes)):
            msg = str(msg)
        for m in msg.split(b'\n') if isinstance(msg, bytes) else msg.split('\n'):
            super(IndentedLoggerAdapter, self).error(Fore.LIGHTMAGENTA_EX + m, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        if not isinstance(msg, (str, bytes)):
            msg = str(msg)
        for m in msg.split(b'\n') if isinstance(msg, bytes) else msg.split('\n'):
            super(IndentedLoggerAdapter, self).warning(Fore.LIGHTYELLOW_EX + m, *args, **kwargs)
    warn = warning

    def info(self, msg, *args, **kwargs):
        if not isinstance(msg, (str, bytes)):
            msg = str(msg)
        for m in msg.split(b'\n') if isinstance(msg, bytes) else msg.split('\n'):
            super(IndentedLoggerAdapter, self).info(Fore.LIGHTWHITE_EX + m, *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        if not isinstance(msg, (str, bytes)):
            msg = str(msg)
        for m in msg.split(b'\n') if isinstance(msg, bytes) else msg.split('\n'):
            super(IndentedLoggerAdapter, self).debug(Fore.LIGHTBLUE_EX + m, *args, **kwargs)

    # ----- scoped logging -----

    def scoped_critical(self, msg='', curly=True):
        return ScopedLog(self, CRITICAL, msg=msg, curly=curly)

    def scoped_error(self, msg='', curly=True):
        return ScopedLog(self, ERROR, msg=msg, curly=curly)

    def scoped_warning(self, msg='', curly=True):
        return ScopedLog(self, WARNING, msg=msg, curly=curly)
    scoped_warn = scoped_warning

    def scoped_info(self, msg='', curly=True):
        return ScopedLog(self, INFO, msg=msg, curly=curly)

    def scoped_debug(self, msg='', curly=True):
        return ScopedLog(self, DEBUG, msg=msg, curly=curly)

    # ----- useful warning messages -----

    def warn_last_exception(self):
        lines = _tb_format_exc_info(*_sys.exc_info())
        for x in lines:
            self.warning(x)

    def warn_module_move(self, old_module, new_module):
        '''Warns that an old module has been moved to a new module.

        Parameters
        ----------
        old_module : str
            short string representing the old module
        new_module : str
            short string representing the new module
        '''
        self.warn("IMPORT: module '{}' has been deprecated. Please use module '{}' instead.".format(old_module, new_module))
        lines = _tb_extract_stack_compact()
        if len(lines) > 9:
            selected_lines = lines[-9:-7]
        else:
            selected_lines = lines
        for x in selected_lines:
            self.warn(x)

    def warn_func_move(self, old_func, new_func):
        '''Warns that an old function has been moved to a new func.

        Parameters
        ----------
        old_func : str
            short string representing the old function
        new_func : str
            short string representing the new function
        '''
        self.warn("IMPORT: function '{}()' has been deprecated. Please use function '{}()' instead.".format(old_func, new_func))
        lines = _tb_extract_stack_compact()
        if len(lines) > 9:
            selected_lines = lines[-9:-7]
        else:
            selected_lines = lines
        for x in selected_lines:
            self.warn(x)

    def debug_call_stack(self):
        lines = _tb_format_list(_tb_extract_stack())
        lines = "".join(lines).split('\n')
        lines.pop(-2)
        lines.pop(-2)
        for x in lines:
            self.debug(x)

_format_str = Fore.CYAN+'%(asctime)s '+Fore.LIGHTGREEN_EX+'%(levelname)8s'+Fore.WHITE+': ['+Fore.LIGHTMAGENTA_EX+'%(name)s'+Fore.LIGHTWHITE_EX+'] %(message)s'+Fore.RESET
#_format_str = f'{Fore.CYAN}%(asctime)s {Fore.LIGHTGREEN_EX}%(levelname)8s{Fore.WHITE}: [{Fore.LIGHTMAGENTA_EX}%(name)s{Fore.LIGHTWHITE_EX}] %(message)s{Fore.RESET}'
#_format_str = '%(asctime)s %(levelname)8s: [%(name)s] %(message)s'


old_factory = getLogRecordFactory()

def record_factory(*args, **kwargs):
    record = old_factory(*args, **kwargs)

    frames = _tb_extract_stack()
    for frame in reversed(frames):
        if not 'logging' in frame.filename:
            record.pathname = frame.filename
            record.lineno = frame.lineno
            record.funcName = frame.name
            filename = _op.basename(frame.filename)
            record.filename = filename
            record.module = filename[:-3] if filename.endswith('.py') else filename
            break

    return record

setLogRecordFactory(record_factory)

def make_logger(logger_name, max_indent=4):
    '''Make a singleton logger.

    :Parameters:
        logger_name : str
            name of the logger
        max_indent : int
            max number of indents. Default to 4

    The generated logger has 2 handlers, one standard output and one watched file handler. Both handlers threshold at DEBUG. The standard handler further thresholds at `max_indent`. The debug handler thresholds at DEBUG level. The standard handler is brief, not showing thread id. The debug level is verbose, showing everything.

    '''
    init()

    logger = getLogger(logger_name)
    logger.setLevel(1) # capture everything but let the handlers decide
    logger = IndentedLoggerAdapter(logger, extra=None)

    # standard handler
    if True:
        class StdFilter(Filter):

            def __init__(self, max_indent=None, name=''):
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
        column_length = stty_size()[1]-20
        log_lvl_length = min(max(int(column_length*0.03), 1), 8)
        s1 = '{}.{}s '.format(log_lvl_length, log_lvl_length)
        column_length -= log_lvl_length
        s5 = '-{}.{}s'.format(column_length, column_length)

        fmt_str = Fore.CYAN+'%(asctime)s '+Fore.LIGHTGREEN_EX+'%(levelname)'+s1+\
            Fore.LIGHTWHITE_EX+'%(message)'+s5+Fore.RESET
        std_handler.setFormatter(Formatter(fmt_str))

        logger.logger.addHandler(std_handler)

    return logger


def prepare_file_handler(prefix='ml', filepath=None):
    '''Prepares a file handler for logging.'''
    init()
    if filepath is None:
        filepath = _op.join(init._temp_dirpath, prefix+'.debug.log')
    if _op.exists(filepath): # remove previous file
        _os.remove(filepath)
    file_handler = FileHandler(filepath)

    file_handler.setLevel(DEBUG)

    file_formatter = Formatter('%(asctime)s {pid=%(process)5d tid=%(thread)12x log_level=%(levelno)2d} %(message)s')
    file_handler.setFormatter(file_formatter)

    return file_handler


# -----------------------------------------------------------------------------
# module initialisation
# -----------------------------------------------------------------------------


def init():
    '''Initialises the module if it has not been initialised.'''
    if init._completed:
        return

    setattr(init, '_home_dirpath', _op.join(_op.expanduser('~'), '.mtbase'))
    _os.makedirs(init._home_dirpath, exist_ok=True)

    temp_dirpath = _op.join(_tf.gettempdir(), _gp.getuser(), '.mtbase')
    _os.makedirs(temp_dirpath, exist_ok=True)
    setattr(init, '_temp_dirpath', temp_dirpath)

    debuglog_filepath = _op.join(temp_dirpath, 'debug.log')
    if _op.exists(debuglog_filepath):
        _os.remove(debuglog_filepath)
    setattr(init, '_debuglog_filepath', debuglog_filepath)

    init._completed = True
init._completed = False

logger = make_logger("mtbase")


