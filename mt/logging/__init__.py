"""Customised logging.

Note
----
Since version 3.6, this module has been deprecated and replaced by :module:`mt.logg`.

This module extends Python's package `logging`_ with some customisation made specifically for MT's
code. Instead of:

.. code-block:: python

   import logging

You do:

.. code-block:: python

   from mt import logging

It will import the logging package plus the additional stuff implemented here.

Please see Python package `logging`_ for more details.

.. _logging:
   https://docs.python.org/3/library/logging.html
"""

from mt.logg import *

from mt import tp, logg
from mt.base import deprecated_func

logger.warn_module_move("mt.logging", "mt.logg")


__api__ = [
    "critical",
    "error",
    "info",
    "warning",
    "warn",
    "debug",
    "scoped_critical",
    "scoped_error",
    "scoped_info",
    "scoped_warn",
    "scoped_warning",
    "scoped_debug",
]


# convenient log functions
@deprecated_func(
    "3.6",
    suggested_func="mt.logg.critical",
    removed_version="4.0",
    docstring_prefix="    ",
)
def critical(logger, msg: tp.Union[str, bytes], *args, **kwargs):
    return logg.critical(msg, *args, logger=logger, **kwargs)


@deprecated_func(
    "3.6",
    suggested_func="mt.logg.error",
    removed_version="4.0",
    docstring_prefix="    ",
)
def error(logger, msg: tp.Union[str, bytes], *args, **kwargs):
    return logg.error(msg, *args, logger=logger, **kwargs)


@deprecated_func(
    "3.6", suggested_func="mt.logg.warn", removed_version="4.0", docstring_prefix="    "
)
def warning(logger, msg: tp.Union[str, bytes], *args, **kwargs):
    return logg.warning(msg, *args, logger=logger, **kwargs)


warn = warning


@deprecated_func(
    "3.6", suggested_func="mt.logg.info", removed_version="4.0", docstring_prefix="    "
)
def info(logger, msg: tp.Union[str, bytes], *args, **kwargs):
    return logg.info(msg, *args, logger=logger, **kwargs)


@deprecated_func(
    "3.6",
    suggested_func="mt.logg.debug",
    removed_version="4.0",
    docstring_prefix="    ",
)
def debug(logger, msg: tp.Union[str, bytes], *args, **kwargs):
    return logg.debug(msg, logger=logger, *args, **kwargs)


# convenient scoped-log functions
@deprecated_func(
    "3.6",
    suggested_func="mt.logg.scoped_critical",
    removed_version="4.0",
    docstring_prefix="    ",
)
def scoped_critical(
    indented_logger_adapter, msg: tp.Union[str, bytes], curly: bool = False
):
    return logg.scoped_critical(msg, logger=indented_logger_adapter, curly=curly)


@deprecated_func(
    "3.6",
    suggested_func="mt.logg.scoped_error",
    removed_version="4.0",
    docstring_prefix="    ",
)
def scoped_error(
    indented_logger_adapter, msg: tp.Union[str, bytes], curly: bool = False
):
    return logg.scoped_error(msg, logger=indented_logger_adapter, curly=curly)


@deprecated_func(
    "3.6",
    suggested_func="mt.logg.scoped_warn",
    removed_version="4.0",
    docstring_prefix="    ",
)
def scoped_warning(
    indented_logger_adapter, msg: tp.Union[str, bytes], curly: bool = False
):
    return logg.scoped_warn(msg, logger=indented_logger_adapter, curly=curly)


scoped_warn = scoped_warning


@deprecated_func(
    "3.6",
    suggested_func="mt.logg.scoped_info",
    removed_version="4.0",
    docstring_prefix="    ",
)
def scoped_info(
    indented_logger_adapter, msg: tp.Union[str, bytes], curly: bool = False
):
    return logg.scoped_info(msg, logger=indented_logger_adapter, curly=curly)


@deprecated_func(
    "3.6",
    suggested_func="mt.logg.scoped_debug",
    removed_version="4.0",
    docstring_prefix="    ",
)
def scoped_debug(
    indented_logger_adapter, msg: tp.Union[str, bytes], curly: bool = False
):
    return logg.scoped_debug(msg, logger=indented_logger_adapter, curly=curly)
    return scoped_log(indented_logger_adapter, INFO, msg=msg, curly=curly)
