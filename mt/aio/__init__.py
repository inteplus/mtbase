"""Useful subroutines dealing with asyn and asynch functions.

Asyn functions and asynch functions are functions that can operate either asynchronously or
synchronously. Asynchronicity means like a coroutine function that can yield control back to the
current event loop. Synchronicity means it does not yield control during its execution. Both asyn
and functions accept a keyword argument 'context_vars' which is a dictionary of context variables.
One of which must be a boolean variable 'async' telling whether the function is to be executed in
asynchronous mode (`context_vars['async']` is True) or in synchronous mode
(`context_vars['async']` is False). There can be other context variables, they are usually the
enter-results of either asynchronous or normal with statements.

An asyn function is declared with 'async def'. It can be invoked without an event loop, via
invoking :func:`mt.aio.srun`. Asyn functions are good for building library subroutines supporting
both asynchronous and synchronous modes, but they break backward compatibility because of their
'async' declaration requirement.

An asynch function is declared with 'def' like a normal function. When in asynchronous mode, the
function returns a coroutine that must be intercepted with keyword 'await', as if it is a coroutine
function. When in synchronous mode, the function behaves like a normal function. Asynch functions
are good for backward compatibility, because they are normal functions that can pretend to be a
coroutine function, but they are bad for developing library subroutines supporting both
asynchronous and synchronous modes.

It is discouraged to implement an asynch function. You should only do so if you have no other
choice.
"""

from .base import *
from .files import *
from .multiprocessing import *
from .procedure import *

__api__ = [
    "ipython",  # for backward compatibility
    "srun",
    "arun",
    "arun2",
    "sleep",
    "yield_control",
    "safe_chmod",
    "safe_rename",
    "read_binary",
    "write_binary",
    "read_text",
    "write_text",
    "json_load",
    "json_save",
    "mkdtemp",
    "CreateFileH5",
    "qput_aio",
    "qget_aio",
    "BgProcess",
    "AprocManager",
]
