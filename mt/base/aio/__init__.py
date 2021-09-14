'''Useful subroutines dealing with asyn and asynch functions.

An asyn function is a coroutine function (declared with 'async def') but can operate in one of two
modes: asynchronously and synchronously, specified by a boolean keyword argument 'asyn'.
Asynchronicity means as usual an asynchronous function declared with 'async def'. However,
synchronicity means that the function can be invoked without an event loop, via invoking
:func:`srun`. Asyn functions are good for building library subroutines supporting both asynchronous
and synchronous modes, but they break backward compatibility because of their 'async' declaration.

An asynch function is a normal function (declared with 'def') but can operate in one of two modes:
asynchronously and synchronously, specified by a boolean keyword argument 'asynch'. When in
asynchronous mode, the function returns a coroutine that must be intercepted with keyword 'await',
as if this is a coroutine function. When in synchronous mode, the function behaves like a normal
function. Asynch functions are good for backward compatibility, because they are normal functions
that can pretend to be a coroutine function, but they are bad for developing library subroutines
supporting both asynchronous and synchronous modes.

An asyn or asynch function may accept keyword argument 'context_vars', a dictionary of context
variables supporting the function in either asynchronous mode or synchronous mode. The values in
'context_vars' are usually the enter-results of either asynchronous or normal with statements.

It is discouraged to implement an asynch function. You should only do so if you have no other
choice.
'''

from .base import *
from .files import *
from .multiprocessing import *
