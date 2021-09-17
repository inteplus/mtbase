'''Asynchronous procedure.

An asynchronous procedure, a.k.a. an aproc, is a procedure that is asynchronous and has been
wrapped into an :class:`asyncio.Future`. A procedure is a function that returns None.
'''


import asyncio


__all__ = ['AprocManager']


class AprocManager:
    '''Manages the completion of aprocs.

    With this manager, the user can just send an aproc to it and forget. To ensure all aprocs
    are completed, please invoke the cleanup function. Otherwise, some aprocs may never get
    awaited when the manager dies.

    Parameters
    ----------
    max_concurrency : int
        maximum number of concurrent aprocs that can be held pending
    handle_exception : {'raise', 'silent', 'warn'}
        policy for handling an exception raised by an aproc. If 'raise', re-raise the caught
        exception. If 'silent', ignore the exception. If 'warn', use the provided logger to
        warn the user.
    logger : logging.Logger or equivalent
        logger for warning purposes
    '''

    def __init__(self, max_concurrency: int = 1024, handle_exception: str = 'raise', logger=None):
        self.max_concurrency = max_concurrency
        self.aproc_set = set()
        self.handle_exception = handle_exception
        self.logger = logger
        if handle_exception == 'warn' and logger is None:
            raise ValueError("A logger must be provided if keyword 'handle_exception' is set to 'warn'.")


    async def _sleep_well(self, max_concurrency=None):
        max_concurrency = self.max_concurrency if max_concurrency is None else 1
        while len(self.aproc_set) >= max_concurrency:
            done_set, completed_set = await asyncio.wait(self.aproc_set, return_when=asyncio.FIRST_COMPLETED)
            for task in done_set:
                if task.cancelled():
                    if self.handle_exception == 'raise':
                        raise asyncio.CancelledError("An aproc has been cancelled.")
                    if self.handle_exception == 'warn':
                        self.logger.warn("An aproc has been cancelled: {}.".format(task))
                elif task.exception() is not None:
                    if self.handle_exception == 'raise':
                        raise task.exception()
                    if self.handle_exception == 'warn':
                        self.logger.warn("An exception has been caught (and ignored) in an aproc.")
                        self.logger.warn(str(task.exception()))
            self.aproc_set = completed_set


    async def send(self, aproc: asyncio.Future):
        '''Sends an aproc to the manager so the user can forget about it.

        The function usually returns immediately. However, if the maximum number of concurrent
        aprocs has been exceeded. It will await.

        Parameters
        ----------
        aproc : asyncio.Future
            a future (returned via :func:`asyncio.create_task` or :func:`asyncio.ensure_future`)
            that is a procedure
        '''
        await self._sleep_well()
        self.aproc_set.add(aproc)


    async def cleanup(self):
        '''Awaits until all aprocs are done.'''
        await self._sleep_well(1)
