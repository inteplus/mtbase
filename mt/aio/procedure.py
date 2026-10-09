"""Asynchronous procedure.

An asynchronous procedure, a.k.a. an aproc, is a procedure that is asynchronous and has been
wrapped into an :class:`asyncio.Future`. A procedure is a function that returns None.
"""


import asyncio


class AprocManager:
    """Manages the completion of aprocs.

    With this manager, the user can just send an aproc to it and forget. To ensure all aprocs
    are completed, please invoke the cleanup function. Otherwise, some aprocs may never get
    awaited when the manager dies.

    Exceptions raised by aprocs are only inspected when the manager waits for some aprocs to
    finish, i.e. inside :meth:`send` when the pending set is full and inside :meth:`cleanup`.

    Parameters
    ----------
    max_concurrency : int
        maximum number of concurrent aprocs that can be held pending
    handle_exception : {'raise', 'silent', 'warn'}
        policy for handling an exception raised by (or the cancellation of) an aproc. If 'raise',
        re-raise the caught exception. If 'silent', ignore the exception. If 'warn', use the
        provided logger to warn the user.
    logger : logging.Logger or equivalent
        logger for warning purposes. Required if `handle_exception` is 'warn'.

    Raises
    ------
    ValueError
        if `handle_exception` is 'warn' but no logger is provided

    Examples
    --------
    >>> import asyncio
    >>> from mt.aio import AprocManager
    >>> results = []
    >>> async def proc(i):
    ...     await asyncio.sleep(0.01)
    ...     results.append(i)
    >>> async def main():
    ...     manager = AprocManager(max_concurrency=2)
    ...     for i in range(5):
    ...         await manager.send(asyncio.ensure_future(proc(i)))
    ...     await manager.cleanup()
    >>> asyncio.run(main())
    >>> sorted(results)
    [0, 1, 2, 3, 4]
    """

    def __init__(
        self, max_concurrency: int = 1024, handle_exception: str = "raise", logger=None
    ):
        self.max_concurrency = max_concurrency
        self.aproc_set = set()
        self.handle_exception = handle_exception
        self.logger = logger
        if handle_exception == "warn" and logger is None:
            raise ValueError(
                "A logger must be provided if keyword 'handle_exception' is set to 'warn'."
            )

    async def _sleep_well(self, max_concurrency=None):
        """Waits until fewer aprocs than the limit are pending, applying the exception policy.

        If `max_concurrency` is None, the limit is `self.max_concurrency`. Any other value makes
        the limit 1, i.e. waits until no aproc is pending.
        """
        max_concurrency = self.max_concurrency if max_concurrency is None else 1
        while len(self.aproc_set) >= max_concurrency:
            done_set, completed_set = await asyncio.wait(
                self.aproc_set, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done_set:
                if task.cancelled():
                    if self.handle_exception == "raise":
                        raise asyncio.CancelledError("An aproc has been cancelled.")
                    if self.handle_exception == "warn":
                        self.logger.warn(
                            f"An aproc has been cancelled: {task}."
                        )
                elif task.exception() is not None:
                    if self.handle_exception == "raise":
                        raise task.exception()
                    if self.handle_exception == "warn":
                        self.logger.warn(
                            "An exception has been caught (and ignored) in an aproc."
                        )
                        self.logger.warn(str(task.exception()))
            self.aproc_set = completed_set

    async def send(self, aproc: asyncio.Future):
        """Sends an aproc to the manager so the user can forget about it.

        The function usually returns immediately. However, if the maximum number of concurrent
        aprocs has been reached, it awaits until some aprocs finish. While doing so, it applies
        the exception policy to the finished aprocs (it may raise with policy 'raise').

        Parameters
        ----------
        aproc : asyncio.Future
            a future (returned via :func:`asyncio.create_task` or :func:`asyncio.ensure_future`)
            that is a procedure
        """
        await self._sleep_well()
        self.aproc_set.add(aproc)

    async def cleanup(self):
        """Awaits until all aprocs are done.

    The exception policy is applied to the aprocs that finish in the meantime, so with policy
    'raise' the first failed or cancelled aproc found makes this function raise. Call it before
    discarding the manager.
    """
        await self._sleep_well(1)
