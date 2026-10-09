"""Useful subroutines dealing with asyn functions from another process.

It provides asyncio-friendly versions of queue put/get, which poll instead of blocking the event
loop, and :class:`BgProcess`, a child process driven by message passing.
"""


import asyncio
import os
import queue
import multiprocessing as mp
import multiprocessing.queues as mq

from .base import yield_control


async def qput_aio(
    q: mq.Queue,
    obj,
    block: bool = True,
    timeout: float = None,
    aio_interval: float = 0.001,
):
    """Puts obj into the queue q.

    If the optional argument `block` is True (the default) and `timeout` is None (the default),
    block asynchronously if necessary until a free slot is available. If timeout is a positive
    number, it blocks asynchronously at most timeout seconds and raises the :class:`queue.Full`
    exception if no free slot was available within that time. Otherwise (`block` is False),
    put an item on the queue if a free slot is immediately available, else raise the
    :class:`queue.Full` exception (timeout is ignored in that case).

    Parameters
    ----------
    q : multiprocessing.Queue
        the queue to put into. Any object with `full()` and `put(obj, block=...)` methods, such as
        a :class:`queue.Queue`, also works.
    obj : object
        the item to put
    block : bool
        whether to wait asynchronously for a free slot
    timeout : float, optional
        maximum number of seconds to wait for a free slot. Only used if `block` is True.
    aio_interval : float
        number of seconds to sleep (asynchronously) between two checks of the queue. Default is
        1ms.

    Raises
    ------
    queue.Full
        if no free slot became available in time

    See Also
    --------
    qget_aio : the counterpart for getting an item

    Examples
    --------
    >>> import asyncio, queue
    >>> from mt.aio import qget_aio, qput_aio
    >>> async def main():
    ...     q = queue.Queue(maxsize=1)
    ...     await qput_aio(q, "a")
    ...     try:
    ...         await qput_aio(q, "b", timeout=0.01)
    ...     except queue.Full:
    ...         print("full")
    ...     return await qget_aio(q)
    >>> asyncio.run(main())
    full
    'a'
    """

    if not block:
        return q.put(obj, block=False)

    if timeout is None:
        while q.full():
            await asyncio.sleep(aio_interval)
        return q.put(obj, block=True)

    cnt = int(timeout / aio_interval) + 1
    while cnt > 0:
        if not q.full():
            return q.put(obj, block=True)
        await asyncio.sleep(aio_interval)
        cnt -= 1
    raise queue.Full()


async def qget_aio(
    q: mq.Queue, block: bool = True, timeout: float = None, aio_interval: float = 0.001
):
    """Removes and returns an item from the queue q.

    If optional args `block` is True (the default) and `timeout` is None (the default), block
    asynchronously if necessary until an item is available. If `timeout` is a positive number,
    it blocks asynchronously at most timeout seconds and raises the :class:`queue.Empty`
    exception if no item was available within that time. Otherwise (`block` is False), return
    an item if one is immediately available, else raise the :class:`queue.Empty` exception
    (`timeout` is ignored in that case).

    Parameters
    ----------
    q : multiprocessing.Queue
        the queue to get from. Any object with `empty()` and `get(block=...)` methods, such as
        a :class:`queue.Queue`, also works.
    block : bool
        whether to wait asynchronously for an item
    timeout : float, optional
        maximum number of seconds to wait for an item. Only used if `block` is True.
    aio_interval : float
        number of seconds to sleep (asynchronously) between two checks of the queue. Default is
        1ms.

    Returns
    -------
    object
        the item removed from the queue

    Raises
    ------
    queue.Empty
        if no item became available in time

    See Also
    --------
    qput_aio : the counterpart for putting an item

    Examples
    --------
    >>> import asyncio, queue
    >>> from mt.aio import qget_aio
    >>> async def main():
    ...     q = queue.Queue()
    ...     try:
    ...         await qget_aio(q, block=False)
    ...     except queue.Empty:
    ...         return "empty"
    >>> asyncio.run(main())
    'empty'
    """

    if not block:
        return q.get(block=False)

    if timeout is None:
        while q.empty():
            await asyncio.sleep(aio_interval)
        return q.get(block=True)

    cnt = int(timeout / aio_interval) + 1
    while cnt > 0:
        if not q.empty():
            return q.get(block=True)
        await asyncio.sleep(aio_interval)
        cnt -= 1
    raise queue.Empty()


class BgProcess:
    """Launches a child process that communicates with the parent process via message passing.

    You should subclass this class and implement :func:`child_handle_message`. See the docstring
    of the function below. The child process is started (via :class:`multiprocessing.Process`)
    when the instance is constructed. The parent talks to it with the asyn method :meth:`send`,
    one message at a time. Anything the child prints is relayed to the parent's stdout, line by
    line, prefixed with 'BgProcess (stdout): ' or 'BgProcess (stderr): '. The child exits when
    the parent calls :meth:`close` (also invoked on deletion), or when the parent process no
    longer exists.

    Attributes
    ----------
    msg_p2c : multiprocessing.Queue
        parent-to-child message queue
    msg_c2p : multiprocessing.Queue
        child-to-parent message queue
    child_process : multiprocessing.Process
        the child process

    Notes
    -----
    In interactive mode, remember to delete any instance of the the class when you exit or else it
    will not exit.

    The string message "exit" is reserved to stop the child process.

    Examples
    --------
    .. code-block:: python

        import asyncio
        from mt.aio import BgProcess

        class Squarer(BgProcess):
            def child_handle_message(self, msg):
                return msg * msg

        async def main():
            proc = Squarer()
            try:
                return await proc.send(3)  # 9
            finally:
                proc.close()

        asyncio.run(main())
    """

    def __init__(self):
        self.msg_p2c = mp.Queue()
        self.msg_c2p = mp.Queue()
        self.msg_cnt = 0
        self.parent_pid = os.getpid()
        self.child_process = mp.Process(target=self._worker_process)
        self.child_process.start()
        self.sending = False

    def __del__(self):
        self.close()

    async def send(self, msg, recv_timeout: float = None, recv_aio_interval=0.001):
        """Sends a message to the child process and awaits for the returning message.

        Concurrent callers are serialised: while one message is in flight, other calls wait.
        Messages printed by the child in the meantime are echoed to the parent's stdout.

        Parameters
        ----------
        msg : object
            message to be sent to the child process
        recv_timeout : float
            If specified, the number of seconds to wait asynchronously to receive the message,
            before raising a :class:`queue.Empty` exception. If not, asynchronously blocks until
            the message from the child process is received.
        recv_aio_interval : float
            time unit to simulate asynchronous blocking while waiting for message response. Default
            is 1ms.

        Returns
        -------
        object
            message received from the child process

        Raises
        ------
        RuntimeError
            if the child process is not alive while processing the message, or if
            :func:`child_handle_message` raised an exception (the exception and the child's
            call stack are in the arguments of the error)
        queue.Empty
            if `recv_timeout` is specified and no response arrived in time
        """

        while self.sending:
            await yield_control()

        try:
            self.sending = True
            self.msg_p2c.put_nowait(msg)
            while True:
                retval = await qget_aio(
                    self.msg_c2p, timeout=recv_timeout, aio_interval=recv_aio_interval
                )
                if retval[0] == "ignored_exception":
                    continue
                if retval[0] == "write":  # child printing something
                    for line in retval[2].splitlines():
                        print(f"BgProcess ({retval[1]}): {line}")
                    continue
                break
            if retval[0] == "exit":
                if retval[1] is None:
                    raise RuntimeError(
                        "Child process died normally while parent process is expecting "
                        "some message response.",
                        msg,
                    )
                else:
                    raise RuntimeError(
                        "Child process died abruptedly with an exception.",
                        retval[1],
                        msg,
                    )
            if retval[0] == "raised_exception":
                raise RuntimeError(
                    "Child raised an exception while processing the message.",
                    retval[1],
                    retval[2],
                )
        finally:
            self.sending = False
        return retval[1]

    def child_handle_message(self, msg: object) -> object:
        """Handles a message obtained from the queue.

        This function should only be called by the child process.

        It takes as input a message from the parent-to-child queue and processes the message.
        Once done, it returns an object which will be wrapped into a message and placed into the
        child-to-parent queue.

        An input message can be anything. Usually it is a tuple with the first component being
        a command string. The output message can also be anything. If the handle succeeds,
        the returning value is then wrapped into a `('returned', retval)` output message. If
        an exception is raised, it is wrapped into a `('raised_exception', exc, callstack_lines)`
        output message.

        If the child process prints anything to stdout our stderr, it will be redirected as
        `('write', 'stdout' or 'stderr', text)` in the output queue. Note that for now only
        Python-generated printouts can be redirected. Native printouts require more cumbersome
        solutions. See: https://exceptionshub.com/python-multiprocessing-how-can-i-reliably-redirect-stdout-from-a-child-process-2.html

        The user should override this function. The default behaviour is returning whatever
        sent to it.

        If KeyboardInterrupt is raised in the child process but outside this function, you will
        get a `('ignored_exception', KeyboardInterrupt)` message. If the child process dies
        you will get a `('exit', None or Exception)` message depending on whether the child process
        dies normally or abruptedly.

        Parameters
        ----------
        msg : object
            the message sent by the parent via :meth:`send`. It must not be the string "exit".

        Returns
        -------
        object
            the response, which :meth:`send` returns to the parent. It must be picklable.
        """
        return msg

    def _worker_process(self):
        """The main loop of the child process. Do not call it directly."""
        import psutil
        import queue
        import sys

        from mt import traceback

        class Writer:
            def __init__(self, msg_c2p, prefix):
                self.msg_c2p = msg_c2p
                self.prefix = prefix

            def write(self, text):
                self.msg_c2p.put_nowait(("write", self.prefix, text))

        sys.stderr = Writer(self.msg_c2p, "stderr")
        sys.stdout = Writer(self.msg_c2p, "stdout")

        while True:
            try:
                if self.parent_pid is not None:
                    if not psutil.pid_exists(self.parent_pid):
                        self.msg_c2p.put_nowait(
                            ("exit", RuntimeError("Parent does not exist."))
                        )
                        return

                try:
                    msg = self.msg_p2c.get(block=True, timeout=1)
                except queue.Empty:
                    continue

                if msg == "exit":
                    break

                try:
                    retval = self.child_handle_message(
                        msg
                    )  # handle the message and return
                    msg = ("returned", retval)
                except Exception as e:
                    msg = ("raised_exception", e, traceback.extract_stack_compact())
                self.msg_c2p.put_nowait(msg)
            except KeyboardInterrupt as e:
                self.msg_c2p.put_nowait(("ignored_exception", e))
            except Exception as e:
                self.msg_c2p.put_nowait(("exit", e))
                return

        self.msg_c2p.put_nowait(("exit", None))

    def close(self):
        """Asks the child process to exit, by sending it the reserved message "exit".

        It does not wait for the child process to terminate.
        """
        self.msg_p2c.put_nowait("exit")
