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


import time
import json
import asyncio
import os
import queue
import multiprocessing as mp
import multiprocessing.queues as mq
import aiofiles


__all__ = ['srun', 'arun', 'arun2', 'sleep', 'read_binary', 'write_binary', 'read_text', 'write_text', 'json_load', 'json_save', 'yield_control', 'qput_aio', 'qget_aio', 'BgProcess']


def srun(asyn_func, *args, **kwargs) -> object:
    '''Invokes an asyn function synchronously, without using keyword 'await'.

    Parameters
    ----------
    asyn_func : function
        an asyn function taking 'asyn' as a keyword argument
    args : list
        postitional arguments to be passed to the function
    kwargs : dict
        other keyword arguments to be passed to the function

    Returns
    -------
    object
        whatever the function returns
    '''

    try:
        coro = asyn_func(*args, asyn=False, **kwargs)
        coro.send(None)
        coro.close()
    except StopIteration as e:
        return e.value


def arun(asyn_func, *args, asynch: bool = False, **kwargs) -> object:
    '''Invokes an asyn function from inside an asynch function.

    Parameters
    ----------
    asyn_func : function
        an asyn function (declared with 'async def')
    args : list
        positional arguments of the asyn function
    asynch : bool
        whether to invoke the function asynchronously (True) or synchronously (False)
    kwargs : dict
        keyword arguments of the asyn function

    Returns
    -------
    object
        whatver the asyn function returns
    '''

    async def async_func(*args, **kwargs):
        return await asyn_func(*args, **kwargs)

    def sync_func(*args, **kwargs):
        return srun(asyn_func, *args, **kwargs)

    func = async_func if asynch else sync_func
    return func(*args, **kwargs)


async def arun2(asynch_func, *args, asyn: bool = True, **kwargs) -> object:
    '''Invokes an asynch function from inside an asyn function.

    Parameters
    ----------
    asyn_func : function
        an asyn function (declared with 'async def')
    args : list
        positional arguments of the asyn function
    asynch : bool
        whether to invoke the function asynchronously (True) or synchronously (False)
    kwargs : dict
        keyword arguments of the asyn function

    Returns
    -------
    object
        whatver the asyn function returns
    '''

    if asyn:
        retval = await asynch_func(*args, asynch=True, **kwargs)
    else:
        retval = asynch_func(*args, asynch=False, **kwargs)
    return retval


async def sleep(secs: float, asyn: bool = True):
    '''An asyn function that sleeps for a number of seconds.

    In asynchronous mode, it invokes :func:`asyncio.sleep`. In synchronous mode, it invokes
    :func:`time.sleep`.

    Parameters
    ----------
    secs : float
        number of seconds to sleep
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously
    '''

    if asyn:
        await asyncio.sleep(secs)
    else:
        time.sleep(secs)


async def read_binary(filepath, size: int = None, asyn: bool = True):
    '''An asyn function that opens a binary file and reads the content.

    Parameters
    ----------
    filepath : str
        path to the file
    size : int
        size to read from the beginning of the file, in bytes. If None is given, read the whole
        file.
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously

    Returns
    -------
    bytes
        the content read from file
    '''

    if asyn:
        async with aiofiles.open(filepath, mode='rb') as f:
            return await f.read(size)
    else:
        with open(filepath, mode='rb') as f:
            return f.read(size)


async def write_binary(filepath, buf: bytes, asyn: bool = True):
    '''An asyn function that creates a binary file and writes the content.

    Parameters
    ----------
    filepath : str
        path to the file
    buf : bytes
        data (in bytes) to be written to the file
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously

    Returns
    -------
    bytes
        the content read from file
    '''

    if asyn:
        async with aiofiles.open(filepath, mode='wb') as f:
            return await f.write(buf)
    else:
        with open(filepath, mode='wb') as f:
            return f.write(buf)


async def read_text(filepath, size: int = None, asyn: bool = True):
    '''An asyn function that opens a text file and reads the content.

    Parameters
    ----------
    filepath : str
        path to the file
    size : int
        size to read from the beginning of the file, in bytes. If None is given, read the whole
        file.
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously

    Returns
    -------
    str
        the content read from file
    '''

    if asyn:
        async with aiofiles.open(filepath, mode='rt') as f:
            return await f.read(size)
    else:
        with open(filepath, mode='rt') as f:
            return f.read(size)


async def write_text(filepath, buf: str, asyn: bool = True):
    '''An asyn function that creates a text file and writes the content.

    Parameters
    ----------
    filepath : str
        path to the file
    buf : str
        data (in bytes) to be written to the file
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously

    Returns
    -------
    bytes
        the content read from file
    '''

    if asyn:
        async with aiofiles.open(filepath, mode='wt') as f:
            return await f.write(buf)
    else:
        with open(filepath, mode='wt') as f:
            return f.write(buf)


async def json_load(filepath, asyn: bool = True, **kwargs):
    '''An asyn function that loads the json-like object of a file.

    Parameters
    ----------
    filepath : str
        path to the file
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously
    kwargs : dict
        keyword arguments passed as-is to :func:`json.loads`

    Returns
    -------
    object
        the loaded json-like object
    '''

    content = await read_text(filepath, asyn=asyn)
    return json.loads(content, **kwargs)


async def json_save(filepath, obj, asyn: bool = True, **kwargs):
    '''An asyn function that saves a json-like object to a file.

    Parameters
    ----------
    filepath : str
        path to the file
    obj : object
        json-like object to be written to the file
    asyn : bool
        whether the function is to be invoked asynchronously or synchronously
    kwargs : dict
        keyword arguments passed as-is to :func:`json.dumps`
    '''

    content = json.dumps(obj, **kwargs)
    await write_text(filepath, content, asyn=asyn)


async def yield_control():
    '''Yields the control back to the current event loop.'''
    fut = asyncio.Future()
    fut.set_result(None)
    await fut


async def qput_aio(q: mq.Queue, obj, block : bool = True, timeout : float = None, aio_interval : float = 0.001):
    '''Puts obj into the queue q.

    If the optional argument `block` is True (the default) and `timeout` is None (the default),
    block asynchronously if necessary until a free slot is available. If timeout is a positive
    number, it blocks asynchronously at most timeout seconds and raises the :class:`queue.Full`
    exception if no free slot was available within that time. Otherwise (`block` is False),
    put an item on the queue if a free slot is immediately available, else raise the
    :class:`queue.Full` exception (timeout is ignored in that case).
    '''

    if not block:
        return q.put(obj, block=False)

    if timeout is None:
        while q.full():
            await asyncio.sleep(aio_interval)
        return q.put(obj, block=True)

    cnt = int(timeout / aio_interval)+1
    while cnt > 0:
        if not q.full():
            return q.put(obj, block=True)
        await asyncio.sleep(aio_interval)
        cnt -= 1
    raise queue.Full()


async def qget_aio(q: mq.Queue, block : bool = True, timeout : float = None, aio_interval : float = 0.001):
    '''Removes and returns an item from the queue q.

    If optional args `block` is True (the default) and `timeout` is None (the default), block
    asynchronously if necessary until an item is available. If `timeout` is a positive number,
    it blocks asynchronously at most timeout seconds and raises the :class:`queue.Empty`
    exception if no item was available within that time. Otherwise (`block` is False), return
    an item if one is immediately available, else raise the :class:`queue.Empty` exception
    (`timeout` is ignored in that case).
    '''

    if not block:
        return q.get(block=False)

    if timeout is None:
        while q.empty():
            await asyncio.sleep(aio_interval)
        return q.get(block=True)

    cnt = int(timeout / aio_interval)+1
    while cnt > 0:
        if not q.empty():
            return q.get(block=True)
        await asyncio.sleep(aio_interval)
        cnt -= 1
    raise queue.Empty()


class BgProcess:
    '''Launches a child process that communicates with the parent process via message passing.

    You should subclass this class and implement :func:`child_handle_message`. See the docstring of the
    function below.

    Notes
    -----
    In interactive mode, remember to delete any instance of the the class when you exit or else it
    will not exit.
    '''

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

    async def send(self, msg, recv_timeout : float = None, recv_aio_interval = 0.001):
        '''Sends a message to the child process and awaits for the returning message.

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
            if the child process is not alive while processing the message
        '''

        while self.sending:
            await yield_control()

        try:
            self.sending = True
            self.msg_p2c.put_nowait(msg)
            while True:
                retval = await qget_aio(self.msg_c2p, timeout=recv_timeout, aio_interval=recv_aio_interval)
                if retval[0] == 'ignored_exception':
                    continue
                if retval[0] == 'write': # child printing something
                    for line in retval[2].splitlines():
                        print("BgProcess ({}): {}".format(retval[1], line))
                    continue
                break
            if retval[0] == 'exit':
                if retval[1] is None:
                    raise RuntimeError("Child process died normally while parent process is expecting some message response.", msg)
                else:
                    raise RuntimeError("Child process died abruptedly with an exception.", retval[1], msg)
            if retval[0] == 'raised_exception':
                raise RuntimeError("Child raised an exception while processing the message.", retval[1], retval[2])
        finally:
            self.sending = False
        return retval[1]

    def child_handle_message(self, msg: object) -> object:
        '''Handles a message obtained from the queue.

        This function should only be called by the child process.

        It takes as input a message from the parent-to-child queue and processes the message.
        Once done, it returns an object which will be wrapped into a message and placed into the
        child-to-parent queue.

        An input message can be anything. Usually it is a tuple with the first component being
        a command string. The output message can also be anything. If the handle succeeds,
        the returning value is then wrapped into a `('returned', retval)` output message. If
        an exception is raised, it is wrapped into a `('raised_exception', exc)` output message.
        Sending a traceback is a pain so you should instead pass a logger to your subclass.

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
        '''
        return msg

    def _worker_process(self):
        import psutil
        import queue
        import sys

        class Writer:

            def __init__(self, msg_c2p, prefix):
                self.msg_c2p = msg_c2p
                self.prefix = prefix

            def write(self, text):
                self.msg_c2p.put_nowait(('write', self.prefix, text))

        sys.stderr = Writer(self.msg_c2p, 'stderr')
        sys.stdout = Writer(self.msg_c2p, 'stdout')

        while True:
          try:
            if self.parent_pid is not None:
                if not psutil.pid_exists(self.parent_pid):
                    self.msg_c2p.put_nowait(('exit', RuntimeError('Parent does not exist.')))
                    return

            try:
                msg = self.msg_p2c.get(block=True, timeout=1)
            except queue.Empty:
                continue

            if msg == 'exit':
                break

            try:
                retval = self.child_handle_message(msg) # handle the message and return
                msg = ('returned', retval)
            except Exception as e:
                msg = ('raised_exception', e, msg)
            self.msg_c2p.put_nowait(msg)
          except KeyboardInterrupt as e:
            self.msg_c2p.put_nowait(('ignored_exception', e))
          except Exception as e:
            self.msg_c2p.put_nowait(('exit', e))
            return

        self.msg_c2p.put_nowait(('exit', None))

    def close(self):
        self.msg_p2c.put_nowait('exit')

