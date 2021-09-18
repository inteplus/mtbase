'''QueenBee concurrency using asyncio and multiprocessing.'''

import asyncio
import multiprocessing as mp
import multiprocessing.connection as mc


from .base import used_cpu_too_much, used_memory_too_much


__all__ = ['QueenBee']


async def wait_for_msg(conn):
    while not conn.poll(0):
        await asyncio.sleep(0.1)
    return conn.recv()


class QueenBee:
    '''The queenbee concurrency model.

    This is a concurrency model made up by Minh-Tri. In this model, there is one queen bee main
    process and multiple worker bee subprocesses. The queen bee is responsible for spawning and
    destroying worker bees herself. She is also responsible for organisation-level tasks. The
    queen bee communicates with every worker bee using a full-duplex pipe, via a message-passing
    mechanism. She does not do individual-level tasks but instead assigns those tasks to worker
    bees.

    The queenbee model can be useful for applications like making ML model predictions from a
    dataframe. In this case, the queen bee owns access to the ML model, delegates the potentially
    IO-related preprocessing and postprocessing tasks of every row to worker bees, and deals with
    making batch predictions from the model.

    The queen bee and her worker bees work in asynchronous mode. Each worker bee operates within a
    context returned from :func:`mt.base.s3.create_context_vars`, with a given profile. It is asked
    to upper-limit number of concurrent asyncio tasks to a given threshold.

    The user should make a subclass from this class and override the 'worker_process' and
    'queen_process' coroutine functions to handle messages.

    Each message is of format `(message_type, ...)` where `message_type` is a string. When a worker
    bee sends a message 'worker_died' to the queen bee, it signals that the worker process has
    terminated.

    There are some more special notes which will be added to here later on.

    Parameters
    ----------
    profile : str, optional
        the S3 profile from which the context vars are created. See :func:`mt.base.s3.create_context_vars`.
    max_concurrency : int
        the maximum number of concurrent tasks at any time for a worker bee, good for managing
        memory allocations. Non-integer values are not accepted.
    '''

    def __init__(self, profile: str = None, max_concurrency: int = 1024):
        self.profile = profile
        if not isinstance(max_concurrency, int):
            raise ValueError("Argument 'max_concurrency' is not an integer: {}.".format(max_concurrency))
        self.max_concurrency = max_concurrency
        self.worker_list = [] # item=(process, q2w_conn), q2w_conn = queen-to-worker connection

    async def worker_process(self, w2q_conn: mc.Connection, max_concurrency: int = 1024, context_vars: dict = {}):
        '''The main process of a worker bee.

        The user should implement this async function.

        The default behaviour is that the worker bee awaits until the queen bee says 'suicide'.

        Parameters
        ----------
        w2q_conn : multiprocessing.connection.Connection
            worker-to-queen connection
        max_concurrency : int
            the maximum number of concurrent tasks at any time for the worker bee
        context_vars : dict
            the asynchronous context vars within which the worker process runs. The dictionary can
            be created by invoking :func:`mt.base.s3.create_context_vars`.

        TBD
        '''

        task = asyncio.ensure_future(wait_for_msg(w2q_conn))
        task_map = {task: 'wait_for_msg'} # task -> task_type

        while True:
            done_tasks, pending_tasks = await asyncio.wait(task_map.keys(), return_when=asyncio.FIRST_COMPLETED)
            for task in done_tasks:
                task_type = task_map[done]
                del task_map[done]

                if task_type == 'wait_for_msg': # MT-TODO: continue from here
                    task.cancelled()
                    task.exception()
                    msg = task.result()

    async def queen_process(self, context_vars: dict = {}):
        '''The main process of the queen bee.

        The user should implement this async function.

        Parameters
        ----------
        context_vars : dict
            the asynchronous context vars within which the queen process runs. The dictionary can
            be created by invoking :func:`mt.base.s3.create_context_vars`.

        TBD
        '''

        pass

    async def run(self, context_vars: dict = {}):
        '''Runs the queenbee concurrency model asynchronously.

        Parameters
        ----------
        context_vars : dict
            the asynchronous context vars within which the queen bee worker process runs. The
            dictionary can be created by invoking :func:`mt.base.s3.create_context_vars`.
        '''
        return await self.queen_process(context_vars=context_vars)
        
