'''BeeHive concurrency using asyncio and multiprocessing.

This is a concurrency model made up by Minh-Tri. In this model, there is main process called the
queen bee and multiple subprocesses called worker bees. The queen bee is responsible for spawning
and destroying worker bees herself. She is also responsible for organisation-level tasks. She does
not do individual-level tasks but instead assigns those tasks to worker bees.

The BeeHive model can be useful for applications like making ML model predictions from a dataframe.
In this case, the queen bee owns access to the ML model and the dataframe, delegates the
potentially IO-related preprocessing and postprocessing tasks of every row of the dataframe to
worker bees, and deals with making batch predictions from the model.
'''

from typing import Optional

import random
import queue
import multiprocessing as mp
import multiprocessing.connection as mc
from copy import copy

from ..contextlib import nullcontext
from ..logging import IndentedLoggerAdapter


__all__ = ['logger_debug_msg', 'Bee', 'WorkerBee', 'subprocess_workerbee', 'QueenBee', 'beehive_run']


def logger_debug_msg(msg, logger=None):
    '''Logs debugging statements extracted from a bee message. For internal use only.'''
    if logger is None:
        return # only works if logger is available

    msg = copy(msg) # because we are going to modify it
    exception = msg.pop('exception', None)
    traceback = msg.pop('traceback', None)
    other_details = msg.pop('other_details', None)

    if traceback is not None:
        with logger.scoped_debug("Traceback", curly=False):
            for x in traceback:
                logger.debug(x)
    if msg:
        with logger.scoped_debug("Message", curly=False):
            logger.debug(msg)
    if exception is not None:
        with logger.scoped_debug("Exception", curly=False):
            logger.debug(repr(exception))
    if isinstance(other_details, dict):
        other_details = copy(other_details) # because we are going to modify it
        msg1 = other_details.pop('msg', None)
        with logger.scoped_debug("Other details", curly=False):
            logger.debug("Data: {}".format(other_details))
            if msg1 is not None:
                logger_debug_msg(msg1, logger=logger)


class Bee:
    '''The base class of a bee.

    A bee is an asynchronous multi-tasking worker that can communicate with its parent and zero or
    more child bees via message passing. Communication means executing a task for the parent and
    delegating child bees to do subtasks. All tasks are asynchronous.

    Each bee maintains a private communication its parent via a full-duplex `(p_p2m, p_m2p)` pair
    of queues. `p2m` and `m2p` stand for parent-to-me and me-to-parent. For every new task to
    be delegated to a child, the bee broadcasts a zero-based task id by sending a message
    `{'msg_type': 'new_task', 'task_id': int}` to every child. If a child wishes to process the
    order, it places a  message `{'msg_type': 'task_accepted', 'task_id': int}` back to the parent
    via its `p_m2p`. If it does not wish to process the order, it should place a different message
    `{'msg_type': 'task_denied', 'task_id': int}`. The parent would choose one of the responders
    for doing the task and would send `{'msg_type': 'task_taken', 'task_id': int}` to all late
    accepters regarding the task. The communication to the chosen child is conducted via 2 further
    messages, each of which is a key-value dictionary.

    The first message is sent from the parent bee describing the task to be delegated to the child
    `{'msg_type': 'task_info', 'task_id': int, 'name': str, 'args': tuple, 'kwargs': dict}`.
    Variables `name` is the task name. It is expected that the child bee class implements an asyn
    member function of the same name as the task name to handle the task. Variables `args` and
    `kwargs` will be passed as positional and keyword arguments respectively to the function. The
    parent assumes that the child will process the task asynchronously at some point in time, and
    awaits for response.

    Upon finishing processing the task, either successfully or not, the child bee must send
    `{'msg_type': 'task_done', 'task_id': int, 'status': str, ...}` back to the parent bee.
    `status` can be one of 3 values: 'cancelled', 'raised' and 'succeeded', depending on whether
    the task was cancelled, raised an exception, or succeeded without any exception. If `status` is
    'cancelled', key `reason` tells the reason if it is not None. If `status` is 'raised', key
    `exception` contains an Exception instance and key `traceback` contains a list of text lines
    describing the call stack, and key `other_details` contains other supporting information.
    Finally, if `status` is 'succeeded', key `returning_value` holds the returning value.

    Child bees are run in daemon subprocesses of the parent bee process. There are some special
    messages apart from messages controlling task delegation above. The first one is
    `{'msg_type': 'dead', 'death_type': str, ...}` sent from a child bee to its parent, where
    `death_type` is either 'normal' or 'killed'. If it is 'normal', key `exit_code` is either None
    or a value representing an exit code or a last word. If it is 'killed', key `exception` holds
    the Exception that caused the bee to be killed, and key `traceback` holds a list of text lines
    describing the call stack. This message is sent only when the bee has finished up tasks and its
    worker process is about to terminate. The second one is `{'msg_type': 'die'}` sent from a
    parent bee to one of its children, telling the child bee to finish its life gracecfully. Bees
    are very sensitive, if the parent says 'die' they become very sad, stop accepting new tasks,
    and die as soon as all existing tasks have been done.

    Each bee comes with its own life cycle, implemented in the async :func:`run`. The user should
    not have to override any private member function. They just have to create new asyn member
    functions representing different tasks.

    Parameters
    ----------
    my_id : int
        the id that the creator has assigned to the bee
    p_m2p : queue.Queue
        the me-to-parent queue for private communication
    p_p2m : queue.Queue
        the parent-to-me queue for private communication
    max_concurrency : int, optional
        maximum number of concurrent tasks that the bee handles at a time. If None is provided,
        there is no constraint in maximum number of concurrent tasks.
    logger : mt.base.logging.IndentedLoggerAdapter, optional
        logger for debugging purposes
    '''


    def __init__(
            self, my_id: int,
            p_m2p: queue.Queue,
            p_p2m: queue.Queue,
            max_concurrency: Optional[int] = 1024,
            logger: Optional[IndentedLoggerAdapter] = None,
    ):
        self.my_id = my_id
        self.p_m2p = p_m2p # me-to-parent, private
        self.p_p2m = p_p2m # parent-to-me, private

        self.child_conn_list = [] # (p_c2m, p_m2c)
        self.child_alive_list = []
        self.num_children = 0
        if not isinstance(max_concurrency, int) and max_concurrency is not None:
            raise ValueError("Argument 'max_concurrency' is not an integer: {}.".format(max_concurrency))
        self.max_concurrency = max_concurrency
        self.logger = logger

        # task scheduling
        self.pending_order_map = {} # task_id -> (timestamp, candidate_children_set), orders that have been placed but not yet accepted
        self.assigned_order_map = {} # task_id -> child_id, orders that have been accepted but not yet awaited by the parent bee
        self.task_id_cnt = 0 # task id counter


    # ----- public -----


    async def run(self):
        '''Implements the life-cycle of the bee. Invoke this function only once.'''
        await self._run_initialise()
        while (not self.to_terminate) or (self.pending_task_cnt > 0) or self.towork_task_list or self.working_task_map:
            await self._run_inloop()

        # ask every worker bee to die gracefully
        for child_id in range(len(self.child_conn_list)):
            if self.child_alive_list[child_id]:
                self._put_msg(child_id, {'msg_type': 'die'})

        while self.num_children > 0:
            await self._run_inloop()

        await self._run_finalise()


    async def delegate(self, name: str, *args, **kwargs):
        '''Delegates a task to one of child bees and awaits the result.

        Parameters
        ----------
        name : str
            the name of the task/asyn member function of the child bee class responsible for
            handling the task
        args : tuple
            positional arguments to be passed as-is to the task/asyn member function
        kwargs : dict
            keyword arguments to be passed as-is to the task/asyn member function

        Returns
        -------
        task_result : dict
            returning message in the form `{'status': str, ...}` where `status` is one of
            'cancelled', 'raised' and 'succeeded'. The remaining keys are described in the class
            docstring.
        child_id : int
            the id of the child that has executed the task
        '''

        import asyncio
        from mt import pd

        # generate new task id
        task_id = self.task_id_cnt
        self.task_id_cnt += 1

        # broadcast an order across all children
        msg = {'msg_type': 'new_task', 'task_id': task_id}
        ts = pd.Timestamp.now()
        candidate_children_set = set()
        self.pending_order_map[task_id] = (ts, candidate_children_set)
        for child_id in range(len(self.child_conn_list)):
            if self.child_alive_list[child_id]:
                self._put_msg(child_id, msg)
                candidate_children_set.add(child_id)

        # wait for some child to respond
        while task_id in self.pending_order_map:
            if not candidate_children_set: # no more candidate
                task_result = {
                    'status': 'cancelled',
                    'reason': 'Every child bee has denied the task.',
                }
                self.pending_order_map.pop(task_id)
                return task_result, -1
            delay = pd.Timestamp.now() - ts
            if delay.total_seconds() >= 600: # 10 minutes
                task_result = {
                    'status': 'cancelled',
                    'reason': 'No child bee has picked up the task for 10 minutes.',
                }
                self.pending_order_map.pop(task_id)
                return task_result, -1
            await asyncio.sleep(0) # await changes elsewhere

        child_id = self.assigned_order_map.pop(task_id)

        # send the task info to the child
        self._put_msg(child_id, {
            'msg_type': 'task_info',
            'task_id': task_id,
            'name': name,
            'args': args,
            'kwargs': kwargs,
        })
        self.started_dtask_map[task_id] = child_id

        # wait for the child bee to finish the specified task (it can concurrently do other tasks)
        while task_id not in self.done_dtask_map:
            await asyncio.sleep(0) # await changes elsewhere

        msg = self.done_dtask_map.pop(task_id)

        return msg, child_id


    # ----- private -----


    def _put_msg(self, child_id, msg):
        '''Puts a message to another bee.'''
        conn = self.p_m2p if child_id < 0 else self.child_conn_list[child_id][1] # child case: m2c
        try:
            conn.put_nowait(msg)
        except:
            if self.logger:
                self.logger.warn_last_exception()
                self.logger.warn("Unexpected exception above while sending a message.")
            raise


    async def _run_initialise(self):
        '''Initialises the life cycle of the bee.'''

        import asyncio

        self.to_terminate = False # let other futures decide when to terminate

        # tasks delegated to child bees
        self.started_dtask_map = {} # task_id -> child_id
        self.done_dtask_map = {} # task_id -> {'status': str, ...}

        # tasks handled by the bee itself
        self.pending_task_cnt = 0 # number of tasks awaiting further info
        self.towork_task_list = [] # list of (task, task_id)'s, tasks that have got full info but awaiting to be worked on
        self.working_task_map = {} # task -> task_id, tasks that are being worked on


    async def _run_inloop(self):
        '''The thing that the bee does every day until it dies, operating at 1kHz.'''

        import asyncio
        import queue

        # dispatch all messages from the parent
        while not self.p_p2m.empty():
            try:
                msg = self.p_p2m.get_nowait()
                self._dispatch_new_parent_msg(msg)
            except queue.Empty:
                break

        # dispatch all messages from all the children
        child_id_list = list(range(len(self.child_conn_list)))
        random.shuffle(child_id_list)
        for child_id in child_id_list:
            if not self.child_alive_list[child_id]:
                continue
            conn = self.child_conn_list[child_id][0] # c2m
            while not conn.empty():
              try:
                msg = conn.get_nowait()
                self._dispatch_new_child_msg(child_id, msg)
              except queue.Empty:
                break

        # grab some tasks for work
        while self.towork_task_list and \
              ((self.max_concurrency is None) or (len(self.working_task_map) < self.max_concurrency)):
            task, task_id = self.towork_task_list.pop(0)
            self.working_task_map[task] = task_id

        # scout for tasks done by the bee
        if self.working_task_map:
            done_task_set, _ = await asyncio.wait(self.working_task_map.keys(), timeout=0, return_when=asyncio.FIRST_COMPLETED)

            # process each of the tasks done by the bee
            for task in done_task_set:
                self._deliver_done_task(task)

        # yield control a bit
        await asyncio.sleep(0.001)


    async def _run_finalise(self):
        '''Finalises the life cycle of the bee.'''
        msg = {'msg_type': 'dead', 'death_type': 'normal', 'exit_code': self.exit_code}
        self._put_msg(-1, msg) # tell the parent it has died normally


    async def _execute_task(self, name: str, args: tuple = (), kwargs: dict = {}):
        '''Processes a request message coming from one of the parent bee.

        Parameters
        ----------
        name : str
            name of the task/asyn member function to be invoked
        args : tuple
            positional arguments to be passed to the task as-is
        kwargs : dict
            keyword arguments to be passed to the task as-is

        Returns
        -------
        object
            user-defined

        Raises
        ------
        Exception
            user-defined
        '''
        try:
            func = getattr(self, name)
            return await func(*args, **kwargs)
        except:
            if self.logger:
                self.logger.warn_last_exception()
                self.logger.warn("Caught the exception above while executing a task.")
            raise


    def _mourn_death(self, child_id, msg): # msg = {'death_type': str, ...}
        '''Processes the death wish of a child bee.'''

        import asyncio
        from ..traceback import extract_stack_compact

        self.child_alive_list[child_id] = False # mark the child as dead

        if msg['death_type'] == 'killed':
            with self.logger.scoped_debug("Child bee {} was killed".format(child_id), curly=False) if self.logger else nullcontext():
                logger_debug_msg(msg, logger=self.logger)

        # process all pending tasks held up by the dead bee
        for task_id in self.started_dtask_map:
            if child_id != self.started_dtask_map[task_id]:
                continue
            del self.started_dtask_map[task_id] # pop it
            self.done_dtask_map[task_id] = {
                'status': 'raised',
                'exception': asyncio.CancelledError('Delegated task cancelled as the delegated bee has been killed.'),
                'traceback': extract_stack_compact(),
                'other_details': {
                    'child_id': child_id,
                    'msg': msg,
                },
            }
        self.num_children -= 1


    def _deliver_done_task(self, task):
        '''Sends the result of a task that has been done by the bee back to its parent.'''

        task_id = self.working_task_map.pop(task)

        if task.cancelled():
            self._put_msg(-1, {'msg_type': 'task_done', 'task_id': task_id, 'status': 'cancelled', 'reason': None})
        elif task.exception() is not None:
            import io
            traceback = io.StringIO()
            task.print_stack(file=traceback)
            traceback = traceback.getvalue().split('\n')
            msg = {
                'msg_type': 'task_done',
                'task_id': task_id,
                'status': 'raised',
                'exception': task.exception(),
                'traceback': traceback,
                'other_details': None,
            }
            self._put_msg(-1, msg)
        else:
            self._put_msg(-1, {
                'msg_type': 'task_done',
                'task_id': task_id,
                'status': 'succeeded',
                'returning_value': task.result(),
            })


    def _dispatch_new_child_msg(self, child_id, msg):
        '''Dispatches a new message from a child.'''

        import asyncio
        from ..traceback import extract_stack_compact

        msg_type = msg['msg_type']
        if msg_type == 'dead':
            self._mourn_death(child_id, msg)
        elif msg_type == 'task_accepted':
            task_id = msg['task_id']
            if task_id in self.pending_order_map:
                self.assigned_order_map[task_id] = child_id
                self.pending_order_map.pop(task_id)
            else:
                self._put_msg(child_id, {'msg_type': 'task_taken', 'task_id': task_id}) # tell the child the task has been taken
        elif msg_type == 'task_denied':
            task_id = msg['task_id']
            if task_id in self.pending_order_map:
                candidate_children_set = self.pending_order_map[task_id][1]
                if child_id in candidate_children_set:
                    candidate_children_set.remove(child_id)
                elif self.logger: # log a warning
                    self.logger.warn("Bee {}: Child bee {} has denied task {}.".format(
                        self.my_id, child_id, task_id))
                    self.logger.warn("But the child is no longer a candidate for the task.")
        elif msg_type == 'task_done': # delegated task done
            task_id = msg['task_id']
            status = msg['status']
            child_id = self.started_dtask_map.pop(task_id)
            if status == 'cancelled':
                self.done_dtask_map[task_id] = {
                    'status': 'raised',
                    'exception': asyncio.CancelledError('Delegated task cancelled.'),
                    'traceback': extract_stack_compact(),
                    'other_details': {'child_id': child_id, 'reason': msg['reason']},
                }
            elif status == 'raised':
                self.done_dtask_map[task_id] = {
                    'status': 'raised',
                    'exception': RuntimeError('Delegated task raised an exception.'),
                    'traceback': extract_stack_compact(),
                    'other_details': {'child_id': child_id, 'msg': msg},
                }
            else:
                self.done_dtask_map[task_id] = {
                    'status': 'succeeded',
                    'returning_value': msg['returning_value'],
                }
        else:
            self.to_terminate = True
            self.exit_code = "Unknown message with type '{}'.".format(msg_type)


    def _dispatch_new_parent_msg(self, msg):
        '''Dispatches a new message from the parent.'''

        import asyncio

        from ..traceback import extract_stack_compact

        msg_type = msg['msg_type']
        if msg_type == 'die': # request to die?
            self.to_terminate = True
            self.exit_code = "The parent bee ordered to die gracefully."
        elif msg_type == 'new_task':
            # volunteer only if we can
            task_id = msg['task_id']
            if (not self.to_terminate) and \
                ((self.max_concurrency is None) or (len(self.working_task_map) < self.max_concurrency)):
                self._put_msg(-1, {'msg_type': 'task_accepted', 'task_id': task_id}) # notify the parent
                self.pending_task_cnt += 1 # awaiting further info
            else:
                self._put_msg(-1, {'msg_type': 'task_denied', 'task_id': task_id}) # notify the parent
        elif msg_type == 'task_taken':
            self.pending_task_cnt -= 1
        elif msg_type == 'task_info': # task info -> new task
            self.pending_task_cnt -= 1
            task_id = msg['task_id']
            task = asyncio.ensure_future(self._execute_task(name=msg['name'], args=msg['args'], kwargs=msg['kwargs']))
            self.towork_task_list.append((task,task_id))
        else:
            self.to_terminate = True
            self.exit_code = "Unknown message with type '{}'.".format(msg_type)


class WorkerBee(Bee):
    '''A worker bee in the BeeHive concurrency model.

    See parent :class:`Bee` for more details.

    Parameters
    ----------
    my_id : int
        the id that the queen bee has assigned to the worker bee
    p_m2p : multiprocessing.Queue
        the me-to-parent connection for private communication
    p_p2m : multiprocessing.Queue
        the parent-to-me connection for private communication
    max_concurrency : int
        maximum number of concurrent tasks that the bee handles at a time
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`.
    logger : mt.base.logging.IndentedLoggerAdapter, optional
        logger for debugging purposes
    '''

    def __init__(
            self, my_id: int,
            p_m2p: mp.Queue,
            p_p2m: mp.Queue,
            max_concurrency: int = 1024,
            context_vars: dict = {},
            logger: Optional[IndentedLoggerAdapter] = None,
    ):
        super().__init__(my_id, p_m2p, p_p2m, max_concurrency=max_concurrency, logger=logger)
        self.context_vars = context_vars


    # ----- public -----


    async def busy_status(self, context_vars: dict = {}):
        '''A task to check how busy the worker bee is.

        Parameters
        ----------
        context_vars : dict
            a dictionary of context variables within which the function runs. It must include
            `context_vars['async']` to tell whether to invoke the function asynchronously or not.
            In addition, variable 's3_client' must exist and hold an enter-result of an async with
            statement invoking :func:`mt.base.s3.create_s3_client`.

        Returns
        -------
        float
            a scalar between 0 and 1 where 0 means completely free and 1 means completely busy
        '''
        return 0.0 if self.max_concurrency is None else len(self.working_task_map)/self.max_concurrency


    # ----- private -----


    async def _execute_task(self, name: str, args: tuple = (), kwargs: dict = {}):
        new_kwargs = {'context_vars': self.context_vars}
        new_kwargs.update(kwargs)
        return await super()._execute_task(name, args, new_kwargs)
    _execute_task.__doc__ = Bee._execute_task.__doc__


def subprocess_workerbee(
        workerbee_class, workerbee_id: int,
        init_args: tuple = (),
        init_kwargs: dict = {},
        s3_profile: Optional[str] = None,
        max_concurrency: int = 1024,
        logger: Optional[IndentedLoggerAdapter] = None,
):
    '''Creates a daemon subprocess that holds a worker bee and runs the bee in the subprocess.

    Parameters
    ----------
    workerbee_class : class
        subclass of :class:`WorkerBee` whose constructor accepts all positional and keyword
        arguments of the constructor of the super class
    workerbee_id : int
        the id that the queen bee has assigned to the worker bee
    init_args : tuple
        additional positional arguments to be passed as-is to the new bee's constructor
    init_kwargs : dict
        additional keyword arguments to be passed as-is to the new bee's constructor
    s3_profile : str, optional
        the S3 profile from which the context vars are created.
        See :func:`mt.base.s3.create_context_vars`.
    max_concurrency : int
        maximum number of concurrent tasks that the bee handles at a time
    logger : mt.base.logging.IndentedLoggerAdapter, optional
        logger for debugging purposes

    Returns
    -------
    process : multiprocessing.Process
        the created subprocess
    p_m2p : multiprocessing.Queue
        the me-to-parent connection for private communication with the worker bee
    p_p2m : multiprocessing.Queue
        the parent-to-me connection for private communication with the worker bee
    '''

    import multiprocessing as mp

    async def subprocess_asyn(
            workerbee_class,
            workerbee_id,
            p_m2p: mp.Queue,
            p_p2m: mp.Queue,
            init_args: tuple = (),
            init_kwargs: dict = {},
            s3_profile: Optional[str] = None,
            max_concurrency: int = 1024,
            logger: Optional[IndentedLoggerAdapter] = None,
    ):
        from ..s3 import create_context_vars

        async with create_context_vars(profile=s3_profile, asyn=True) as context_vars:
            bee = workerbee_class(
                workerbee_id,
                p_m2p,
                p_p2m,
                *init_args,
                max_concurrency=max_concurrency,
                context_vars=context_vars,
                logger=logger,
                **init_kwargs)
            await bee.run()

    def subprocess(
            workerbee_class,
            workerbee_id,
            p_m2p: mp.Queue,
            p_p2m: mp.Queue,
            init_args: tuple = (),
            init_kwargs: dict = {},
            s3_profile: Optional[str] = None,
            max_concurrency: int = 1024,
            logger: Optional[IndentedLoggerAdapter] = None,
    ):
        import asyncio
        from ..traceback import extract_stack_compact

        try:
            asyncio.run(subprocess_asyn(
                workerbee_class,
                workerbee_id,
                p_m2p,
                p_p2m,
                init_args=init_args,
                init_kwargs=init_kwargs,
                s3_profile=s3_profile,
                max_concurrency=max_concurrency,
                logger=logger,
            ))
        except Exception as e: # tell the queen bee that the worker bee has been killed by an unexpected exception
            msg = {
                'msg_type': 'dead',
                'death_type': 'killed',
                'exception': e,
                'traceback': extract_stack_compact(),
            }
            p_m2p.put_nowait(msg)

    p_p2m = mp.Queue()
    p_m2p = mp.Queue()
    process = mp.Process(
        target=subprocess,
        args=(workerbee_class, workerbee_id, p_m2p, p_p2m),
        kwargs={'init_args': init_args, 'init_kwargs': init_kwargs, 's3_profile': s3_profile, 'max_concurrency': max_concurrency},
        daemon=True)
    process.start()

    return process, p_m2p, p_p2m


class QueenBee(WorkerBee):
    '''The queen bee in the BeeHive concurrency model.

    The queen bee and her worker bees work in asynchronous mode. Each worker bee operates within a
    context returned from :func:`mt.base.s3.create_context_vars`, with a given profile. It is asked
    to upper-limit number of concurrent asyncio tasks to a given threshold.

    The queen bee herself is a worker bee. However, initialising a queen bee, the user must provide
    a subclass of :class:`WorkerBee` to represent the worker bee class, from which the queen can
    spawn worker bees. Like other bees, it is expected that the user write member asynchornous
    functions to handle tasks.

    Parameters
    ----------
    my_id : int
        the id that the creator has assigned to the queen bee
    p_m2p : queue.Queue
        the me-to-parent connection for private communication between the user (parent) and the queen
    p_p2m : queue.Queue
        the parent-to-me connection for private communication with the worker bee
    workerbee_class : class
        subclass of :class:`WorkerBee` whose constructor accepts all positional and keyword
        arguments of the constructor of the super class
    worker_init_args : tuple
        additional positional arguments to be passed as-is to each new worker bee's constructor
    worker_init_kwargs : dict
        additional keyword arguments to be passed as-is to each new worker bee's constructor
    s3_profile : str, optional
        the S3 profile from which the context vars are created. See :func:`mt.base.s3.create_context_vars`.
    max_concurrency : int, optional
        the maximum number of concurrent tasks at any time for the bee, good for managing
        memory allocations. Should be default to None to allow the queen bee to deal with all
        requests.
    workerbee_max_concurrency : int
        the maximum number of concurrent tasks at any time for a worker bee, good for managing
        memory allocations. Non-integer values are not accepted.
    context_vars : dict
        a dictionary of context variables within which the queen be functions runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`.
    logger : mt.base.logging.IndentedLoggerAdapter, optional
        logger for debugging purposes
    '''

    def __init__(
            self, my_id: int,
            p_m2p: queue.Queue,
            p_p2m: queue.Queue,
            workerbee_class,
            worker_init_args: tuple = (),
            worker_init_kwargs: dict = {},
            s3_profile: Optional[str] = None,
            max_concurrency: Optional[int] = None,
            workerbee_max_concurrency: Optional[int] = 1024,
            context_vars: dict = {},
            logger: Optional[IndentedLoggerAdapter] = None,
    ):
        super().__init__(
            my_id, p_m2p, p_p2m, max_concurrency=max_concurrency, context_vars=context_vars,
            logger=logger)

        self.workerbee_class = workerbee_class
        self.worker_init_args = worker_init_args
        self.worker_init_kwargs = worker_init_kwargs
        self.workerbee_max_concurrency = workerbee_max_concurrency
        self.s3_profile = s3_profile

        # the processes of existing workers, excluding those that are dead
        self.process_map = {} # worker_id/child_id -> process


    def __del__(self):
        for process in self.process_map:
            if not process.is_alive():
                continue
            process.join(10) # give maximum 10 seconds for each process to terminate
            if process.exitcode is not None:
                if self.logger:
                    self.logger.warn("Subprocess {} has terminated with exit code {}.".format(process.pid, process.exitcode))


    # ----- private -----


    async def _manage_population(self):
        '''Manages the population of worker bees regularly.'''

        import asyncio
        import multiprocessing as mp

        from .base import used_cpu_too_much, used_memory_too_much
        from ..traceback import extract_stack_compact

        min_num_workers = 1
        max_num_workers = max(mp.cpu_count()-1, min_num_workers)

        self.stop_managing = False # let other futures decide when to stop managing
        inited = False
        while not self.stop_managing and not self.to_terminate:
            num_workers = len(self.process_map)

            if num_workers < min_num_workers:
                if not inited:
                    cnt = min(min_num_workers, max_num_workers//2)
                    for i in range(cnt):
                        self._spawn_new_workerbee()
                    inited = True
                else:
                    raise RuntimeError("All children bee have been killed.")
            elif (num_workers > min_num_workers) and ((num_workers > max_num_workers) or used_cpu_too_much() or used_memory_too_much()):
                # kill the worker bee that responds to 'busy_status' task
                task_result, worker_id = await self.delegate('busy_status')
                self._put_msg(worker_id, {'msg_type': 'die'})
            elif num_workers < max_num_workers:
                self._spawn_new_workerbee()

            await asyncio.sleep(10) # sleep for 10 seconds

            # check the children that have been killed without sending the last wish
            for child_id in range(len(self.child_conn_list)):
                if not self.child_alive_list[child_id]:
                    continue
                if not self.process_map[child_id].is_alive(): # killed without sending the last wish?
                    msg = {
                        'msg_type': 'dead',
                        'death_type': 'killed',
                        'exception': RuntimeError("Child bee id {} has died without sending a last wish.".format(child_id)),
                        'traceback': extract_stack_compact(),
                    }
                    self._mourn_death(child_id, msg)


    def _mourn_death(self, child_id, msg): # msg = {'death_type': str, ...}
        super()._mourn_death(child_id, msg)
        self.process_map.pop(child_id)
    _mourn_death.__doc__ = WorkerBee._mourn_death.__doc__


    def _spawn_new_workerbee(self):
        worker_id = len(self.child_conn_list) # get new worker id
        child_id = len(self.child_conn_list) # get new child id
        process, p_c2m, p_m2c = subprocess_workerbee(
            self.workerbee_class,
            child_id,
            init_args=self.worker_init_args,
            init_kwargs=self.worker_init_kwargs,
            s3_profile=self.s3_profile,
            max_concurrency=self.workerbee_max_concurrency)
        self.child_conn_list.append((p_c2m, p_m2c))
        self.child_alive_list.append(True) # the new child is assumed alive
        self.num_children += 1
        self.process_map[worker_id] = process
        return worker_id


    async def _run_initialise(self):
        await super()._run_initialise()
        import asyncio
        self.managing_task = asyncio.ensure_future(self._manage_population())


    async def _run_finalise(self):
        import asyncio

        self.stop_managing = True
        await asyncio.wait([self.managing_task])

        await super()._run_finalise()


async def beehive_run(
        queenbee_class,
        workerbee_class,
        task_name: str,
        task_args: tuple = (),
        task_kwargs: dict = {},
        queenbee_init_args: tuple = (),
        queenbee_init_kwargs: dict = {},
        workerbee_init_args: tuple = (),
        workerbee_init_kwargs: dict = {},
        s3_profile: Optional[str] = None,
        max_concurrency: int = 1024,
        queenbee_max_concurrency: Optional[int] = None,
        context_vars: dict = {},
        logger: Optional[IndentedLoggerAdapter] = None,
):
    '''An asyn function that runs a task in a BeeHive concurrency model.

    Parameters
    ----------
    queenbee_class : class
        a subclass of :class:`QueenBee`
    workerbee_class : class
        a subclass of :class:WorkerBee`
    task_name : str
        name of a task assigned to the queen bee, and equivalently the queen bee's member asyn
        function handling the task
    task_args : tuple
        positional arguments to be passed to the member function as-is
    task_kwargs : dict
        keyword arguments to be passed to the member function as-is
    queenbee_init_args : tuple
        additional positional arguments to be passed as-is to the new queen bee's constructor
    queenbee_init_kwargs : dict
        additional keyword arguments to be passed as-is to the new queen bee's constructor
    workerbee_init_args : tuple
        additional positional arguments to be passed as-is to each new worker bee's constructor
    workerbee_init_kwargs : dict
        additional keyword arguments to be passed as-is to each new worker bee's constructor
    s3_profile : str, optional
        the S3 profile from which the context vars are created. See
        :func:`mt.base.s3.create_context_vars`.
    max_concurrency : int
        maximum number of concurrent tasks that each worker bee handles at a time
    queenbee_max_concurrency : int, optional
        maximum number of concurrent tasks that the queen bee handles at a time. Default is no
        limit.
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`.
    logger : mt.base.logging.IndentedLoggerAdapter, optional
        logger for debugging purposes

    Returns
    -------
    object
        anything returned by the task assigned to the queen bee

    Raises
    ------
    RuntimeError
        when the queen bee has cancelled her task
    Exception
        anything raised by the task assigned to the queen bee
    '''

    if not context_vars['async']:
        raise ValueError("The function can only be run in asynchronous mode.")

    import multiprocessing as mp
    import asyncio

    # create a private communication channel with the queen bee
    p_u2q = queue.Queue()
    p_q2u = queue.Queue()

    # create a queen bee and start her life
    queen = queenbee_class(
        10000, # queen bee has id 10000
        p_q2u,
        p_u2q,
        workerbee_class,
        *queenbee_init_args,
        worker_init_args=workerbee_init_args,
        worker_init_kwargs=workerbee_init_kwargs,
        s3_profile=s3_profile,
        max_concurrency=queenbee_max_concurrency,
        workerbee_max_concurrency=max_concurrency,
        context_vars=context_vars,
        logger=logger,
        **queenbee_init_kwargs,
    )
    queen_life = asyncio.ensure_future(queen.run())

    # advertise a task to the queen bee and waits for her response
    p_u2q.put_nowait({'msg_type': 'new_task', 'task_id': 0})
    while p_q2u.empty():
        await asyncio.sleep(0.001)

    # get her confirmation
    msg = p_q2u.get_nowait()

    # describe the task to her
    p_u2q.put_nowait({
        'msg_type': 'task_info',
        'task_id': 0,
        'name': task_name,
        'args': task_args,
        'kwargs': task_kwargs,
    })

    # wait for the task to be done
    while p_q2u.empty():
        await asyncio.sleep(0.1)

    # get the result
    msg = p_q2u.get_nowait()

    # tell the queen to die
    p_u2q.put_nowait({'msg_type': 'die'})

    # wait for her to die, hopefully gracefully
    await asyncio.wait([queen_life])

    # intepret the result
    if msg['status'] == 'cancelled':
        raise asyncio.CancelledError("The queen bee cancelled the task. Reason: {}".format(msg['reason']))

    if msg['status'] == 'raised':
        with logger.scoped_debug("Exception raised by the queen bee", curly=False) if logger else nullcontext():
            logger_debug_msg(msg, logger=logger)
            raise msg['exception']

    else: # msg['status'] == done'
        return msg['returning_value']
