'''BeeHive concurrency using asyncio and multiprocessing.

This is a concurrency model made up by Minh-Tri. In this model, there is main process called the
queen bee and multiple subprocesses called worker bees. The queen bee is responsible for spawning
and destroying worker bees herself. She is also responsible for organisation-level tasks. The queen
bee communicates with every worker bee using a full-duplex pipe, in a message-passing fasion. She
does not do individual-level tasks but instead assigns those tasks to worker bees.

The BeeHive model can be useful for applications like making ML model predictions from a dataframe.
In this case, the queen bee owns access to the ML model and the dataframe, delegates the
potentially IO-related preprocessing and postprocessing tasks of every row of the dataframe to
worker bees, and deals with making batch predictions from the model.
'''

import multiprocessing.connection as mc

from .base import used_cpu_too_much, used_memory_too_much


__all__ = ['Bee', 'WorkerBee', 'QueenBee']


class Bee:
    '''The base class of a bee.

    A bee is an asynchronous multi-tasking worker that can communicate with one or more bees via
    message passing. Communication means requesting another bee do to a task and waiting for some
    result, or doing a task for another bee. While doing a task, the bee can request yet another
    bee to do yet another task for it. And so on, as long as there is no task cycle. All tasks are
    asynchronous.

    Each message is a key-value dictionary. A task that is delegated from one bee to another bee is
    communicated via 2 or 3 messages. The first message
    `{'msg_type': 'request', 'msg_id': int, 'task_info': dict}` represents a request to do a task
    from a sending bee to a receiving bee. `msg_id` is a 0-based integer generated by the sender.
    `task_info = {'name': str, 'args': list, kwargs: dict}` contains the name of the task, the
    positional arguments of the task, and the keyword arguments of the task. The default behaviour
    is to invoke the asynchronous member function of the class with name `task_info['name']`,
    passing the positional arguments and the keyword arguments to the function.

    Upon receiving the request, the receiver must send an acknowledgment
    `{'msg_type': 'acknowledged', 'msg_id': int, 'accepted': bool}` with the same `msg_id` and
    `accepted` is a boolean telling if the receiver will proceed with doing the task (True) or if
    it rejects the request (False). If it proceeds with doing the task, upon completing the task,
    it must send `{'msg_type': 'result', 'msg_id': int, 'status': str, ...}` back to the sender.
    `status` can be one of 3 values: 'cancelled', 'raised' and 'done', depending on whether the
    task was cancelled, raised an exception, or completed without any exception. If `status` is
    'cancelled', key `reason` tells the reason if it is not None. If `status` is 'raised', key
    `exception` contains an Exception instance and key `callstack` contains a list of text lines
    describing the call stack, and key `other_details` contains other supporting information.
    Finally, if `status` is 'done', key `returning_value` holds the returning value.

    The bee posesses a number of connections to communicate with other bees. There are some special
    messages apart from messages controlling task delegation above. The first one is
    `{'msg_type': 'dead', 'death_type': str, ...}` where `death_type` is either 'normal' or
    'killed'. If it is normal, key `exit_code` is either None or a string representing an exit code
    or a last word. If it is 'killed', key `exception` holds the Exception that caused the bee to
    be killed, and key `callstack` holds a list of text lines describing the call stack. This
    message is sent to all bees that the dying bee is connected to. It is sent only when the bee
    has finished up tasks and its worker process is about to terminate. The second one is
    `{'msg_type': 'die'}` which represents the sender's wish for the receiver to finish its life
    gracecfully. Bees are very sensitive, if someone says 'die' they will be very sad, stop
    accepting new tasks, and die as soon as all existing tasks have been done.

    Each bee comes with its own life cycle, implemented in the async :func:`run`. The user should
    not have to override any private member function. They just have to create new asyn member
    functions representing different tasks.

    Parameters
    ----------
    max_concurrency : int
        maximum number of concurrent tasks that the bee handles at a time
    '''


    def __init__(self, max_concurrency: int = 1024):
        self.conn_list = []
        self.conn_alive_list = []
        self.max_concurrency = max_concurrency


    # ----- public -----


    def add_new_connection(self, conn: mc.Connection):
        '''Adds a new connection to the bee's list of connections.'''

        self.conn_list.append(conn)
        self.conn_alive_list.append(True) # the new connection is assumed alive


    async def delegate_task(self, conn_id: int, task_info: dict):
        '''Delegates a task to another bee.

        Parameters
        ----------
        conn_id : int
            the index of the intended connection to which the task is delegated
        task_info : dict
            a dictionary `{'name': str, 'args': list, 'kwargs': dict}` containing information about
            the task.

        Returns
        -------
        task_result : dict
            returning message in the form `{'status': str, ...}` where `status` is one of
            'cancelled', 'raised' and 'done'. The remaining keys are described in the class
            docstring.
        '''

        # generate new msg id
        msg_id = self.msg_id
        self.msg_id += 1

        # send the request
        conn = self.conn_list[conn_id]
        conn.send({'msg_type': 'request', 'msg_id': msg_id, 'task_info': task_info})
        self.requesting_delegated_task_map[msg_id] = conn_id

        # yield control until the task is done
        from ..aio import yield_control
        while msg_id not in self.done_delegated_task_map:
            await yield_control()

        # get the result
        task_result = self.done_delegated_task_map[msg_id]
        del self.done_delegated_task_map[msg_id] # pop it

        return task_result


    # ----- private -----


    async def _run(self):
        '''Implements the life-cycle of a bee. Please do not invoke this function directly.'''

        import asyncio

        self.msg_id = 0 # internal message counter
        self.to_terminate = False # let other futures decide when to terminate

        # tasks delegated to other bees
        self.requesting_delegated_task_map = {} # msg_id -> conn_id
        self.started_delegated_task_map = {} # msg_id -> conn_id
        self.done_delegated_task_map = {} # msg_id -> {'status': str, ...}

        # tasks handled by the bee itself
        self.pending_request_list = [] # (conn_id, msg_id, {'task': str, ...})
        self.working_task_map = {} # task -> (conn_id, msg_id)

        listening_task = asyncio.ensure_future(self._listen_and_dispatch_new_msgs())

        while (not self.to_terminate) or self.pending_request_list or self.working_task_map:
            # form the list of current tasks performed by the bee
            done_task_set, _ = await asyncio.wait(self.working_task_map.keys(), return_when=asyncio.FIRST_COMPLETED)

            # process each of the tasks done by the bee
            for task in done_task_set:
                self._deliver_done_task(task)

            # transform some pending requests into working tasks
            num_requests = min(self.max_concurrency - len(self.working_task_map), len(self.pending_request_list))
            for i in range(num_requests):
                conn_id, msg_id, task_info = self.pending_request_list.pop(0) # pop it
                task = asyncio.ensure_future(self._execute_task(**task_info))
                self.working_task_map[task] = (conn_id, msg_id)

        self.stop_listening = True
        await asyncio.wait([listening_task])
        self._inform_death({'death_type': 'normal', 'exit_code': self.exit_code})


    async def _execute_task(self, name: str, args: list = [], kwargs: dict = {}):
        '''Processes a request message coming from one of the connecting bees.

        The default behaviour implemented here is to invoke the member asyn function with name
        `task_info['name']`.

        Parameters
        ----------
        name : str
            name of the task, or in the default behaviour, name of the member function to be
            invoked
        args : list
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
        func = getattr(self, name)
        return await func(*args, **kwargs)


    def _mourn_death(self, conn_id, msg): # msg = {'death_type': str, ...}
        '''Processes the death wish of another bee.'''

        from ..traceback import extract_stack_compact

        self.conn_alive_list[conn_id] = False # mark the connection as dead

        # process all pending tasks held up by the dead bee
        for msg_id, other_conn_id in self.requesting_delegated_task_map.items():
            if other_conn_id != conn_id:
                continue
            del self.requesting_delegated_task_map[msg_id] # pop it
            self.done_delegated_task_map[msg_id] = {
                'status': 'raised',
                'exception': RuntimeError('Delegated task rejected as the delegated bee had been killed.'),
                'callstack': extract_stack_compact(),
                'other_details': {
                    'conn_id': conn_id,
                    'last_word_msg': msg,
                },
            }
        for msg_id, other_conn_id in self.started_delegated_task_map.items():
            if other_conn_id != conn_id:
                continue
            del self.started_delegated_task_map[msg_id] # pop it
            self.done_delegated_task_map[msg_id] = {
                'status': 'raised',
                'exception': RuntimeError('Delegated task cancelled as the delegated bee had been killed.'),
                'callstack': extract_stack_compact(),
                'other_details': {
                    'conn_id': conn_id,
                    'last_word_msg': msg,
                },
            }


    def _deliver_done_task(self, task):
        '''Sends the result of a task that has been done by the bee back to its sender.'''

        import io

        conn_id, msg_id = self.working_task_map[task]
        del self.working_task_map[task] # pop it
        conn = self.conn_list[conn_id]

        if task.cancelled():
            conn.send({'msg_type': 'result', 'msg_id': msg_id, 'status': 'cancelled', 'reason': None})
        elif task.exception() is not None:
            tracestack = io.StringIO()
            task.print_stack(file=tracestack)

            conn.send({
                'msg_type': 'result',
                'msg_id': msg_id,
                'status': 'raised',
                'exception': task.exception(),
                'callstack': tracestack.getvalue(),
                'other_details': None,
            })
        else:
            conn.send({
                'msg_type': 'result',
                'msg_id': msg_id,
                'status': 'done',
                'returning_value': task.result(),
            })


    def _dispatch_new_msg(self, conn_id, msg):
        '''Dispatches a new message.'''

        from ..traceback import extract_stack_compact

        msg_type = msg['msg_type']
        if msg_type == 'die': # request to die?
            self.to_terminate = True
            self.exit_code = "A bee with conn_id={} said 'die'.".format(conn_id)
        elif msg_type == 'dead':
            self._mourn_death(conn_id, msg)
        elif msg_type == 'request': # new task
            msg_id = msg['msg_id']
            conn = self.conn_list[conn_id]
            if self.to_terminate:
                conn.send({'msg_type': 'acknowledged', 'msg_id': msg_id, 'accepted': False}) # reject the request
            else:
                self.pending_request_list.append((conn_id, msg_id, msg['task_info']))
                conn.send({'msg_type': 'acknowledged', 'msg_id': msg_id, 'accepted': True})
        elif msg_type == 'acknowledged': # delegated task accepted or not
            msg_id = msg['msg_id']
            conn_id = self.requesting_delegated_task_map[msg_id]
            del self.requesting_delegated_task_map[msg_id] # pop it
            if msg['accepted']:
                self.started_delegated_task_map[msg_id] = conn_id
            else:
                self.done_delegated_task_map[msg_id] = {
                    'status': 'raised',
                    'exception': RuntimeError('Delegated task rejected.'),
                    'callstack': extract_stack_compact(),
                    'other_details': {'conn_id': conn_id},
                }
        elif msg_type == 'result': # delegated task done
            msg_id = msg['msg_id']
            status = msg['status']
            conn_id = self.started_delegated_task_map[msg_id]
            del self.started_delegated_task_map[msg_id] # pop it
            if status == 'cancelled':
                self.done_delegated_task_map[msg_id] = {
                    'status': 'raised',
                    'exception': RuntimeError('Delegated task cancelled.'),
                    'callstack': extract_stack_compact(),
                    'other_details': {'conn_id': conn_id, 'reason': msg['reason']},
                }
            elif status == 'raised':
                self.done_delegated_task_map[msg_id] = {
                    'status': 'raised',
                    'exception': RuntimeError('Delegated task raised an exception.'),
                    'callstack': extract_stack_compact(),
                    'other_details': {'conn_id': conn_id, 'msg': msg},
                }
            else:
                self.done_delegated_task_map[msg_id] = {
                    'status': 'done',
                    'returning_value': msg['returning_value'],
                }
        else:
            self.to_terminate = True
            self.exit_code = "Unknown message with type '{}'.".format(msg_type)


    async def _listen_and_dispatch_new_msgs(self):
        '''Listens to new messages regularly and dispatches them.'''

        import asyncio

        self.stop_listening = False # let other futures decide when to stop listening
        while not self.stop_listening:
            dispatched = False
            for conn_id, conn in self.conn_list:
                if not self.conn_alive_list[conn_id]:
                    continue
                if conn.poll(0): # has data?
                    msg = conn.recv()
                    self._dispatch_new_msg(conn_id, msg)
                    dispatched = True

            if not dispatched: # has not dispatched anything?
                await asyncio.sleep(0.1) # sleep a bit


    def _inform_death(self, msg):
        '''Informs all other alive bees that it has died.

        Parameters
        ----------
        msg : dict
            message in the form `{'death_type': str, ...}` where death type can be either 'normal' or 'killed'
        '''

        wrapped_msg = {'msg_type': 'dead'}
        wrapped_msg.update(msg)
        for conn_id, conn in self.conn_list:
            if not self.conn_alive_list[conn_id]:
                continue
            conn.send(wrapped_msg)


class WorkerBee(Bee):
    '''A worker bee in the BeeHive concurrency model.

    See parent :class:`Bee` for more details.

    Parameters
    ----------
    conn : multiprocessing.connection.Connection
        a connection to communicate with the queen bee
    profile : str, optional
        the S3 profile from which the context vars are created. See :func:`mt.base.s3.create_context_vars`.
    max_concurrency : int
        maximum number of concurrent tasks that the bee handles at a time

    '''

    def __init__(self, profile, max_concurrency: int = 1024):

        import multiprocessing as mp
        
        super().__init__(max_concurrency=max_concurrency)
        self.profile = profile
        self.q2w_conn, self.w2q_conn = mp.Pipe()
        self.process = mp.Process(target=self._worker_process, daemon=True)
        self.process.start()


    # ----- private -----


    async def _worker_process_async(self):
        from ..s3 import create_context_vars

        async with create_context_vars(profile=self.profile, asyn=True) as context_vars:
            self.context_vars = context_vars
            self.add_new_connection(self.w2q_conn)

            await self._run()


    def _worker_process(self):
        import asyncio
        from ..traceback import extract_stack_compact

        try:
            asyncio.run(self._worker_process_async())
        except Exception as e: # tell the queen bee that the worker bee has been killed by an unexpected exception
            self.w2q_conn.send({
                'msg_type': 'dead',
                'death_type': 'killed',
                'exception': e,
                'callstack': extract_stack_compact(),
            })


    async def _execute_task(self, name: str, args: list = [], kwargs: dict = {}):
        new_kwargs = {'context_vars': self.context_vars}
        new_kwargs.update(kwargs)
        return await super()._execute_task(name, args, new_kwargs)
    _execute_task.__doc__ = Bee._execute_task.__doc__


class QueenBee(Bee):
    '''The queen bee in the BeeHive concurrency model.

    The queen bee and her worker bees work in asynchronous mode. Each worker bee operates within a
    context returned from :func:`mt.base.s3.create_context_vars`, with a given profile. It is asked
    to upper-limit number of concurrent asyncio tasks to a given threshold.

    Upon initialising a queen bee, the user must provide a subclass of :class:`WorkerBee` to
    represent the worker bee class, from which the queen can spawn worker bees. Like other bees, it
    is expected that the user write member asynchornous functions to handle tasks.

    Parameters
    ----------
    worker_bee_class : class
        a subclass of :class:`WorkerBee`
    profile : str, optional
        the S3 profile from which the context vars are created. See :func:`mt.base.s3.create_context_vars`.
    max_concurrency : int
        the maximum number of concurrent tasks at any time for a worker bee, good for managing
        memory allocations. Non-integer values are not accepted.
    '''

    def __init__(self, worker_bee_class, profile: str = None, max_concurrency: int = 1024):
        self.worker_bee_class = worker_bee_class
        self.profile = profile
        if not isinstance(max_concurrency, int):
            raise ValueError("Argument 'max_concurrency' is not an integer: {}.".format(max_concurrency))
        self.max_concurrency = max_concurrency
        self.worker_map = {} # worker_id -> WorkerBee
        self.worker_id = 0

    # MT-TODO: launch the main task, concurrently manage bee spawning and removal


    # ----- private -----


    async def manage_population(self):
        pass # MT-TODO: what do we do?


    def _remove_dead_worker_bee(self, worker_id):
        self.conn_alive_list[worker_id] = False # mark the connection dead
        del self.worker_map[worker_id]


    def _spawn_new_worker_bee(self):
        # get a new worker id
        worker_id = self.worker_id
        self.worker_id += 1

        worker_bee = self.worker_bee_class(profile=self.profile, max_concurrency=self.max_concurrency)
        self.worker_map[worker_id] = worker_bee
        self.add_new_connection(worker_bee.q2c_conn)

        return worker_id
