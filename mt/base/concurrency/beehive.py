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

from ..contextlib import nullcontext


__all__ = ['Bee', 'WorkerBee', 'subprocess_worker_bee', 'QueenBee', 'beehive_run']


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
    `exception` contains an Exception instance and key `traceback` contains a list of text lines
    describing the call stack, and key `other_details` contains other supporting information.
    Finally, if `status` is 'done', key `returning_value` holds the returning value.

    The bee posesses a number of connections to communicate with other bees. There are some special
    messages apart from messages controlling task delegation above. The first one is
    `{'msg_type': 'dead', 'death_type': str, ...}` where `death_type` is either 'normal' or
    'killed'. If it is normal, key `exit_code` is either None or a string representing an exit code
    or a last word. If it is 'killed', key `exception` holds the Exception that caused the bee to
    be killed, and key `traceback` holds a list of text lines describing the call stack. This
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
    conn : multiprocessing.connection.Connection
        connection to the parent who owns the bee
    max_concurrency : int
        maximum number of concurrent tasks that the bee handles at a time
    '''


    def __init__(self, conn, max_concurrency: int = 1024):
        self.conn_list = []
        self.conn_alive_list = []
        if not isinstance(max_concurrency, int):
            raise ValueError("Argument 'max_concurrency' is not an integer: {}.".format(max_concurrency))
        self.max_concurrency = max_concurrency
        self.add_new_connection(conn) # first connection is always the parent


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

        import asyncio

        # generate new msg id
        msg_id = self.msg_id
        self.msg_id += 1

        # send the request
        conn = self.conn_list[conn_id]
        conn.send({'msg_type': 'request', 'msg_id': msg_id, 'task_info': task_info})
        self.requesting_delegated_task_map[msg_id] = conn_id

        async def wait_for_key(key, col):
            # yield control until the task is done
            while key not in col:
                await asyncio.sleep(0)
        wait_task = asyncio.ensure_future(wait_for_key(msg_id, self.done_delegated_task_map))
        await asyncio.wait([wait_task])

        # get the result
        task_result = self.done_delegated_task_map[msg_id]
        del self.done_delegated_task_map[msg_id] # pop it

        return task_result


    async def run(self):
        '''Implements the life-cycle of the bee. Invoke this function only once.'''
        await self._run_initialise()
        while (not self.to_terminate) or self.pending_request_list or self.working_task_map:
            await self._run_inloop()
        await self._run_finalise()


    # ----- private -----


    async def _run_initialise(self):
        '''TBD'''

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

        self.listening_task = asyncio.ensure_future(self._listen_and_dispatch_new_msgs())


    async def _run_inloop(self):
        '''TBD'''

        import asyncio

        # transform some pending requests into working tasks
        num_requests = min(self.max_concurrency - len(self.working_task_map), len(self.pending_request_list))
        for i in range(num_requests):
            conn_id, msg_id, task_info = self.pending_request_list.pop(0) # pop it
            #print("task_info", task_info)
            task = asyncio.ensure_future(self._execute_task(**task_info))
            self.working_task_map[task] = (conn_id, msg_id)

        # await for some tasks to be done
        if self.working_task_map:
            done_task_set, _ = await asyncio.wait(self.working_task_map.keys(), timeout=0.1, return_when=asyncio.FIRST_COMPLETED)

            # process each of the tasks done by the bee
            for task in done_task_set:
                self._deliver_done_task(task)
        else:
            await asyncio.sleep(0.1)


    async def _run_finalise(self):
        '''TBD'''

        import asyncio

        self.stop_listening = True
        await asyncio.wait([self.listening_task])
        self._inform_death({'death_type': 'normal', 'exit_code': self.exit_code})


    async def _execute_task(self, name: str, args: list = [], kwargs: dict = {}):
        '''Processes a request message coming from one of the connecting bees.

        The default behaviour implemented here is to invoke the member asyn function with the same
        name.

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
                'traceback': extract_stack_compact(),
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
                'traceback': extract_stack_compact(),
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
            #print("task_cancelled")
        elif task.exception() is not None:
            #print("task_raised")
            tracestack = io.StringIO()
            task.print_stack(file=tracestack)

            msg = {
                'msg_type': 'result',
                'msg_id': msg_id,
                'status': 'raised',
                'exception': task.exception(),
                'traceback': tracestack.getvalue(),
                'other_details': None,
            }
            #print("msg=",msg)
            conn.send(msg)
        else:
            #print("task_done")
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
            #print("request: conn_id={}, msg_id={} for {}".format(conn_id, msg_id, self))
            if self.to_terminate:
                conn.send({'msg_type': 'acknowledged', 'msg_id': msg_id, 'accepted': False}) # reject the request
            else:
                self.pending_request_list.append((conn_id, msg_id, msg['task_info']))
                conn.send({'msg_type': 'acknowledged', 'msg_id': msg_id, 'accepted': True})
        elif msg_type == 'acknowledged': # delegated task accepted or not
            msg_id = msg['msg_id']
            conn_id = self.requesting_delegated_task_map[msg_id]
            #print("acknowledged: conn_id={}, msg_id={} for {}".format(conn_id, msg_id, self))
            del self.requesting_delegated_task_map[msg_id] # pop it
            if msg['accepted']:
                self.started_delegated_task_map[msg_id] = conn_id
            else:
                self.done_delegated_task_map[msg_id] = {
                    'status': 'raised',
                    'exception': RuntimeError('Delegated task rejected.'),
                    'traceback': extract_stack_compact(),
                    'other_details': {'conn_id': conn_id},
                }
        elif msg_type == 'result': # delegated task done
            msg_id = msg['msg_id']
            status = msg['status']
            conn_id = self.started_delegated_task_map[msg_id]
            #print("result: conn_id={}, msg_id={} for {}".format(conn_id, msg_id, self))
            del self.started_delegated_task_map[msg_id] # pop it
            if status == 'cancelled':
                self.done_delegated_task_map[msg_id] = {
                    'status': 'raised',
                    'exception': RuntimeError('Delegated task cancelled.'),
                    'traceback': extract_stack_compact(),
                    'other_details': {'conn_id': conn_id, 'reason': msg['reason']},
                }
            elif status == 'raised':
                self.done_delegated_task_map[msg_id] = {
                    'status': 'raised',
                    'exception': RuntimeError('Delegated task raised an exception.'),
                    'traceback': extract_stack_compact(),
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
            #print("listening:", self)
            for conn_id, conn in enumerate(self.conn_list):
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
        for conn_id, conn in enumerate(self.conn_list):
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
    max_concurrency : int
        maximum number of concurrent tasks that the bee handles at a time
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`.
    '''

    def __init__(self, conn: mc.Connection, max_concurrency: int = 1024, context_vars: dict = {}):
        super().__init__(conn, max_concurrency=max_concurrency)
        self.context_vars = context_vars


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
        return len(self.working_task_map) / self.max_concurrency


    # ----- private -----


    async def _execute_task(self, name: str, args: list = [], kwargs: dict = {}):
        new_kwargs = {'context_vars': self.context_vars}
        new_kwargs.update(kwargs)
        return await super()._execute_task(name, args, new_kwargs)
    _execute_task.__doc__ = Bee._execute_task.__doc__


def subprocess_worker_bee(workerbee_class, profile: str, max_concurrency: int = 1024):
    '''Creates a daemon subprocess that holds a worker bee and runs the bee in the subprocess.

    Parameters
    ----------
    workerbee_class : class
        subclass of :class:`WorkerBee` that shares the same constructor arguments as the super
        class
    profile : str, optional
        the S3 profile from which the context vars are created.
        See :func:`mt.base.s3.create_context_vars`.
    max_concurrency : int
        maximum number of concurrent tasks that the bee handles at a time

    Returns
    -------
    process : multiprocessing.Process
        the created subprocess
    conn : multiprocessing.connection.Connection
        the connection to allow the parent to communicate with the worker bee
    '''

    async def subprocess_asyn(workerbee_class, conn: mc.Connection, profile: str, max_concurrency: int = 1024):
        from ..s3 import create_context_vars

        async with create_context_vars(profile=profile, asyn=True) as context_vars:
            bee = workerbee_class(conn, max_concurrency=max_concurrency, context_vars=context_vars)
            await bee.run()

    def subprocess(workerbee_class, conn: mc.Connection, profile: str, max_concurrency: int = 1024):
        import asyncio
        from ..traceback import extract_stack_compact

        try:
            asyncio.run(subprocess_asyn(workerbee_class, conn, profile, max_concurrency=max_concurrency))
        except Exception as e: # tell the queen bee that the worker bee has been killed by an unexpected exception
            msg = {
                'msg_type': 'dead',
                'death_type': 'killed',
                'exception': e,
                'traceback': extract_stack_compact(),
            }
            #print("WorkerBee exception msg", msg)
            conn.send(msg)

    import multiprocessing as mp

    w2q_conn, q2w_conn = mp.Pipe()
    process = mp.Process(
        target=subprocess,
        args=(workerbee_class, w2q_conn, profile),
        kwargs={'max_concurrency': max_concurrency},
        daemon=True)
    process.start()

    return process, q2w_conn


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
    conn : multiprocessing.connection.Connection
        connection to the user, where the user delegates tasks to the queen bee
    worker_bee_class : class
        a subclass of :class:`WorkerBee`
    profile : str, optional
        the S3 profile from which the context vars are created. See :func:`mt.base.s3.create_context_vars`.
    max_concurrency : int
        the maximum number of concurrent tasks at any time for a worker bee, good for managing
        memory allocations. Non-integer values are not accepted.
    context_vars : dict
        a dictionary of context variables within which the queen be functions runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`.
    '''

    def __init__(self, conn, worker_bee_class, profile: str = None, max_concurrency: int = 1024, context_vars: dict = {}):
        super().__init__(conn, max_concurrency=max_concurrency, context_vars=context_vars)

        self.worker_bee_class = worker_bee_class
        self.profile = profile
        self.worker_map = {} # worker_id -> (conn: mc.Connection, respected: bool, process: mp.Process)
        self.worker_id = 1 # 0 is for the parent


    # ----- private -----


    async def _manage_population(self):
        '''Manages the population of worker bees regularly.'''

        import asyncio
        import multiprocessing as mp

        from .base import used_cpu_too_much, used_memory_too_much

        min_num_workers = 1
        max_num_workers = max(mp.cpu_count()-1, min_num_workers)

        self.stop_managing = False # let other futures decide when to stop managing
        while not self.stop_managing:
            num_workers = len(self.worker_map)

            if num_workers < min_num_workers:
                self._spawn_new_worker_bee()
            elif (num_workers > min_num_workers) and ((num_workers > max_num_workers) or used_cpu_too_much() or used_memory_too_much()):
                # pick the least busy worker bee
                best_worker_id = -1
                best_busy_status = 1.1
                for worker_id in self.worker_map:
                    respected = self.worker_map[worker_id][1]
                    if not respected:
                        continue
                    task_info = {'name': 'busy_status', 'args': [], 'kwargs': {}}
                    task_result = await self.delegate_task(worker_id, task_info)
                    if task_result['status'] != 'done':
                        best_worker_id = worker_id
                        best_busy_status = 0
                        break
                    busy_status = task_result['returning_value']
                    if busy_status < best_busy_status:
                        best_worker_id = worker_id
                        best_busy_status = busy_status

                # disrespect it and ask it to die
                self.worker_map[best_worker_id][1] = False
                self.conn_list[best_worker_id].send({'msg_type': 'die'})
            elif num_workers < max_num_workers:
                self._spawn_new_worker_bee()

            await asyncio.sleep(10) # sleep for 10 seconds


    def _spawn_new_worker_bee(self):
        # get a new worker id
        worker_id = self.worker_id
        self.worker_id += 1

        process, q2w_conn = subprocess_worker_bee(self.worker_bee_class, self.profile, max_concurrency=self.max_concurrency)
        self.worker_map[worker_id] = [q2w_conn, True, process] # new worker and is respected
        self.add_new_connection(q2w_conn)
        #print("spawned", self.worker_map, self.conn_list, self.conn_alive_list)

        return worker_id


    def _mourn_death(self, conn_id, msg): # msg = {'death_type': str, ...}
        super()._mourn_death(conn_id, msg)
        del self.worker_map[conn_id]
    _mourn_death.__doc__ = Bee._mourn_death.__doc__


    async def _run_initialise(self):
        await super()._run_initialise()
        import asyncio
        self.managing_task = asyncio.ensure_future(self._manage_population())


    async def _run_finalise(self):
        import asyncio

        # ask every worker bee to die gracefully
        for worker_id in self.worker_map:
            respected = self.worker_map[worker_id][1]
            if not respected:
                continue
            self.worker_map[worker_id][1] = False
            self.conn_list[worker_id].send({'msg_type': 'die'})

        # await until every worker bee has died
        while self.worker_map:
            await asyncio.sleep(0)

        self.stop_managing = True
        await asyncio.wait([self.managing_task])

        await super()._run_finalise()


async def beehive_run(queenbee_class, workerbee_class, task_name: str, task_args: list = [], task_kwargs: dict = {}, profile: str = None, max_concurrency: int = 1024, context_vars: dict = {}, logger=None):
    '''An asyn function that runs a task in a BeeHive concurrency model.

    Parameters
    ----------
    queenbee_class : class
        a subclass of :class:`QueenBee`
    workerbee_class : class
        a subclass of :class:WorkerBee`
    task_name : str
        name of a task assigned to the queen bee, or in the default behaviour, the queen bee's
        member asyn function handling the task
    task_args : list
        positional arguments to be passed to the member function as-is
    task_kwargs : dict
        keyword arguments to be passed to the member function as-is
    profile : str, optional
        the S3 profile from which the context vars are created. See :func:`mt.base.s3.create_context_vars`.
    max_concurrency : int
        maximum number of concurrent tasks that the bee handles at a time
    context_vars : dict
        a dictionary of context variables within which the function runs. It must include
        `context_vars['async']` to tell whether to invoke the function asynchronously or not.
        In addition, variable 's3_client' must exist and hold an enter-result of an async with
        statement invoking :func:`mt.base.s3.create_s3_client`.
    logger : logging.Logger or equivalent
        logger for debugging purposes

    Returns
    -------
    user-defined

    Raises
    ------
    TBD
    '''

    if not context_vars['async']:
        raise ValueError("The function can only be run in asynchronous mode.")

    import multiprocessing as mp
    import asyncio

    # create a pipe
    q2u_conn, u2q_conn = mp.Pipe()

    # create a queen bee and starts her life
    queen = queenbee_class(q2u_conn, workerbee_class, profile=profile, max_concurrency=max_concurrency, context_vars=context_vars)
    queen_life = asyncio.ensure_future(queen.run())

    # delegate the task
    task_info = {'name': task_name, 'args': task_args, 'kwargs': task_kwargs}
    u2q_conn.send({
        'msg_type': 'request',
        'msg_id': 0,
        'task_info': task_info,
    })

    # wait for the task to be done
    while not u2q_conn.poll(0):
        await asyncio.sleep(1)

    # get the result
    msg = u2q_conn.recv()

    if msg['msg_type'] == 'acknowledged':
        if not msg['accepted']:
            raise RuntimeError("The queen bee rejected the task.")

        # wait for the task to be done
        while not u2q_conn.poll(0):
            await asyncio.sleep(1)

        # get the result
        msg = u2q_conn.recv()

    # tell the queen to die
    u2q_conn.send({'msg_type': 'die'})

    # wait for her to die gracefully
    await asyncio.wait([queen_life])

    # intepret the result
    #print("msg to heaven", msg)
    if msg['status'] == 'cancelled':
        raise RuntimeError("The queen bee cancelled the task. Reason: {}".format(msg['reason']))

    if msg['status'] == 'raised':
      with logger.scoped_debug("Exception raised by the queen bee", curly=False) if logger else nullcontext():
        with logger.scoped_debug("Traceback", curly=False) if logger else nullcontext():
            logger.debug(msg['traceback'])
        with logger.scoped_debug("Other details", curly=False) if logger else nullcontext():
            logger.debug(msg['other_details'])
        raise msg['exception']

    else: # msg['status'] == done'
        return msg['returning_value']
