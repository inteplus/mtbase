"""Concurrency using multiprocessing only."""

import os
from time import sleep
import multiprocessing as mp
import multiprocessing.connection as mpc
import threading as _t
import queue as _q

from mt import tp, logg

from mt.base.bg_invoke import BgInvoke, BgThread, BgException
from .base import used_memory_too_much


INTERVAL = 0.5  # 0.5 seconds interval
HEARTBEAT_PERIOD = 5  # send a heart beat every 5 intervals
MAX_MISS_CNT = 240  # number of times before we declare that the other process has died


def worker_process(
    func,
    heartbeat_pipe: mpc.Connection,
    queue_in: mp.Queue,
    queue_out: mp.Queue,
    logger: tp.Optional[logg.IndentedLoggerAdapter] = None,
):
    """The worker process.

    The worker process operates in the following way. There is a pipe, named `heartbeat_pipe` to
    communicate between the worker and its parent process about their heartbeats. Each side of the
    pipe constantly sends a heartbeat in every 0.5 seconds. If one side does not hear any heartbeat
    within 5*240 seconds, then it can assume the other side has died for some unexpected reason.
    After detecting this fateful event, the parent process can abruptly kill the worker process,
    but in contrast, the worker can only finish its own fate.

    A heart beat from the parent process is a byte. If the heartbeat is 0, the parent is alive. If
    it is 1, the worker process must do a seppuku.

    A heart beat from the worker process is also a byte. If it is 0, the worker is alive. If it is
    1, the worker signals that it has received keyboard interruption and is about to die in peace.

    There is global pair of queues, `queue_in` and `queue_out`, to distribute workloads. Workloads
    are divided into smaller pieces identified by zero-based work ids. The parent process sends work
    ids to `queue_in`. Any worker process can extract a work id from the queue, do the work, and
    return the outcome to `queue_out`. The data in `queue_in` are pure integers. The data in
    `queue_out` are tuples of the form `(work_id, ...)`.

    The parent process owns all the pipes and queues.

    Parameters
    ----------
    func : function
        a function taking work_id as the only argument and returning something. The function is run
        in a background thread of the worker process
    heartbeat_pipe : multiprocessing.connection.Connection
        the child connection of a parent-child pipe to communicate with the parent about their
        heartbeats
    queue_in : multiprocessing.Queue
        queue containing work ids
    queue_out : multiprocessing.Queue
        queue containing results `(work_id, ...)`
    logger : IndentedLoggerAdapter, optional
        logger for debugging purposes
    """

    to_die = False
    miss_cnt = 0
    hb_cnt = 0

    bg_thread = BgThread(func)
    has_work = False
    work_id = -1
    result_list = []

    while True:
        if to_die:  # die as soon as we have the opportunity
            if (not has_work) and (len(result_list) == 0):
                # print("  dying gracefully!", work_id)
                break

        # check the background thread
        if has_work and (not bg_thread.is_running()):
            try:
                result_list.append((work_id, bg_thread.result))
            except BgException:  # premature death
                if logger:
                    logger.warn_last_exception()
                    logger.warn(
                        f"Uncaught exception killed worker pid {os.getpid()}."
                    )
                to_die = True
            has_work = False

        # attempt to put some result to queue_out
        while result_list:
            # print("result_list",result_list)
            try:
                queue_out.put_nowait(result_list[-1])
                result_list.pop()
            except _q.Full:
                break

        # get a work id
        if (not to_die) and (not has_work):  # and not used_memory_too_much():
            work_id = -1
            try:
                work_id = queue_in.get_nowait()
            except _q.Empty:
                pass

            if work_id >= 0:
                # print("work_id", work_id)
                has_work = True
                bg_thread.invoke(work_id)  # do the work in background

        # heartbeats
        hb_cnt += 1
        if hb_cnt >= HEARTBEAT_PERIOD:
            hb_cnt = 0
            heartbeat_pipe.send_bytes(bytes((0,)))
        if heartbeat_pipe.poll():
            miss_cnt = 0
            buf = heartbeat_pipe.recv_bytes(16384)
            for x in buf:
                if x == 1:  # request from parent to die
                    to_die = True
        elif not to_die:
            miss_cnt += 1
            if miss_cnt >= MAX_MISS_CNT:  # seppuku
                if logger:
                    logger.warn(
                        f"Uncaught exception killed worker pid {os.getpid()}."
                    )
                to_die = True

        # sleep until next time
        try:
            sleep(INTERVAL)
        except KeyboardInterrupt:
            # the parent process may have died by now, but we want to notify anyway
            heartbeat_pipe.send_bytes(bytes((1,)))
            sleep(INTERVAL)
            to_die = True

    bg_thread.close()
    queue_out.cancel_join_thread()
    queue_in.cancel_join_thread()
    heartbeat_pipe.close()


class ProcessParalleliser(object):

    """Run a function with different inputs in parallel using multiprocessing.

    Parameters
    ----------
    func : function
        a function to be run in parallel. The function takes as input a non-negative integer
        'work_id' and returns some result.
    max_num_workers : int, optional
        maximum number of concurrent workers or equivalently processes to be allocated
    logger : IndentedLoggerAdapter, optional
        for logging messages
    """

    def __init__(self, func, max_num_workers=None, logger=None):
        if max_num_workers is None:
            max_num_workers = mp.cpu_count()

        self.func = func
        self.logger = logger
        self.queue_in = mp.Queue()
        self.queue_out = mp.Queue()
        self.num_workers = max_num_workers
        self.miss_cnt_list = [0] * self.num_workers
        self.pipe_list = []
        self.process_list = []
        for i in range(self.num_workers):
            if used_memory_too_much():
                break
            pipe = mp.Pipe()
            self.pipe_list.append(pipe[0])
            p = mp.Process(
                target=worker_process,
                args=(func, pipe[1], self.queue_in, self.queue_out),
                kwargs={"logger": logger},
            )
            p.start()  # start the process
            self.process_list.append(p)
        self.num_workers = len(self.process_list)

        # launch a background thread to communicate constantly with the worker processes
        self.state = "living"
        self.bg_thread = BgInvoke(self._process)

    def _process(self):
        """A background process to communicate with the worker processes."""

        death_code = "normal"
        hb_cnt = 0
        while True:
            try:
                all_dead = True
                for i, p in enumerate(self.process_list):
                    try:
                        if not p.is_alive():
                            self.state = "dying"
                            continue

                        # heartbeats
                        pipe = self.pipe_list[i]
                        val = int(self.state != "living")
                        if hb_cnt == 0:
                            pipe.send_bytes(bytes((val,)))
                        if pipe.poll():
                            all_dead = False
                            self.miss_cnt_list[i] = 0
                            buf = pipe.recv_bytes(16384)  # cleanse the pipe
                            # self.logger.debug("Pipe from {}: {}".format(p.pid, buf))
                            for x in buf:
                                if x == 1:
                                    if self.logger:
                                        self.logger.debug(
                                            f"Worker {p.pid} wanted parent to die."
                                        )
                                    if death_code == "normal":
                                        death_code = "keyboard_interrupted"
                                    self.state = "dying"
                                    break
                        else:
                            self.miss_cnt_list[i] += 1
                            if (
                                self.miss_cnt_list[i] >= MAX_MISS_CNT
                            ):  # no heartbeat for too long
                                if death_code == "normal":
                                    death_code = "worker_died_prematurely"
                                self.state = "dying"
                                if p.is_alive():
                                    pipe.send_bytes(bytes((1,)))
                                self.miss_cnt_list[i] = 0
                            else:  # assume the current worker still lives
                                all_dead = False
                    except (EOFError, BrokenPipeError, ConnectionResetError):
                        if death_code == "normal":
                            death_code = "broken_pipe_or_something"
                        self.state = "dying"
                    except:  # broken pipe or something, assume worker process is dead
                        if self.logger:
                            self.logger.warn_last_exception()
                        if death_code == "normal":
                            death_code = "uncaught_exception"
                        self.state = "dying"

                # self.logger.debug("  -- main process", all_dead)
                if all_dead:
                    break

                hb_cnt += 1
                if hb_cnt >= HEARTBEAT_PERIOD:
                    hb_cnt = 0

                # sleep until next time
                try:
                    sleep(INTERVAL)
                except KeyboardInterrupt:
                    if logger:
                        logger.warn(
                            "Process {} interrupted by keyboard.".format(os.getpid())
                        )
                    if death_code == "normal":
                        death_code = "keyboard_interrupted"
                    self.state = "dying"
            except:
                if logger:
                    logger.warn_last_exception()
                    logger.warn(
                        "Uncaught exception above killed process.".format(os.getpid())
                    )
                break

        if death_code != "normal":
            if logger:
                logger.warn(
                    "Process {} died with reason: {}.".format(os.getpid(), death_code)
                )

        # self.logger.debug("  -- main process closing")
        self._close()

    def __del__(self):
        self.close()

    def _close(self):
        """Closes the instance when all worker processes have died."""

        if self.state == "dead":
            return

        for p in self.process_list:
            p.join()

        self.state = "dead"

    def close(self):
        """Closes the instance properly."""
        if self.state == "living":
            self.state = "dying"
        # while self.state != 'dead':
        # sleep(INTERVAL)

    def push(self, work_id, timeout=30):
        """Pushes a work id to the background to run the provided function in parallel.

        Parameters
        ----------
        work_id : int
            non-negative integer to be provided to the function
        timeout : float
            number of seconds to wait to push the id to the queue before bailing out

        Returns
        -------
        bool
            whether or not the work id has been pushed

        Notes
        -----
        You should use :func:`pop` or :func:`empty` to check for the output of each work.
        """
        if self.state != "living":  # we can't accept more work when we're not living
            return False
        if not isinstance(work_id, int):
            raise ValueError("Work id is not an integer. Got {}.".format(work_id))
        if work_id < 0:
            raise ValueError("Work id is negative. Got {}.".format(work_id))
        try:
            self.queue_in.put(work_id, block=True, timeout=timeout)
            return True
        except _q.Full:
            return False

    def empty(self):
        """Returns whether the output queue is empty."""
        return self.queue_out.empty()

    def pop(self, timeout=60 * 60):
        """Returns a pair (work_id, result) when at least one such pair is available.

        Parameters
        ----------
        timeout : float
            number of seconds to wait to pop a work result from the queue before bailing output

        Returns
        -------
        int
            non-negative integer representing the work id
        object
            work result

        Raises
        ------
        queue.Empty
            if there is no work result after the timeout
        """
        if self.state == "dead":
            raise RuntimeError(
                "The process paralleliser has been closed. Please reinstantiate."
            )
        return self.queue_out.get(block=True, timeout=timeout)


# ----------------------------------------------------------------------


class WorkIterator(object):
    """Iterates work from id 0 to infinity, returning the work result in each iteration.

    By default, the class invokes ProcessParalleliser to do a few works ahead of time. It switches
    to serial mode if requested.

    Parameters
    ----------
    func : function
        a function representing the work process. The function takes as input a non-negative
        integer 'work_id' and returns some result in the form of `(work_id, ...)` if successful
        else None.
    buffer_size : int, optional
        maximum number of work resultant items to be buffered ahead of time. If not specified,
        default to be twice the number of processes.
    push_timeout : float, optional
        timeout in second for each push to input queue. See :func:`ProcessParalleliser.push`.
    pop_timeout : float, optional
        timeout in second for each pop from output queue. See :func:`ProcessParalleliser.pop`.
    skip_null : bool, optional
        whether or not to skip the iteration that contains None as the work result.
    logger : IndentedLoggerAdapter, optional
        for logging messages
    max_num_workers : int, optional
        maximum number of concurrent workers or equivalently processes to be allocated
    serial_mode : bool
        whether or not to activate the serial mode. Useful when the number of works is small.
    logger : logging.Logger, optional
        logger for debugging purposes

    Notes
    -----
    Instances of the class qualify as a thread-safe Python iterator. Each iteration returns a
    (work_id, result) pair. To avoid a possible deadlock during garbage collection, it is
    recommended to explicitly invoke :func:`close` to clean up background processes.

    As of 2021/2/17, instances of WorkIterator can be used in a with statement. Upon exiting,
    :func:`close` is invoked.

    As of 2021/4/30, you can switch version of paralleliser.

    As of 2021/08/13, the class has been extended to do work in serial if the number of works is
    provided and is less than or equal a provided threshold.
    """

    def __init__(
        self,
        func,
        buffer_size=None,
        skip_null: bool = True,
        push_timeout=30,
        pop_timeout=60 * 60,
        max_num_workers=None,
        serial_mode=False,
        logger=None,
    ):
        self.skip_null = skip_null
        self.send_counter = 0
        self.recv_counter = 0
        self.retr_counter = 0
        if serial_mode:
            self.serial_mode = True
            self.func = func
        else:
            self.serial_mode = False
            self.paralleliser = ProcessParalleliser(
                func, max_num_workers=max_num_workers, logger=logger
            )
            self.push_timeout = push_timeout
            self.pop_timeout = pop_timeout
            self.push_timeout = push_timeout
            self.pop_timeout = pop_timeout
            self.buffer_size = (
                len(self.paralleliser.process_list) * 2
                if buffer_size is None
                else buffer_size
            )
            self.lock = _t.Lock()
            self.alive = True

    # ----- cleaning up resources -----

    def close(self):
        """Closes the iterator for further use."""
        if self.serial_mode:
            return
        with self.lock:
            if not self.alive:
                return

            self.paralleliser.close()
            self.alive = False

    def __del__(self):
        self.close()

    # ----- with-compatibility -----

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self.close()

    # ----- next ------

    def __next__(self):
        if self.serial_mode:
            while True:
                result = self.func(self.send_counter)
                self.send_counter += 1
                self.recv_counter += 1
                if self.skip_null and result is None:
                    continue
                self.retr_counter += 1
                return result

        with self.lock:
            if not self.alive:
                raise RuntimeError(
                    "The instance has been closed. Please reinstantiate."
                )

            while True:
                if self.paralleliser.state == "dead":
                    raise RuntimeError(
                        "Cannot get the next item because the internal paralleliser has been dead."
                    )

                max_items = max(
                    self.recv_counter + self.buffer_size - self.send_counter, 0
                )
                for i in range(max_items):
                    if not self.paralleliser.push(
                        self.send_counter, timeout=self.push_timeout
                    ):
                        break  # can't push, maybe timeout?
                    self.send_counter += 1

                if self.send_counter <= self.recv_counter:
                    raise RuntimeError(
                        "Unable to push some work to background processes."
                    )

                work_id, result = self.paralleliser.pop(timeout=self.pop_timeout)
                self.recv_counter += 1

                if self.skip_null and result is None:
                    continue

                self.retr_counter += 1
                return result

    def next(self):
        return self.__next__()

    def __iter__(self):
        return self
