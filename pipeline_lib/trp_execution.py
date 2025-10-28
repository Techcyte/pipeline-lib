import contextlib
import logging
from multiprocessing.context import (
    BaseContext,
    ForkContext,
    ForkProcess,
    SpawnContext,
    SpawnProcess,
)
import multiprocessing.connection as mp_connection
import os
import queue
import signal
import sys
import threading as tr
import time
import traceback
import typing
import warnings
from collections import deque
from threading import RLock, Semaphore, get_native_id
from typing import Any, Iterable, List, Optional, Set, Union
import multiprocessing as mp

from pipeline_lib.mp_execution import (
    ERR_BUF_SIZE,
    PYTHON_ERR_EXIT_CODE,
    BufferedQueue,
    SignalReceived,
    SpawnContextName,
)

from .pipeline_task import DEFAULT_BUF_SIZE, InactivityError, PipelineTask, TaskError
from .type_checking import MAX_NUM_WORKERS, sanity_check_mp_params

logger = logging.getLogger(__name__)


class PropogateErr(RuntimeError):
    pass


class TaskOutput:
    def __init__(
        self,
        num_upstream_tasks: int,
        packets_in_flight: int,
        error_queue: BufferedQueue,
        last_updated_time: Any,
    ) -> None:
        self.num_tasks_remaining = num_upstream_tasks
        self.queue_len = Semaphore(value=0)
        self.packets_space = Semaphore(value=packets_in_flight)
        self.queue: deque = deque(maxlen=packets_in_flight)
        self.last_updated_time = last_updated_time
        self.lock = RLock()
        self.error_info = None
        self.error_queue = error_queue

    def iter_results(self) -> Iterable[Any]:
        while True:
            self.queue_len.acquire()  # pylint: disable=consider-using-with
            if self.is_errored():
                raise PropogateErr()
            try:
                item = self.queue.popleft()
            except IndexError:
                # only happens when out of results
                return
            yield item

            # this release needs to happen after the yield
            # completes to support full synchronization semantics with packets_in_flight=1
            self.packets_space.release()

            # store the updated time to register that progress was made in the pipeline
            self.last_updated_time = time.time()

    def put_results(self, iterable: Iterable[Any]):
        iterator = iter(iterable)
        try:
            while True:
                # wait for space to be available on queue before iterating to next item
                # essential for full synchronization semantics with packets_in_flight=1
                self.packets_space.acquire()  # pylint: disable=consider-using-with

                if self.is_errored():
                    raise PropogateErr()

                item = next(iterator)

                self.queue.append(item)
                self.queue_len.release()
        except StopIteration:
            # normal end of iteration
            with self.lock:
                self.num_tasks_remaining -= 1
                if self.num_tasks_remaining == 0:
                    for _i in range(MAX_NUM_WORKERS):
                        self.queue_len.release()

    def is_errored(self):
        with self.lock:
            return self.error_info is not None

    def set_error(self, task_name, err, traceback_str):
        with self.lock:
            if self.error_info is None:
                self.error_info = (task_name, err, traceback_str)
                self.error_queue.put((task_name, err, traceback_str))
        # release all consumers and producers semaphores so that they exit quickly
        for _i in range(MAX_NUM_WORKERS):
            self.queue_len.release()
            self.packets_space.release()


def _start_source(
    task: PipelineTask,
    downstream: TaskOutput,
):
    try:
        out_iter = task.generator(**task.constants_dict)
        downstream.put_results(out_iter)
    except Exception as err:  # pylint: disable=broad-except
        tb_str = traceback.format_exc()
        downstream.set_error(task.name, err, tb_str)
        # exiting directly instead of re-raising error, as that would clutter stderr
        # with duplicate tracebacks
        sys.exit(PYTHON_ERR_EXIT_CODE)


def _start_worker(
    task: PipelineTask,
    upstream: TaskOutput,
    downstream: TaskOutput,
):
    try:
        generator_input = upstream.iter_results()
        out_iter = task.generator(generator_input, **task.constants_dict)
        downstream.put_results(out_iter)
    except Exception as err:  # pylint: disable=broad-except
        tb_str = traceback.format_exc()
        # sets upstream and downstream so that error propagates throughout the system
        downstream.set_error(task.name, err, tb_str)
        upstream.set_error(task.name, err, tb_str)
        # exiting directly instead of re-raising error, as that would clutter stderr
        # with duplicate tracebacks
        sys.exit(PYTHON_ERR_EXIT_CODE)


def _start_sink(
    task: PipelineTask,
    upstream: TaskOutput,
):
    try:
        generator_input = upstream.iter_results()
        task.generator(generator_input, **task.constants_dict)
    except Exception as err:  # pylint: disable=broad-except
        tb_str = traceback.format_exc()
        upstream.set_error(task.name, err, tb_str)
        # exiting directly instead of re-raising error, as that would clutter stderr
        # with duplicate tracebacks
        sys.exit(PYTHON_ERR_EXIT_CODE)


@contextlib.contextmanager
def sighandler(signums: Set[int], processes: List[Union[ForkProcess, SpawnProcess]]):
    def sigterm_handler(signum, _frame):
        # propogate the signal to children processes
        for proc in processes:
            # os.kill just sends a signal like the command line tool
            try:
                os.kill(proc.ident, signum)
            except ProcessLookupError:
                logger.warning(f"Failed to find process {proc.ident}")
        # throw an exception to trigger the exceptional cleanup policy
        raise SignalReceived(signum)

    old_handlings = {}
    for signum in signums:
        old_handlings[signum] = signal.getsignal(signum)
        signal.signal(signum, sigterm_handler)
    did_reset_handling = False
    try:
        yield
    except SignalReceived as sigerr:
        if sigerr.signum in signums:
            # if the signal was raised by our signal handler, then retry the old signal handling method
            # so the end user of the library can handle signals in the way they wish to
            for signum, old_handling in old_handlings.items():
                signal.signal(signum, old_handling)
            did_reset_handling = True
            signal.raise_signal(sigerr.signum)
            # else re-raise error
        else:
            raise sigerr
    finally:
        # only reset handling here if not done in except statement
        if not did_reset_handling:
            for signum, old_handling in old_handlings.items():
                signal.signal(signum, old_handling)


def _warn_parameter_overrides(tasks: List[PipelineTask]):
    for task in tasks:
        if (
            task.max_message_size is not None
            and task.max_message_size != DEFAULT_BUF_SIZE
        ):
            warnings.warn(
                f"Task '{task.name}' overrode default value of max_message_size, and this override is ignored by 'thread' parallelism strategy."
            )


def execute_thread_queue_errors(
    tasks: List[PipelineTask], err_queue: BufferedQueue, last_updated_times: List[Any]
):
    if not tasks:
        return

    if len(tasks) == 1:
        (task,) = tasks
        task.generator(**task.constants_dict)
        return

    source_task = tasks[0]
    sink_task = tasks[-1]
    worker_tasks = tasks[1:-1]
    clean_completed: Set[int] = set()

    # number of processes are of the producing task
    data_streams = [
        TaskOutput(t.num_workers, t.packets_in_flight, err_queue, last_update_time)
        for t, last_update_time in zip(tasks[:-1], last_updated_times)
    ]
    # only one source thread per program
    threads: List[tuple[str, tr.Thread]] = [
        (
            source_task.name,
            tr.Thread(
                target=_start_source,
                args=(
                    source_task,
                    data_streams[0],
                ),
            ),
        )
    ]
    for i, worker_task in enumerate(worker_tasks):
        for _ in range(worker_task.num_workers):
            threads.append(
                (
                    worker_task.name,
                    tr.Thread(
                        target=_start_worker,
                        args=(
                            worker_task,
                            data_streams[i],
                            data_streams[i + 1],
                        ),
                    ),
                )
            )

    for _ in range(sink_task.num_workers):
        threads.append(
            (
                sink_task.name,
                tr.Thread(
                    target=_start_sink,
                    args=(
                        sink_task,
                        data_streams[-1],
                    ),
                ),
            )
        )

    for name, thread in threads:
        thread.start()

    for name, thread in threads:
        thread.join()


def execute_trp(
    tasks: List[PipelineTask],
    spawn_method: SpawnContextName,
    inactivity_timeout: Optional[float],
):
    # pylint: disable=too-many-branches,too-many-locals,too-many-statements
    """
    execute tasks until final task completes.
    Raises error if tasks are inconsistently specified or if
    one of the tasks raises an error.

    Also raises an error if no message passing is observed in any task for
    at least `inactivity_timeout` seconds.
    (useful to kill any stuck jobs in a larger distributed system)
    """
    if not tasks:
        return

    sanity_check_mp_params(tasks)
    _warn_parameter_overrides(tasks)

    if len(tasks) == 1:
        (task,) = tasks
        task.generator(**task.constants_dict)
        return

    ctx = typing.cast(Union[ForkContext, SpawnContext], mp.get_context(spawn_method))
    n_total_tasks = sum(task.num_workers for task in tasks)
    # use a BufferedQueue because it synchronizes instantly, unlike PipedQueue or mp.queue
    err_queue = BufferedQueue(ERR_BUF_SIZE, n_total_tasks + 2, False, ctx)

    last_updated_times = [
        ctx.Value("d", time.time(), lock=False) for _ in range(len(tasks) - 1)
    ]
    subprocess = ctx.Process(
        target=execute_thread_queue_errors, args=[tasks, err_queue, last_updated_times]
    )
    subprocess.start()

    # signal setup must be *after* all new processes are started, so that main processes
    # signal handling won't be copied over to children
    with sighandler({signal.SIGINT, signal.SIGTERM}, [subprocess]):
        done_sentinels = None
        try:
            done_sentinels = mp_connection.wait(
                [subprocess.sentinel],
                timeout=(
                    None if inactivity_timeout is None else inactivity_timeout / 10
                ),
            )
            last_updated_time = max(
                float(last_updated_time.value)
                for last_updated_time in last_updated_times
            )
            if inactivity_timeout is not None and not done_sentinels:
                # this means the timeout ended,
                # time to check all of the task outputs timers
                last_updated_time = max(
                    float(last_updated_time.value)
                    for last_updated_time in last_updated_times
                )
                if time.time() - last_updated_time > inactivity_timeout:
                    raise InactivityError(
                        f"Last updated time was {time.time() - last_updated_time}s ago, pipeline inactivity timeout is {inactivity_timeout}s."
                    )

            if done_sentinels:
                done_id = done_sentinels[0]
                assert isinstance(
                    done_id, int
                ), f"mp_connection.wait returned unexpected type: {done_id}"
                # for some reason needs a join, or the exitcode doesn't sync properly
                # but it has already exited, so this should finish very quickly
                subprocess.join()
                if subprocess.exitcode is None:
                    # unsure what could cause this, but we see it in production sometimes
                    # when an instance is shutting down
                    logger.warning("Child process joined with exitcode None.")
                elif subprocess.exitcode != 0:
                    # attempts to catch segfaults and other errors that cannot be caught by python (i.g. sigkill)
                    raise TaskError(
                        f"Process: {subprocess.name} exited with non-zero code {subprocess.exitcode}"
                    )

            try:
                # first entry on the error queue should hopefully be the original error, just raise that one single error
                (task_name, task_err, traceback_str), _ = err_queue.get()
                # should only be at most one unique error, just raise it
                # the main error needs to be the main raise for type-based exception catching to work
                raise task_err from TaskError(
                    f"Task; {task_name} errored\n{traceback_str}\n{task_err}"
                )
            except queue.Empty:
                # if the error queue is empty, then there is no error
                pass

        except BaseException as err:  # pylint: disable=broad-except
            # joins process as cleanup if they successfully exited
            # give them a decent amount of time to process their current task and exit cleanly
            subprocess.join(timeout=15.0)
            # escalate, send sigterm to process
            subprocess.terminate()
            # wait for terminate signal to propagate through the process
            subprocess.join(timeout=5.0)
            # force kill the process (only if they are refusing to terminate cleanly)
            subprocess.kill()
            subprocess.join()

            raise err
