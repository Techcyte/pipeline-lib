import logging
import threading as tr
import time
import traceback
import warnings
from collections import deque
from threading import Lock, Semaphore, get_native_id
from typing import Any, Iterable, List, Optional, Set

from .pipeline_task import DEFAULT_BUF_SIZE, InactivityError, PipelineTask, TaskError
from .type_checking import MAX_NUM_WORKERS, sanity_check_mp_params

logger = logging.getLogger(__name__)


class PropogateErr(RuntimeError):
    pass


class TaskOutput:
    def __init__(self, num_upstream_tasks: int, packets_in_flight: int) -> None:
        self.num_tasks_remaining = num_upstream_tasks
        self.queue_len = Semaphore(value=0)
        self.packets_space = Semaphore(value=packets_in_flight)
        self.queue: deque = deque(maxlen=packets_in_flight)
        self.last_updated_time = time.monotonic()
        self.lock = Lock()
        self.error_info = None

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
            self.last_updated_time = time.monotonic()

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
        # release all consumers and producers semaphores so that they exit quickly
        for _i in range(MAX_NUM_WORKERS):
            self.queue_len.release()
            self.packets_space.release()


def _start_worker(
    task: PipelineTask,
    upstream: TaskOutput,
    downstream: TaskOutput,
    clean_completed: Set[int],
):
    try:
        constants = {} if task.constants is None else task.constants
        generator_input = upstream.iter_results()
        out_iter = task.generator(generator_input, **constants)
        downstream.put_results(out_iter)
        clean_completed.add(get_native_id())
    except BaseException as err:  # pylint: disable=broad-except
        tb_str = traceback.format_exc()
        # sets upstream and downstream so that error propagates throughout the system
        downstream.set_error(task.name, err, tb_str)
        upstream.set_error(task.name, err, tb_str)


def _start_source(
    task: PipelineTask,
    downstream: TaskOutput,
    clean_completed: Set[int],
):
    try:
        out_iter = task.generator(**task.constants_dict)
        downstream.put_results(out_iter)
        clean_completed.add(get_native_id())
    except BaseException as err:  # pylint: disable=broad-except
        tb_str = traceback.format_exc()
        downstream.set_error(task.name, err, tb_str)


def _start_sink(
    task: PipelineTask,
    upstream: TaskOutput,
    clean_completed: Set[int],
):
    try:
        generator_input = upstream.iter_results()
        task.generator(generator_input, **task.constants_dict)
        clean_completed.add(get_native_id())
    except BaseException as err:  # pylint: disable=broad-except
        tb_str = traceback.format_exc()
        upstream.set_error(task.name, err, tb_str)


def _warn_parameter_overrides(tasks: List[PipelineTask]):
    for task in tasks:
        if task.max_message_size is None or task.max_message_size != DEFAULT_BUF_SIZE:
            warnings.warn(
                f"Task '{task.name}' overrode default value of max_message_size, and this override is ignored by 'thread' parallelism strategy."
            )


def execute_tr(tasks: List[PipelineTask], inactivity_timeout: Optional[float]):
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

    source_task = tasks[0]
    sink_task = tasks[-1]
    worker_tasks = tasks[1:-1]
    clean_completed: Set[int] = set()

    # number of processes are of the producing task
    data_streams = [TaskOutput(t.num_workers, t.packets_in_flight) for t in tasks[:-1]]
    # only one source thread per program
    threads: List[tuple[str, tr.Thread]] = [
        (
            source_task.name,
            tr.Thread(
                target=_start_source,
                args=(source_task, data_streams[0], clean_completed),
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
                            clean_completed,
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
                    args=(sink_task, data_streams[-1], clean_completed),
                ),
            )
        )

    for name, thread in threads:
        thread.start()

    has_error = False
    thread_id_to_name = {thread.native_id: name for name, thread in threads}

    sentinel_set = {proc.native_id for _name, proc in threads}
    try:
        while sentinel_set:
            for stream in data_streams:
                # no locking needed to read this value
                # because the other threads are only writing
                last_updated_time = max(
                    float(stream.last_updated_time) for stream in data_streams
                )
                if (
                    inactivity_timeout is not None
                    and time.monotonic() - last_updated_time > inactivity_timeout
                ):
                    raise InactivityError(
                        f"Last updated time was {time.monotonic() - last_updated_time}s ago, pipeline inactivity timeout is {inactivity_timeout}s."
                    )

            done_sentinels = set()
            is_first_thread = True
            for name, thread in threads:
                if thread.native_id not in sentinel_set:
                    continue
                if is_first_thread:
                    timeout = (
                        None if inactivity_timeout is None else inactivity_timeout / 10
                    )
                    is_first_thread = False
                else:
                    timeout = 0
                thread.join(timeout=timeout)
                if not thread.is_alive():
                    done_sentinels.add(thread.native_id)
                    sentinel_set.remove(thread.native_id)

            task_name, err, traceback_str = ("", "", "")
            for done_id in done_sentinels:
                if done_id not in clean_completed:
                    # attempts to catch various errors that aren't caught by python (i.g. sigkill)
                    proc_err_msg = f"Thead: {done_id} exited improperly"
                    task_name = thread_id_to_name[done_id]
                    for stream in data_streams:
                        stream.set_error(
                            thread_id_to_name[done_id], TaskError(proc_err_msg), ""
                        )
                    has_error = True
                    break

            for stream in data_streams:
                if stream.error_info is not None and not isinstance(
                    stream.error_info[1], PropogateErr
                ):
                    # should only be at most one unique error, just raise it
                    task_name, err, traceback_str = stream.error_info
                    raise err

            if has_error:
                # got an error but don't know much if anything about it
                # should only be at most one unique error, just raise it
                for stream in data_streams:
                    stream.set_error(task_name, err, traceback_str)
                raise TaskError(f"Task; {task_name} errored\n{traceback_str}\n{err}")

    except BaseException as err:
        # asks all threads to terminate as quickly as possible
        tb_str = traceback.format_exc()
        for stream in data_streams:
            stream.set_error("main_task", err, tb_str)
        # clean up remaining threads so that main process terminates properly
        for _name, thread in threads:
            thread.join(15)
        raise err
