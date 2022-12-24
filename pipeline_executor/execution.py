import threading as tr
import traceback
from collections import deque
from threading import Lock, Semaphore
from typing import Any, Iterable, List

from .pipeline_task import PipelineTask
from .type_checking import MAX_NUM_WORKERS, type_check_tasks


class TaskError(RuntimeError):
    pass


class PropogateErr(RuntimeError):
    pass


class TaskOutput:
    def __init__(self, num_upstream_tasks: int, packets_in_flight: int) -> None:
        self.num_tasks_remaining = num_upstream_tasks
        self.queue_len = Semaphore(value=0)
        self.packets_space = Semaphore(value=packets_in_flight)
        self.queue: deque = deque(maxlen=packets_in_flight)
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
            self.packets_space.release(1)

    def put_results(self, iterable: Iterable[Any]):
        iterator = iter(iterable)
        try:
            while True:
                # wait for space to be avaliable on queue before iterating to next item
                # essential for full synchronization semantics with packets_in_flight=1
                self.packets_space.acquire()  # pylint: disable=consider-using-with

                if self.is_errored():
                    raise PropogateErr()

                item = next(iterator)

                self.queue.append(item)
                self.queue_len.release(1)
        except StopIteration:
            # normal end of iteration
            with self.lock:
                self.num_tasks_remaining -= 1
                if self.num_tasks_remaining == 0:
                    self.queue_len.release(MAX_NUM_WORKERS)

    def is_errored(self):
        return self.error_info is not None

    def set_error(self, task_name, err, traceback_str):
        with self.lock:
            self.error_info = (task_name, err, traceback_str)
        # release all consumers and producers semaphores so that they exit quickly
        self.packets_space.release(MAX_NUM_WORKERS)
        self.queue_len.release(MAX_NUM_WORKERS)


def _start_singleton(
    task: PipelineTask,
):
    constants = {} if task.constants is None else task.constants
    task.generator(**constants)


def _start_source(
    task: PipelineTask,
    downstream: TaskOutput,
):
    try:
        constants = {} if task.constants is None else task.constants
        out_iter = task.generator(**constants)
        downstream.put_results(out_iter)
    except Exception as err:  # pylint: disable=broad-except
        tb_str = traceback.format_exc()
        downstream.set_error(task.name, err, tb_str)


def _start_worker(
    task: PipelineTask,
    upstream: TaskOutput,
    downstream: TaskOutput,
):
    try:
        constants = {} if task.constants is None else task.constants
        generator_input = upstream.iter_results()
        out_iter = task.generator(generator_input, **constants)
        downstream.put_results(out_iter)
    except Exception as err:  # pylint: disable=broad-except
        tb_str = traceback.format_exc()
        # sets upstream and downstream so that error propogates throughout the system
        downstream.set_error(task.name, err, tb_str)
        upstream.set_error(task.name, err, tb_str)


def _start_sink(
    task: PipelineTask,
    upstream: TaskOutput,
):
    try:
        constants = {} if task.constants is None else task.constants
        generator_input = upstream.iter_results()
        task.generator(generator_input, **constants)
    except Exception as err:  # pylint: disable=broad-except
        tb_str = traceback.format_exc()
        upstream.set_error(task.name, err, tb_str)


def execute(tasks: List[PipelineTask]):
    # pylint: disable=too-many-branches
    """
    execute tasks until final task completes.
    Raises error if tasks are inconsistently specified or if
    one of the tasks raises an error.
    """
    if not tasks:
        return

    type_check_tasks(tasks)

    if len(tasks) == 1:
        _start_singleton(tasks[0])

    else:
        source_task = tasks[0]
        sink_task = tasks[-1]
        worker_tasks = tasks[1:-1]

        # number of processes are of the producing task
        data_streams = [
            TaskOutput(t.num_workers, t.packets_in_flight) for t in tasks[:-1]
        ]
        # only one source thread per program
        threads: List[tr.Thread] = [
            tr.Thread(target=_start_source, args=(source_task, data_streams[0]))
        ]
        for i, worker_task in enumerate(worker_tasks):
            for _ in range(worker_task.num_workers):
                threads.append(
                    tr.Thread(
                        target=_start_worker,
                        args=(worker_task, data_streams[i], data_streams[i + 1]),
                    )
                )

        for _ in range(sink_task.num_workers):
            threads.append(
                tr.Thread(target=_start_sink, args=(sink_task, data_streams[-1]))
            )

        for thread in threads:
            thread.start()

        try:
            for thread in threads:
                thread.join()

        except BaseException as err:
            # asks all threads to terminate as quickly as possible
            tb_str = traceback.format_exc()
            for stream in data_streams:
                stream.set_error("main_task", err, tb_str)
            # clean up remaining threads so that main process terminates properly
            for thread in threads:
                thread.join()
            raise err

        for stream in data_streams:
            if stream.error_info is not None and not isinstance(
                stream.error_info[1], PropogateErr
            ):
                # should only be at most one unique error, just raise it
                task_name, err, traceback_str = stream.error_info
                raise TaskError(
                    f"Task; {task_name} errored\n{traceback_str}\n{err}"
                ) from err
