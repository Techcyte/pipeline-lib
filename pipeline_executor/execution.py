import multiprocessing as mp
import queue as q
import threading as tr
import traceback
from ast import Dict
from concurrent import futures
from threading import Lock
from typing import Any, Iterable, List, Tuple

from .pipeline_task import PipelineTask
from .type_checking import type_check_tasks

QUEUE_POLL_TIMEOUT = 0.05


class TaskOutput:
    def __init__(self, num_upstream_tasks: int) -> None:
        self.queue = q.Queue()
        self.lock = Lock()
        self.remaining_upstream_tasks = num_upstream_tasks
        self.error_info = None

    def iter_results(self) -> Iterable[Any]:
        while True:
            try:
                item = self.queue.get(block=True, timeout=QUEUE_POLL_TIMEOUT)
                yield item
            except q.Empty:
                with self.lock:
                    if (
                        self.error_info is not None
                        or self.remaining_upstream_tasks == 0
                    ):
                        # upstream won't be sending any more information, stop iterating
                        return

    def put_results(self, iterable: Iterable[Any]):
        for item in iterable:
            try:
                self.queue.put(item, block=True, timeout=QUEUE_POLL_TIMEOUT)
            except q.Full:
                with self.lock:
                    if self.error_info is not None:
                        # downstream won't be receiving any more information, stop trying to put stuff on queue
                        return

    def task_done(self):
        with self.lock:
            self.remaining_upstream_tasks -= 1

    def set_error(self, task_name, err, traceback):
        with self.lock:
            self.error_info = (task_name, err, traceback)


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
        downstream.task_done()
    except Exception as err:
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
        downstream.task_done()
    except Exception as err:
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
    except Exception as err:
        tb_str = traceback.format_exc()
        upstream.set_error(task.name, err, tb_str)


def execute(tasks: List[PipelineTask]):
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
        data_streams = [TaskOutput(t.num_threads) for t in tasks[:-1]]
        # only one source thread per program
        threads: List[tr.Thread] = [
            tr.Thread(target=_start_source, args=(source_task, data_streams[0]))
        ]
        for i, worker_task in enumerate(worker_tasks):
            for _ in range(worker_task.num_threads):
                threads.append(
                    tr.Thread(
                        target=_start_worker,
                        args=(worker_task, data_streams[i], data_streams[i + 1]),
                    )
                )

        for _ in range(sink_task.num_threads):
            threads.append(
                tr.Thread(target=_start_sink, args=(sink_task, data_streams[-1]))
            )

        for t in threads:
            # set everything to be daemon threads so that a keyboard interrupt
            # or sigterm kills the whole process
            t.setDaemon(True)
            t.start()

        for t in threads:
            t.join()

        for stream in data_streams:
            if stream.error_info is not None:
                # should only be at most one unique error, just raise it
                task_name, err, traceback_str = stream.error_info
                print(f"Task; {task_name} errored\n{traceback_str}\n{err}")
                raise err
