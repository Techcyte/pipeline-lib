import ctypes
import traceback
from typing import Any, Iterable, List
import multiprocessing as mp
import multiprocessing.connection
import pickle

from .pipeline_task import PipelineTask
from .type_checking import MAX_NUM_WORKERS, type_check_tasks


class TaskError(RuntimeError):
    pass


class PropogateErr(RuntimeError):
    pass


class BufferedQueue:
    def __init__(self, buf_size: int, max_num_elements: int) -> None:
        self.max_num_elements = max_num_elements + 2
        self.buf_size = buf_size
        self._raw_data = [mp.RawArray(ctypes.c_byte, buf_size) for _ in range(self.max_num_elements)]
        self.buf_sizes = mp.RawArray(ctypes.c_int, self.max_num_elements)
        self.first_item_pos = mp.Value('i',0,lock=False)
        self.last_item_pos = mp.Value('i',0,lock=False)
        self.lock = mp.Lock()

    def put(self, item: Any):
        item_bytes = pickle.dumps(item)
        if len(item_bytes) > self.buf_size:
            raise ValueError(f"Tried to pass item of size {len(item_bytes)} but max buffer isze is {self.buf_size}")
        with self.lock:
            write_pos = int(self.first_item_pos.value)
            new_pos = (write_pos + 1) % self.max_num_elements
            self.first_item_pos.value = new_pos

            self.buf_sizes[write_pos] = len(item_bytes)
            self._raw_data[write_pos][:len(item_bytes)] = item_bytes

    def get(self):
        with self.lock:
            read_pos = int(self.last_item_pos.value)
            new_pos = (read_pos + 1) % self.max_num_elements
            self.last_item_pos.value = new_pos

            num_bytes = self.buf_sizes[read_pos]
            # perform a full copy of the data so that unpickling can be done outside the lock
            data_bytes = bytes(memoryview(self._raw_data[read_pos])[:num_bytes])
        return pickle.loads(data_bytes)

    def __len__(self):
        with self.lock:
            return (self.last_item_pos.value - self.first_item_pos.value + self.max_num_elements) % self.max_num_elements



class TaskOutput:
    def __init__(self, num_upstream_tasks: int, packets_in_flight: int, has_error: mp.Event, error_info: mp.Queue) -> None:
        self.num_tasks_remaining = mp.Value('i', num_upstream_tasks, lock=True)
        self.queue_len = mp.Semaphore(value=0)
        self.packets_space = mp.Semaphore(value=packets_in_flight)
        DEFAULT_BUF_SIZE = 100000
        # using a custom queue implementation rather than multiprocessing.queue
        # because mp.Queue has strange synchronization properties with the semaphores, leading to many bugs
        self.queue = BufferedQueue(DEFAULT_BUF_SIZE, packets_in_flight)
        self.has_error = has_error
        self.error_info = error_info

    def iter_results(self) -> Iterable[Any]:
        while True:
            self.queue_len.acquire()  # pylint: disable=consider-using-with
            if self.has_error.is_set():
                raise PropogateErr()
            
            # only happens when out of results
            if len(self.queue) == 0:
                break
            item = self.queue.get()
            yield item

            # this release needs to happen after the yield
            # completes to support full synchronization semantics with packets_in_flight=1
            self.packets_space.release()

    def put_results(self, iterable: Iterable[Any]):
        iterator = iter(iterable)
        try:
            while True:
                # wait for space to be avaliable on queue before iterating to next item
                # essential for full synchronization semantics with packets_in_flight=1
                self.packets_space.acquire()  # pylint: disable=consider-using-with

                if self.has_error.is_set():
                    raise PropogateErr()

                item = next(iterator)

                self.queue.put(item)
                self.queue_len.release()
        except StopIteration:
            # normal end of iteration
            with self.num_tasks_remaining.get_lock():
                self.num_tasks_remaining.value -= 1
                if self.num_tasks_remaining.value == 0:
                    for _i in range(MAX_NUM_WORKERS):
                        self.queue_len.release()

    def set_error(self, task_name, err, traceback_str):
        if not self.has_error.is_set():
            self.error_info.put((task_name, err, traceback_str))
        # release all consumers and producers semaphores so that they exit quickly
        for _i in range(MAX_NUM_WORKERS):
            self.queue_len.release()
            self.packets_space.release()


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
    except BaseException as err:  # pylint: disable=broad-except
        tb_str = traceback.format_exc()
        downstream.set_error(task.name, err, tb_str)
        raise err


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

    except BaseException as err:  # pylint: disable=broad-except
        tb_str = traceback.format_exc()
        # sets upstream and downstream so that error propogates throughout the system
        downstream.set_error(task.name, err, tb_str)
        upstream.set_error(task.name, err, tb_str)
        raise err


def _start_sink(
    task: PipelineTask,
    upstream: TaskOutput,
):
    try:
        constants = {} if task.constants is None else task.constants
        generator_input = upstream.iter_results()
        task.generator(generator_input, **constants)
    except BaseException as err:  # pylint: disable=broad-except
        tb_str = traceback.format_exc()
        upstream.set_error(task.name, err, tb_str)
        raise err


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

        ERR_BUF_SIZE = 2**17
        n_total_tasks = sum(task.num_workers for task in tasks)
        err_queue = BufferedQueue(ERR_BUF_SIZE, n_total_tasks + 2)
        err_event = mp.Event()
        # number of processes are of the producing task
        data_streams = [
            TaskOutput(t.num_workers, t.packets_in_flight, err_event, err_queue) for t in tasks[:-1]
        ]
        # only one source thread per program
        processes: List[mp.Process] = [
            mp.Process(target=_start_source, args=(source_task, data_streams[0]), name=f"{source_task}_{worker_idx}")
            for worker_idx in range(source_task.num_workers)
        ]
        for i, worker_task in enumerate(worker_tasks):
            for worker_idx in range(worker_task.num_workers):
                processes.append(
                    mp.Process(
                        target=_start_worker,
                        args=(worker_task, data_streams[i], data_streams[i + 1]),
                        name=f"{worker_task}_{worker_idx}"
                    )
                )

        for worker_idx in range(sink_task.num_workers):
            processes.append(
                mp.Process(target=_start_sink, args=(sink_task, data_streams[-1]), name=f"{sink_task}_{worker_idx}")
            )

        for process in processes:
            process.start()

        try:
            sentinel_map = {proc.sentinel:proc for proc in processes}
            sentinel_set = {proc.sentinel for proc in processes}
            while sentinel_set and not err_event.is_set():
                done_sentinels = mp.connection.wait(list(sentinel_set))
                sentinel_set -= set(done_sentinels)
                for d in done_sentinels:
                    # attempts to catch segfaults and other errors that cannot be caught by python (i.g. sigkill)
                    if sentinel_map[d].exitcode != 0:
                        proc_err_msg = f"Process: {sentinel_map[d].name} exited with non-zero code {sentinel_map[d].exitcode}"
                        err_event.set()
                        err_queue.put((sentinel_map[d].name, TaskError(proc_err_msg), ""))

        except BaseException as err:
            tb_str = traceback.format_exc()
            err_event.set()
            err_queue.put(("_main_thread", err, tb_str))

        finally:
            # terminate remaining processes if they still exist (which they shouldn't, under normal execution)
            for proc in processes:
                proc.terminate()
            # joins processes as cleanup if they successfully exited
            for proc in processes:
                proc.join(timeout=0.5)
            # force kill the processes if they failed to terminate cleanly
            for proc in processes:
                proc.kill()
                proc.join()

            if err_event.is_set():
                # first entry on the error queue should hopefully be the original error, just raise that one single error
                task_name, err, traceback_str = err_queue.get()
                # should only be at most one unique error, just raise it
                raise TaskError(
                    f"Task; {task_name} errored\n{traceback_str}\n{err}"
                ) from err
