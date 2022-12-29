import ctypes
import multiprocessing as mp
import multiprocessing.connection as mp_connection
import pickle
import traceback
from dataclasses import dataclass
from multiprocessing import synchronize
from typing import Any, Iterable, List

import numpy as np

from .pipeline_task import PipelineTask
from .type_checking import MAX_NUM_WORKERS, type_check_tasks

ERR_BUF_SIZE = 2**17
# some arbitrary, hopefully unused number that signals python exiting after placing the error in the queue
PYTHON_ERR_EXIT_CODE = 187
# copies are much faster if they are aligned to 16 byte or 32 byte boundaries (depending on archtecture)
ALIGN_SIZE = 32


class TaskError(RuntimeError):
    pass


class PropogateErr(RuntimeError):
    # should never be raised in main scope, meant to act as a proxy
    # for propogating errors up and down the pipeline
    pass


@dataclass
class Value:
    value: Any


def roundup_to_align(size):
    return size + (-size) % ALIGN_SIZE


tot_time = 0


class BufferedQueue:
    """
    A custom queue implementation based on fixed-size shared memory buffers to transfer data.

    Advantages over standard library multiprocessing.Queue:

    1. Objects, once placed on the queue can be fetched immidiately without delay (mp.Queue `put` function returns immidiately after spawning the "sender" thread, so there can be a delay before the message is readable from the queue)
    2. Even if producer process dies, the message is still avaliable to consume (only possible in ordinary queue if the message is placed fully in the mp.Pipe hardcoded 64kb buffer)
    3. No extra thread (on producer) is needed to send data to consumer
    4. Large volume communication between processes occurs fully asynchronously (vs back and forth queue message passing)
    5. Large buffer copys are aligned to 32 byte boundaries, making copies quite fast (benchmarked at 4gb/s throughput, about 20% of max, almost 100x faster than queue)

    Disadvantage:

    1. Doesn't work well if messages can be arbitrarily large
    2. Requires significant number of file handles

    Note that the complexities around using picke protocol version 5 buffer_callback
    overrides are significant, but according to the benchmark in `run_benchmark.py`, it
    is 100x faster for large buffers. For apples to apples comparison,

    """

    def __init__(self, buf_size: int, max_num_elements: int) -> None:
        self.max_num_elements = max_num_elements
        self.orig_buf_size = buf_size
        # round buffer size up to align size so that every packets buffer starts as aligned
        self.buf_size = roundup_to_align(buf_size)
        self._pickle_data = mp.RawArray(ctypes.c_byte, buf_size * self.max_num_elements)
        self._out_of_band_data = mp.RawArray(
            ctypes.c_byte, buf_size * self.max_num_elements
        )
        self.buf_sizes = mp.RawArray(ctypes.c_int, self.max_num_elements)
        self.first_item_pos = mp.Value("i", 0, lock=False)
        self.last_item_pos = mp.Value("i", 0, lock=False)
        self.lock = mp.Lock()

    def put(self, item: Any):
        with self.lock:
            write_pos = int(self.first_item_pos.value)
            new_pos = (write_pos + 1) % self.max_num_elements
            self.first_item_pos.value = new_pos

            block_start = write_pos * self.buf_size

            item_bytes = pickle.dumps(
                item,
                protocol=pickle.HIGHEST_PROTOCOL,
                buffer_callback=self.make_format_callback(write_pos),
            )
            if len(item_bytes) > self.buf_size:
                raise ValueError(
                    f"Tried to pass item which picked to size {len(item_bytes)}, but PipelineTask.max_message_size is {self.orig_buf_size}"
                )

            pickled_view = np.frombuffer(item_bytes, dtype="uint8")
            mem_view = np.frombuffer(self._pickle_data, dtype="uint8")
            mem_view[block_start : block_start + len(item_bytes)] = pickled_view
            self.buf_sizes[write_pos] = len(item_bytes)

    def make_format_callback(self, write_pos):
        out_of_band_view = np.frombuffer(self._out_of_band_data, dtype="uint8")
        out_of_band_size_view = np.frombuffer(self._out_of_band_data, dtype="int32")

        start_pos = write_pos * self.buf_size
        # zero out starting marker
        out_of_band_size_view[start_pos // 4] = 0

        cur_block_pos = Value(start_pos)

        def format_data(buf_obj):
            src_obj = np.frombuffer(buf_obj, dtype=np.uint8)
            # print(len(src_len))
            src_len = len(src_obj)
            cur_pos = cur_block_pos.value
            next_pos = cur_pos + roundup_to_align(ALIGN_SIZE + src_len)
            if next_pos - start_pos > self.buf_size:
                raise ValueError(
                    f"Serialized numpy data coming out to size at least {next_pos - start_pos} in size, but PipelineTask.max_message_size is {self.orig_buf_size}"
                )
            elif next_pos - start_pos < self.buf_size:
                # mark current end of buffer by zeroing out size information
                out_of_band_size_view[next_pos // 4] = 0

            # set integer size of next chunk
            out_of_band_size_view[cur_pos // 4] = src_len

            # set chunk data
            out_of_band_view[
                ALIGN_SIZE + cur_pos : ALIGN_SIZE + cur_pos + src_len
            ] = src_obj

            cur_block_pos.value = next_pos

        return format_data

    def iter_chunks(self, read_pos):
        out_of_band_view = np.frombuffer(self._out_of_band_data, dtype="uint8")
        out_of_band_size_view = np.frombuffer(self._out_of_band_data, dtype="int32")
        cur_pos = read_pos * self.buf_size
        end_pos = (read_pos + 1) * self.buf_size
        while cur_pos < end_pos:
            chunk_size = int(out_of_band_size_view[cur_pos // 4])
            if chunk_size == 0:
                break
            # a copy is required because the shared memory will be mutating after this returns
            # TODO: check if this is actually required given the "packets in flight concept"
            out_buffer = out_of_band_view[
                ALIGN_SIZE + cur_pos : ALIGN_SIZE + cur_pos + chunk_size
            ].copy()
            yield out_buffer
            cur_pos += roundup_to_align(ALIGN_SIZE + chunk_size)

    def get(self):
        # print("getting")
        with self.lock:
            read_pos = int(self.last_item_pos.value)
            new_pos = (read_pos + 1) % self.max_num_elements
            self.last_item_pos.value = new_pos

            num_bytes = self.buf_sizes[read_pos]
            read_block = read_pos * self.buf_size
            mem_view = np.frombuffer(self._pickle_data, dtype="uint8")
            # perform a full copy of the data so that unpickling can be done outside the lock
            data_bytes = mem_view[read_block : read_block + num_bytes].tobytes()

            loaded_data = pickle.loads(data_bytes, buffers=self.iter_chunks(read_pos))
            return loaded_data

    def __len__(self):
        with self.lock:
            return (
                self.last_item_pos.value
                - self.first_item_pos.value
                + self.max_num_elements
            ) % self.max_num_elements


class TaskOutput:
    def __init__(
        self,
        num_upstream_tasks: int,
        packets_in_flight: int,
        error_info: BufferedQueue,
        max_message_size: int,
    ) -> None:
        self.num_tasks_remaining = mp.Value("i", num_upstream_tasks, lock=True)
        self.queue_len = mp.Semaphore(value=0)
        self.packets_space = mp.Semaphore(value=packets_in_flight)
        # using a custom queue implementation rather than multiprocessing.queue
        # because mp.Queue has strange synchronization properties with the semaphores, leading to many bugs
        self.queue = BufferedQueue(max_message_size, packets_in_flight + 1)
        self.has_error = mp.Event()
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
            self.has_error.set()
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
    except Exception as err:  # pylint: disable=broad-except
        tb_str = traceback.format_exc()
        downstream.set_error(task.name, err, tb_str)
        # exiting directly instead of re-raising error, as that would clutter stderr
        # with duplicate tracebacks
        exit(PYTHON_ERR_EXIT_CODE)


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
        # exiting directly instead of re-raising error, as that would clutter stderr
        # with duplicate tracebacks
        exit(PYTHON_ERR_EXIT_CODE)


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
        # exiting directly instead of re-raising error, as that would clutter stderr
        # with duplicate tracebacks
        exit(PYTHON_ERR_EXIT_CODE)


def execute(tasks: List[PipelineTask]):
    # pylint: disable=too-many-branches,too-many-locals
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
        return

    source_task = tasks[0]
    sink_task = tasks[-1]
    worker_tasks = tasks[1:-1]

    n_total_tasks = sum(task.num_workers for task in tasks)
    err_queue = BufferedQueue(ERR_BUF_SIZE, n_total_tasks + 2)
    # number of processes are of the producing task
    data_streams = [
        TaskOutput(
            t.num_workers,
            t.packets_in_flight,
            err_queue,
            max_message_size=t.max_message_size,
        )
        for t in tasks[:-1]
    ]
    processes: List[mp.Process] = [
        mp.Process(
            target=_start_source,
            args=(source_task, data_streams[0]),
            name=f"{source_task}_{worker_idx}",
        )
        for worker_idx in range(source_task.num_workers)
    ]
    for i, worker_task in enumerate(worker_tasks):
        for worker_idx in range(worker_task.num_workers):
            processes.append(
                mp.Process(
                    target=_start_worker,
                    args=(worker_task, data_streams[i], data_streams[i + 1]),
                    name=f"{worker_task}_{worker_idx}",
                )
            )

    for worker_idx in range(sink_task.num_workers):
        processes.append(
            mp.Process(
                target=_start_sink,
                args=(sink_task, data_streams[-1]),
                name=f"{sink_task}_{worker_idx}",
            )
        )

    for process in processes:
        process.start()

    has_error = False
    try:
        sentinel_map = {proc.sentinel: proc for proc in processes}
        sentinel_set = {proc.sentinel for proc in processes}
        while sentinel_set and not has_error:
            done_sentinels = mp_connection.wait(list(sentinel_set))
            sentinel_set -= set(done_sentinels)
            for done_id in done_sentinels:
                # attempts to catch segfaults and other errors that cannot be caught by python (i.g. sigkill)
                if sentinel_map[done_id].exitcode != 0:
                    proc_err_msg = f"Process: {sentinel_map[done_id].name} exited with non-zero code {sentinel_map[done_id].exitcode}"
                    for stream in data_streams:
                        stream.set_error(
                            sentinel_map[done_id].name, TaskError(proc_err_msg), ""
                        )
                    has_error = True
                    break

    except BaseException as err:  # pylint: disable=broad-except
        tb_str = traceback.format_exc()
        has_error = True
        for stream in data_streams:
            stream.set_error("_main_thread", err, tb_str)

    finally:
        # joins processes as cleanup if they successfully exited
        # give them a decent amount of time to process their current task and exit cleanly
        for proc in processes:
            proc.join(timeout=15.0)
        # escalate, send sigterm to processes
        for proc in processes:
            proc.terminate()
        # wait for terminate signal to propogate through the processes
        for proc in processes:
            proc.join(timeout=5.0)
        # force kill the processes (only if they are refusing to terminate cleanly)
        for proc in processes:
            proc.kill()
            proc.join()

        if has_error:
            # first entry on the error queue should hopefully be the original error, just raise that one single error
            task_name, task_err, traceback_str = err_queue.get()
            # should only be at most one unique error, just raise it
            raise TaskError(
                f"Task; {task_name} errored\n{traceback_str}\n{task_err}"
            ) from task_err
