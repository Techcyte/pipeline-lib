import contextlib
import ctypes
import logging
import multiprocessing as mp
import multiprocessing.connection as mp_connection
import os
import queue
import select
import signal
import sys
import threading as tr
import time
import traceback
import typing
from dataclasses import dataclass
from functools import reduce
from multiprocessing import synchronize
from multiprocessing.context import (
    BaseContext,
    ForkContext,
    ForkProcess,
    SpawnContext,
    SpawnProcess,
)
from operator import mul
from typing import Any, Iterable, List, Literal, Optional, Set, Tuple, Union

import cloudpickle

from .pipeline_task import InactivityError, PipelineTask, TaskError
from .type_checking import MAX_NUM_WORKERS, sanity_check_mp_params

logger = logging.getLogger(__name__)

ERR_BUF_SIZE = 2**16
# some arbitrary, hopefully unused number that signals python exiting after placing the error in the queue
PYTHON_ERR_EXIT_CODE = 187
# copies are much faster if they are aligned to 16 byte or 32 byte boundaries (depending on architecture)
ALIGN_SIZE = 32

SpawnContextName = Literal["spawn", "fork", "forkserver"]


class PropogateErr(RuntimeError):
    # should never be raised in main scope, meant to act as a proxy
    # for propogating errors up and down the pipeline
    pass


class SignalReceived(Exception):
    def __init__(self, signum: int) -> None:
        self.signum = signum


@dataclass
class Value:
    value: Any


def roundup_to_align(size):
    return size + (-size) % ALIGN_SIZE


class CyclicAllocator:
    # pylint: disable=too-many-instance-attributes
    def __init__(self, max_num_elements: int, ctx: BaseContext) -> None:
        self.max_num_elements = max_num_elements
        self.producer_idxs = ctx.RawArray(ctypes.c_int, self.max_num_elements)
        self.producer_next = ctx.Value(ctypes.c_int, 0, lock=False)
        self.producer_last = ctx.Value(ctypes.c_int, 0, lock=False)
        # initialize to putting everything available
        for i in range(self.max_num_elements):
            self.producer_idxs[i] = i

        self.consumer_idxs = ctx.RawArray(ctypes.c_int, self.max_num_elements)
        self.consumer_next = ctx.Value(ctypes.c_int, 0, lock=False)
        self.consumer_last = ctx.Value(ctypes.c_int, 0, lock=False)

        self.position_lock = ctx.Lock()

    def pop_write_pos(self):
        with self.position_lock:
            write_entry = int(self.producer_next.value)
            write_pos = int(self.producer_idxs[write_entry])
            self.producer_next.value = (write_entry + 1) % self.max_num_elements
        return write_pos

    def push_read_pos(self, written_pos: int):
        with self.position_lock:
            write_entry = int(self.consumer_last.value)
            self.consumer_idxs[write_entry] = written_pos
            self.consumer_last.value = (write_entry + 1) % self.max_num_elements  # type: ignore

    def pop_read_pos(self):
        with self.position_lock:
            read_entry = int(self.consumer_next.value)
            last_read_entry = int(self.consumer_last.value)
            # print(read_entry, last_read_entry)
            if read_entry == last_read_entry:
                # no entry available for reading, cannot continue
                raise queue.Empty()
            read_pos = self.consumer_idxs[read_entry]
            self.consumer_next.value = (read_entry + 1) % self.max_num_elements
        return read_pos

    def push_write_pos(self, read_pos):
        with self.position_lock:
            read_entry = int(self.producer_last.value)
            self.producer_idxs[read_entry] = read_pos
            self.producer_last.value = (read_entry + 1) % self.max_num_elements


class AsyncQueue:
    def get(self) -> Tuple[Any, int]:
        """
        returns a tuple of (queued_item, packet_id)

        Need to call free(packet_id) after finished using the object.

        If no objects to get, raises queue.Empty error
        """
        raise NotImplementedError()

    def put(self, _item: Any):
        """
        Puts item on queue. Undefined behavior if there are more items put
        on the queue than the space available.

        TODO: raise an error
        """
        raise NotImplementedError()

    def free(self, _packet_id: int):
        """
        Free up fetched packet for writing
        """
        raise NotImplementedError()

    def flush(self, has_error: synchronize.Event):
        """
        Flush all pending writes (will block unless has_error is set)
        """


END_OF_BUFFER_ID = -2


class BufferedQueue(AsyncQueue):
    """
    A custom queue implementation based on fixed-size shared memory buffers to transfer data.

    Advantages over standard library multiprocessing.Queue:

    1. Objects, once placed on the queue can be fetched immediately without delay (mp.Queue `put` function returns immediately after spawning the "sender" thread, so there can be a delay before the message is readable from the queue)
    2. Even if producer process dies, the message is still available to consume (only possible in ordinary queue if the message is placed fully in the mp.Pipe hardcoded 64kb buffer)
    3. No extra thread (on producer) is needed to send data to consumer
    4. Buffered communication between processes occurs fully asynchronously (vs back and forth queue message passing between spawned producer thread and consumer)
    5. One queue buffered read can process concurrently with one write

    Disadvantage:

    1. Doesn't work well if messages can be arbitrarily large
    2. Requires significant number of file handles

    Additionally, this library makes use of the pickle protocol v5's
    buffer interface in `make_buffer_callback` and `iter_stored_buffers` methods.
    This is to support fast copies of numpy arrays, and other buffers.
    It was chosen over the default pickle serialization method for performance.
    According to the benchmark in `run_benchmark.py`, it
    is around 30x faster for large buffers. For apples to apples comparison, look at how the benchmark performs in
    commit 82e01b395e736e5c1cdaae7e1bd8a7dca3f78435 vs commit 89111ffea55063fd40f1894315b8c26673cbe6ce
    """

    # pylint: disable=too-many-instance-attributes

    def __init__(
        self,
        buf_size: int,
        max_num_elements: int,
        shared_buffer: bool,
        ctx: BaseContext,
    ) -> None:
        self.max_num_elements = max_num_elements
        self.orig_buf_size = buf_size
        # round buffer size up to align size so that every packets buffer starts as aligned
        self.buf_size = buf_size = roundup_to_align(buf_size)
        self._shared_mem_buf = ctx.RawArray(
            ctypes.c_byte, self.buf_size * self.max_num_elements
        )
        self.buf_sizes = ctx.RawArray(ctypes.c_int, self.max_num_elements)
        self.entry_alloc = CyclicAllocator(max_num_elements, ctx)
        self.shared_buffer = shared_buffer

    def put(self, item: Any):
        write_pos = self.entry_alloc.pop_write_pos()
        # now we own the relevant buffers and can write to them
        block_start = write_pos * self.buf_size
        mutable_block_position = Value(block_start)
        item_bytes = cloudpickle.dumps(
            item,
            protocol=cloudpickle.DEFAULT_PROTOCOL,
            buffer_callback=self.make_buffer_callback(mutable_block_position),
        )
        buffer_data_write_size = mutable_block_position.value - block_start
        self.buf_sizes[write_pos] = len(item_bytes)

        # ensure that buffer procol data (starting from beginning of entry)
        # and serialized pickle data (starting from end of entry) won't overlap and clobber each other
        total_write_size = len(item_bytes) + buffer_data_write_size
        if total_write_size > self.orig_buf_size:
            raise ValueError(
                f"Tried to pass item of serialized size {total_write_size}, but PipelineTask.max_message_size is {self.orig_buf_size}"
            )

        pickled_view = memoryview(item_bytes).cast("b")
        mem_view = memoryview(self._shared_mem_buf).cast("b")
        block_end = block_start + self.buf_size
        mem_view[block_end - len(item_bytes) : block_end] = pickled_view
        # makes entry avaliable for reading
        self.entry_alloc.push_read_pos(write_pos)

    def get(self):
        read_pos = self.entry_alloc.pop_read_pos()
        # no producers should be writing to this queue entry due to datastream's semaphores
        num_bytes = int(self.buf_sizes[read_pos])
        read_block_end = (read_pos + 1) * self.buf_size
        mem_view = memoryview(self._shared_mem_buf).cast("b")
        data_bytes = mem_view[read_block_end - num_bytes : read_block_end]

        loaded_data = cloudpickle.loads(
            data_bytes, buffers=self.iter_stored_buffers(read_pos)
        )
        return loaded_data, read_pos

    def free(self, read_pos: int):
        # frees up element for writing
        self.entry_alloc.push_write_pos(read_pos)

    def make_buffer_callback(self, block_position: Value):
        out_of_band_view = memoryview(self._shared_mem_buf).cast("b")
        out_of_band_size_view = memoryview(self._shared_mem_buf).cast("b").cast("i")

        start_pos = block_position.value
        # zero out starting marker
        out_of_band_size_view[start_pos // 4] = END_OF_BUFFER_ID

        def format_buffer(buf_obj):
            mem_view = memoryview(buf_obj)
            if mem_view.shape is None or any(
                dimsize == 0 for dimsize in mem_view.shape  # pylint: disable=E1133
            ):
                # casting not allowed for zero size memory-views, create a new one instead
                src_obj = memoryview(b"").cast("b")
            else:
                src_obj = mem_view.cast("b")
                obj_size = reduce(mul, src_obj.shape, 1)
                src_obj = src_obj.cast("b", (obj_size,))
            src_len = len(src_obj)
            cur_pos = block_position.value
            next_pos = cur_pos + roundup_to_align(ALIGN_SIZE + src_len)
            if next_pos - start_pos > self.buf_size:
                raise ValueError(
                    f"Serialized numpy data coming out to size at least {next_pos - start_pos} in size, but PipelineTask.max_message_size is {self.orig_buf_size}"
                )
            if next_pos - start_pos < self.buf_size:
                # mark current end of buffer by zeroing out size information
                out_of_band_size_view[next_pos // 4] = END_OF_BUFFER_ID

            # set integer size of next chunk
            out_of_band_size_view[cur_pos // 4] = src_len

            # set chunk data
            out_of_band_view[ALIGN_SIZE + cur_pos : ALIGN_SIZE + cur_pos + src_len] = (
                src_obj
            )

            block_position.value = next_pos

        return format_buffer

    def iter_stored_buffers(self, read_pos):
        out_of_band_view = memoryview(self._shared_mem_buf).cast("b")
        out_of_band_size_view = memoryview(self._shared_mem_buf).cast("b").cast("i")
        cur_pos = read_pos * self.buf_size
        end_pos = (read_pos + 1) * self.buf_size
        while cur_pos < end_pos:
            chunk_size = int(out_of_band_size_view[cur_pos // 4])
            if chunk_size == END_OF_BUFFER_ID:
                break
            out_buffer = out_of_band_view[
                ALIGN_SIZE + cur_pos : ALIGN_SIZE + cur_pos + chunk_size
            ]
            if not self.shared_buffer:
                # NOTE: this copies the memory out of shared memory, which is quite expensive
                # but can cause serious problems if references to this shared memory is kept
                # and is also passed to another pipeline step
                out_buffer = bytearray(out_buffer)
            yield out_buffer
            cur_pos += roundup_to_align(ALIGN_SIZE + chunk_size)

    def flush(self, has_error: synchronize.Event):
        # writes already flushed...
        pass


def _put_thread_start(
    _put_thread_sent: tr.Event,
    _put_thread_recived: tr.Event,
    _put_thread_joinable: tr.Event,
    _put_thread_item: List[Any],
    _write_end: mp_connection.Connection,
):
    while True:
        _put_thread_sent.wait()
        _put_thread_sent.clear()
        item = _put_thread_item[0]
        _put_thread_recived.set()

        # write position is reserved, no need to synchronize pipe
        _write_end.send_bytes(item)
        _put_thread_joinable.set()


class AsyncItemPassing:
    """A substitute for mp.Queue
    for one-to-one process sequential communication.
    Unlike mp.Pipe, put() does not block.
    Unlike mp.Queue, this is designed with
    sudden process exiting/erroring in mind.
    """

    def __init__(self, ctx: BaseContext) -> None:
        self._read_end, self._write_end = ctx.Pipe(duplex=False)
        self._put_thread_item = [None]
        # these cannot be pickled for spawn multiprocessing
        self._put_thread_recived = None
        self._put_thread_sent = None
        self._put_thread = None
        self._put_thread_joinable: Optional[synchronize.Event] = None

    def put(self, item):
        if self._put_thread_recived is None:
            self._set_write_end()

        # copies all contents to byte array so that
        # further mutations of the data after this
        # returns does not change the result
        self._put_thread_item[0] = cloudpickle.dumps(item)
        self._put_thread_joinable.clear()
        self._put_thread_sent.set()
        self._put_thread_recived.wait()
        self._put_thread_recived.clear()

    def get(self):
        try:
            return self._read_end.recv()
        except EOFError:
            # file wasn't readable at the moment, but hopefully it will be soon
            pass
        # wait for file to become readable
        select.select([self._read_end.fileno()], [], [])
        return self._read_end.recv()

    def _set_write_end(self):
        self._put_thread_recived = tr.Event()
        self._put_thread_sent = tr.Event()
        self._put_thread_joinable = tr.Event()
        self._put_thread_joinable.set()
        self._put_thread = tr.Thread(
            target=_put_thread_start,
            args=(
                self._put_thread_sent,
                self._put_thread_recived,
                self._put_thread_joinable,
                self._put_thread_item,
                self._write_end,
            ),
            daemon=True,
        )
        self._put_thread.start()

    def flush(self, has_error: synchronize.Event):
        if self._put_thread_joinable is not None:
            while not self._put_thread_joinable.wait(timeout=0.02):
                if has_error.is_set():
                    break


class PipedQueue(AsyncQueue):
    """
    A custom queue implementation based on piped buffers to transfer data.
    """

    def __init__(self, max_num_elements: int, ctx: BaseContext) -> None:
        self.max_num_elements = max_num_elements
        self.queues = [AsyncItemPassing(ctx) for _ in range(max_num_elements)]
        self.entry_alloc = CyclicAllocator(max_num_elements, ctx)

    def put(self, item: Any):
        write_pos = self.entry_alloc.pop_write_pos()
        # this pipe is reserved, will only be read by a single reader
        self.queues[write_pos].put(item)
        # makes entry available for reading
        self.entry_alloc.push_read_pos(write_pos)

    def get(self):
        read_pos = self.entry_alloc.pop_read_pos()
        # only one producer will be writing to this endpoint
        loaded_data = self.queues[read_pos].get()
        return loaded_data, read_pos

    def free(self, read_pos: int):
        # frees up element for writing
        self.entry_alloc.push_write_pos(read_pos)

    def flush(self, has_error: synchronize.Event):
        for queue_ in self.queues:
            queue_.flush(has_error)


class TaskOutput:
    def __init__(
        self,
        *,
        num_upstream_tasks: int,
        packets_in_flight: int,
        error_info: BufferedQueue,
        max_message_size: Optional[int],
        shared_buffer: bool,
        ctx: BaseContext,
    ) -> None:
        # pylint: disable=too-many-arguments
        self.num_tasks_remaining = ctx.Value("i", num_upstream_tasks, lock=True)
        self.last_updated_time = ctx.Value("d", time.monotonic(), lock=False)
        self.queue_len = ctx.Semaphore(value=0)
        self.packets_space = ctx.Semaphore(value=packets_in_flight)
        # using a custom queue implementation rather than multiprocessing.queue
        # because mp.Queue has strange synchronization properties with the semaphores, leading to many bugs
        self.queue = (
            BufferedQueue(max_message_size, packets_in_flight + 1, shared_buffer, ctx)
            if max_message_size is not None
            else PipedQueue(packets_in_flight + 1, ctx)
        )
        self.has_error = ctx.Event()
        self.error_info = error_info

    def iter_results(self) -> Iterable[Any]:
        while True:
            self.queue_len.acquire()  # pylint: disable=consider-using-with
            if self.has_error.is_set():
                raise PropogateErr()

            try:
                item, read_pos = self.queue.get()
            except queue.Empty:
                # only occurs if error occurred or no more producers left
                break

            yield item

            # frees shared memory buffer so that it can be used elsewhere
            self.queue.free(read_pos)

            # this release needs to happen after the yield
            # completes to support full synchronization semantics with packets_in_flight=1
            self.packets_space.release()

            # update last updated time
            self.last_updated_time.value = time.monotonic()

    def put_results(self, iterable: Iterable[Any]):
        iterator = iter(iterable)
        try:
            while True:
                # wait for space to be available on queue before iterating to next item
                # essential for full synchronization semantics with packets_in_flight=1
                self.packets_space.acquire()  # pylint: disable=consider-using-with

                if self.has_error.is_set():
                    raise PropogateErr()

                item = next(iterator)

                self.queue.put(item)
                self.queue_len.release()
        except StopIteration:
            # flush and clean up up queue resources since it is done putting
            self.queue.flush(has_error=self.has_error)
            # normal end of iteration
            with self.num_tasks_remaining.get_lock():
                self.num_tasks_remaining.get_obj().value -= 1
                if self.num_tasks_remaining.get_obj().value == 0:
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


def execute_mp(
    tasks: List[PipelineTask],
    spawn_method: SpawnContextName,
    inactivity_timeout: Optional[float] = None,
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

    if len(tasks) == 1:
        (task,) = tasks
        task.generator(**task.constants_dict)
        return

    ctx = typing.cast(Union[ForkContext, SpawnContext], mp.get_context(spawn_method))

    source_task = tasks[0]
    sink_task = tasks[-1]
    worker_tasks = tasks[1:-1]

    n_total_tasks = sum(task.num_workers for task in tasks)
    # use a BufferedQueue because it synchronizes instantly, unlike PipedQueue or mp.queue
    err_queue = BufferedQueue(ERR_BUF_SIZE, n_total_tasks + 2, False, ctx)
    # number of processes are of the producing task
    data_streams = [
        TaskOutput(
            num_upstream_tasks=t.num_workers,
            packets_in_flight=t.packets_in_flight,
            error_info=err_queue,
            max_message_size=t.max_message_size,
            shared_buffer=t.shared_buffer,
            ctx=ctx,
        )
        for t in tasks[:-1]
    ]
    processes: List[Union[ForkProcess, SpawnProcess]] = [
        ctx.Process(
            target=_start_source,
            args=(source_task, data_streams[0]),
            name=f"{source_task}_{worker_idx}",
        )
        for worker_idx in range(source_task.num_workers)
    ]
    for i, worker_task in enumerate(worker_tasks):
        for worker_idx in range(worker_task.num_workers):
            processes.append(
                ctx.Process(
                    target=_start_worker,
                    args=(worker_task, data_streams[i], data_streams[i + 1]),
                    name=f"{worker_task}_{worker_idx}",
                )
            )

    for worker_idx in range(sink_task.num_workers):
        processes.append(
            ctx.Process(
                target=_start_sink,
                args=(sink_task, data_streams[-1]),
                name=f"{sink_task}_{worker_idx}",
            )
        )

    for process in processes:
        process.start()

    # signal setup must be *after* all new processes are started, so that main processes
    # signal handling won't be copied over to children
    with sighandler({signal.SIGINT, signal.SIGTERM}, processes):
        has_error = False
        try:
            sentinel_map = {proc.sentinel: proc for proc in processes}
            sentinel_set = {proc.sentinel for proc in processes}
            while sentinel_set and not has_error:
                done_sentinels = mp_connection.wait(
                    list(sentinel_set),
                    timeout=(
                        None if inactivity_timeout is None else inactivity_timeout / 10
                    ),
                )
                last_updated_time = max(
                    float(stream.last_updated_time.value) for stream in data_streams
                )
                if inactivity_timeout is not None and not done_sentinels:
                    # this means the timeout ended,
                    # time to check all of the task outputs timers
                    last_updated_time = max(
                        float(stream.last_updated_time.value) for stream in data_streams
                    )
                    if time.monotonic() - last_updated_time > inactivity_timeout:
                        raise InactivityError(
                            f"Last updated time was {time.monotonic() - last_updated_time}s ago, pipeline inactivity timeout is {inactivity_timeout}s."
                        )

                sentinel_set -= set(done_sentinels)
                for done_id in done_sentinels:
                    assert isinstance(
                        done_id, int
                    ), f"mp_connection.wait returned unexpected type: {done_id}"
                    # for some reason needs a join, or the exitcode doesn't sync properly
                    # but it has already exited, so this should finish very quickly
                    sentinel_map[done_id].join()
                    if sentinel_map[done_id].exitcode is None:
                        # unsure what could cause this, but we see it in production sometimes
                        # when an instance is shutting down
                        logger.warning("Child process joined with exitcode None.")
                    elif sentinel_map[done_id].exitcode != 0:
                        # attempts to catch segfaults and other errors that cannot be caught by python (i.g. sigkill)
                        proc_err_msg = f"Process: {sentinel_map[done_id].name} exited with non-zero code {sentinel_map[done_id].exitcode}"
                        for stream in data_streams:
                            stream.set_error(
                                sentinel_map[done_id].name, TaskError(proc_err_msg), ""
                            )
                        has_error = True
                        break

            if has_error:
                # first entry on the error queue should hopefully be the original error, just raise that one single error
                (task_name, task_err, traceback_str), _ = err_queue.get()
                # should only be at most one unique error, just raise it
                raise TaskError(
                    f"Task; {task_name} errored\n{traceback_str}\n{task_err}"
                ) from task_err

        except BaseException as err:  # pylint: disable=broad-except
            if not has_error:
                # one of the other processes didn't set an error, so it must be the main process that is the problem
                has_error = True
                tb_str = traceback.format_exc()
                for stream in data_streams:
                    stream.set_error("_main_thread", err, tb_str)

            # joins processes as cleanup if they successfully exited
            # give them a decent amount of time to process their current task and exit cleanly
            for proc in processes:
                proc.join(timeout=15.0)
            # escalate, send sigterm to processes
            for proc in processes:
                proc.terminate()
            # wait for terminate signal to propagate through the processes
            for proc in processes:
                proc.join(timeout=5.0)
            # force kill the processes (only if they are refusing to terminate cleanly)
            for proc in processes:
                proc.kill()
                proc.join()

            raise err
