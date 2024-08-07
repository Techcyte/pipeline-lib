import multiprocessing as mp
import os
import pickle
import signal
import time
import typing
from contextlib import contextmanager
from typing import Any, Dict

import numpy as np
import psutil
import pytest

import pipeline_lib
from pipeline_lib import PipelineTask, execute
from pipeline_lib.execution import ParallelismStrategy
from pipeline_lib.pipeline_task import InactivityError

from .example_funcs import *

all_parallelism_options = typing.get_args(ParallelismStrategy)

TEMP_FILENAME = "_test_pipeline_pickle.data"


def save_results(vals: Iterable[int], tmpdir: str) -> None:
    with open(os.path.join(tmpdir, TEMP_FILENAME), "wb") as file:
        pickle.dump(list(vals), file)


def load_results(tempdir: str):
    with open(os.path.join(tempdir, TEMP_FILENAME), "rb") as file:
        return pickle.load(file)


@pytest.mark.parametrize("parallelism", all_parallelism_options)
def test_execute(parallelism: ParallelismStrategy):
    tasks = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            group_numbers,
            constants={"num_groups": 5},
        ),
        PipelineTask(
            sum_numbers,
        ),
        PipelineTask(
            print_numbers,
        ),
    ]
    execute(tasks, parallelism)


@contextmanager
def raises_from(err_type):
    try:
        yield
    except Exception as err:
        if isinstance(err, err_type) or (
            err.__cause__ and isinstance(err.__cause__, err_type)
        ):
            # passes test
            return
        raise AssertionError(f"expected error of type {err_type} got error {err}")


def test_raises_from():
    # tests testing utility above
    with pytest.raises(AssertionError):
        with raises_from(RuntimeError):
            raise ValueError()
    with raises_from(ValueError):
        raise ValueError()


class TestExpectedException(ValueError):
    pass


def raise_exception_fn(arg: Iterable[int]) -> Iterable[int]:
    # start up input generator/process
    i1 = next(iter(arg))
    yield i1
    raise TestExpectedException()


@pytest.mark.parametrize("parallelism", all_parallelism_options)
def test_execute_exception(parallelism: ParallelismStrategy):
    tasks = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            raise_exception_fn,
        ),
        PipelineTask(
            print_numbers,
        ),
    ]
    with raises_from(TestExpectedException):
        execute(tasks, parallelism)


class SuddenExit(RuntimeError):
    pass


def sudden_exit_fn(arg: Iterable[int]) -> Iterable[int]:
    # start up input generator/process
    next(iter(arg))
    # thread raises exception so that python does not know about it
    raise SuddenExit("sudden exit")


@pytest.mark.parametrize("parallelism", all_parallelism_options)
def test_sudden_exit_middle(parallelism: ParallelismStrategy):
    tasks = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            sudden_exit_fn,
        ),
        PipelineTask(
            print_numbers,
        ),
    ]
    with raises_from(SuddenExit):
        execute(tasks, parallelism)


@pytest.mark.parametrize("parallelism", all_parallelism_options)
def test_sudden_exit_end(parallelism: ParallelismStrategy):
    tasks = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            sudden_exit_fn,
        ),
        PipelineTask(print_numbers),
    ]
    with raises_from(SuddenExit):
        execute(tasks, parallelism)


def sleeper(vals: Iterable[int], sleep_time: float) -> Iterable[int]:
    time.sleep(0.1)
    for i in vals:
        time.sleep(sleep_time)
        yield i


@pytest.mark.parametrize("parallelism", all_parallelism_options)
def test_sudden_exit_middle_sleepers(parallelism: ParallelismStrategy):
    tasks = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(sleeper, constants={"sleep_time": 0.1}),
        PipelineTask(
            sudden_exit_fn,
        ),
        PipelineTask(sleeper, constants={"sleep_time": 0.1}),
        PipelineTask(
            print_numbers,
        ),
    ]
    with raises_from(SuddenExit):
        execute(tasks, parallelism)


def generate_numbers_short() -> Iterable[int]:
    for i in range(9):
        yield i


@pytest.mark.parametrize("parallelism", ["process-fork", "process-spawn"])
def test_inactivty_timeout(parallelism: ParallelismStrategy):
    """
    If we sleep for 1 second and have a task timeout of 0.1 seconds,
    we should error due to the task timeout
    """
    tasks = [
        PipelineTask(
            generate_numbers_short,
        ),
        PipelineTask(sleeper, constants={"sleep_time": 1}),
        PipelineTask(
            print_numbers,
        ),
    ]
    with raises_from(InactivityError):
        execute(tasks, parallelism, inactivity_timeout=0.1)


@pytest.mark.parametrize("parallelism", ["process-fork", "process-spawn"])
def test_inactivity_timeout_missed(parallelism: ParallelismStrategy):
    """
    If we sleep for 0.1 second and have a task timeout of 1 seconds,
    we should not error due to the task timeout
    """
    tasks = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(sleeper, constants={"sleep_time": 0.1}),
        PipelineTask(
            print_numbers,
        ),
    ]
    # pipeline step should take about 10 seconds, 100 iters of 0.1 seconds each, so
    # this catches that it is only inactivity
    execute(tasks, parallelism, inactivity_timeout=1)


@pytest.mark.parametrize("parallelism", all_parallelism_options)
def test_full_contents_buffering(parallelism: ParallelismStrategy):
    tasks = [
        PipelineTask(generate_numbers, packets_in_flight=1000, max_message_size=1000),
        PipelineTask(
            sleeper,
            constants={"sleep_time": 0.1},
            packets_in_flight=1000,
            max_message_size=1000,
        ),
        PipelineTask(
            print_numbers,
        ),
    ]
    execute(tasks, parallelism)


def add_one_to(vals: Iterable[int], value: mp.Value) -> Iterable[int]:
    for v in vals:
        value.value += 1
        assert value.value == 1
        yield v


def sub_one_to(vals: Iterable[int], value: mp.Value) -> Iterable[int]:
    for v in vals:
        value.value -= 1
        assert value.value == 0
        yield v


@pytest.mark.parametrize("parallelism", all_parallelism_options)
def test_full_synchronization(parallelism: ParallelismStrategy):
    val = mp.Value("i", 0, lock=False)
    tasks = [
        PipelineTask(
            generate_numbers,
            packets_in_flight=1,
        ),
        PipelineTask(add_one_to, packets_in_flight=1, constants=dict(value=val)),
        PipelineTask(sub_one_to, packets_in_flight=1, constants=dict(value=val)),
        PipelineTask(add_one_to, packets_in_flight=1, constants=dict(value=val)),
        PipelineTask(sub_one_to, packets_in_flight=1, constants=dict(value=val)),
        PipelineTask(print_numbers, packets_in_flight=1),
    ]
    execute(tasks, parallelism)


def only_error_if_second_proc(
    arg: Iterable[int], started_event: mp.Event
) -> Iterable[int]:
    """
    only exits if it is the first worker process to start up.
    """
    yield next(iter(arg))
    is_second_proc = started_event.is_set()
    started_event.set()
    if is_second_proc:
        raise TestExpectedException()
    else:
        yield from arg


def generate_infinite() -> Iterable[int]:
    yield from range(10000000000000)


@pytest.mark.parametrize("parallelism", ["thread", "process-fork", "process-spawn"])
def test_single_worker_error(parallelism: ParallelismStrategy):
    """
    if one process dies and the others do not, then it should still raise an exception,
    as the dead process might have consumed an important message
    """
    mp_context = mp.get_context("spawn") if parallelism == "process-spawn" else mp
    started_event = mp_context.Event()
    tasks = [
        PipelineTask(
            generate_infinite,
        ),
        PipelineTask(
            only_error_if_second_proc,
            constants={
                "started_event": started_event,
            },
            num_workers=2,
            packets_in_flight=10,
        ),
        PipelineTask(print_numbers, num_workers=2, packets_in_flight=2),
    ]
    with raises_from(TestExpectedException):
        execute(tasks, parallelism)


def force_exit_if_second_proc(
    arg: Iterable[int], started_event: mp.Event
) -> Iterable[int]:
    """
    only exits if it is the first worker process to start up.
    """
    yield next(iter(arg))
    is_second_proc = started_event.is_set()
    started_event.set()
    if is_second_proc:
        # kill process using very low level os utilities
        # so that python does not know anything about process exiting
        os.kill(os.getpid(), signal.SIGKILL)
    else:
        yield from arg


@pytest.mark.parametrize("parallelism", ["process-spawn", "process-fork"])
def test_single_worker_unexpected_exit(parallelism: ParallelismStrategy):
    """
    if one process dies and the others do not, then it should still raise an exception,
    as the dead process might have consumed an important message
    """
    started_event = mp.Event()
    tasks = [
        PipelineTask(
            generate_infinite,
        ),
        PipelineTask(
            force_exit_if_second_proc,
            constants={
                "started_event": started_event,
            },
            num_workers=2,
            packets_in_flight=10,
        ),
        PipelineTask(print_numbers, num_workers=2, packets_in_flight=2),
    ]
    with raises_from(pipeline_lib.pipeline_task.TaskError):
        execute(tasks, parallelism)


def consume_infinite_ints(vals: Iterable[int]) -> None:
    for _ in vals:
        pass


def start_pipeline(parallelism: ParallelismStrategy):
    execute(
        [
            PipelineTask(
                generate_infinite,
            ),
            PipelineTask(consume_infinite_ints, num_workers=2, packets_in_flight=2),
        ],
        parallelism=parallelism,
    )


@pytest.mark.parametrize("parallelism", ["process-spawn", "process-fork"])
def test_main_process_sigterm(parallelism: ParallelismStrategy):
    """
    If main process receives a sigterm signal,
    all the other processes should exit cleanly and quickly
    """
    ctx = mp.get_context("spawn")
    proc = ctx.Process(target=start_pipeline, args=(parallelism,))
    proc.start()
    # wait for worker processes to start up
    time.sleep(2.0)

    # collect all living child processes
    child_procs = psutil.Process(proc.pid).children()
    # there are 3 child processes we expect
    # 1 generate_infinite process, 2 consume_infinite_ints processes
    assert len(child_procs) == 3

    # send a sigterm signal to the main process
    os.kill(proc.pid, signal.SIGTERM)

    # waits for all the processes to shut down
    proc.join(5.0)
    assert (
        proc.exitcode is not None
    ), "join timed out, main process did not exist promptly after signterm"
    assert (
        proc.exitcode == -15
    ), "main process should return a -15 error code after being hit with a sigterm"

    for proc in child_procs:
        assert not psutil.pid_exists(
            proc.pid
        ), "main process didn't exit and join children during its shutdown process"


def generate_many() -> Iterable[int]:
    yield from range(30000)


@pytest.mark.parametrize("parallelism", all_parallelism_options)
def test_many_workers_correctness(tmpdir, parallelism: ParallelismStrategy):
    """
    Tests that many workers working on lots of data
    eventually returns the correct result, without packet loss or exceptions
    """
    tasks = [
        PipelineTask(
            generate_many,
        ),
        PipelineTask(
            add_const,
            constants={
                "add_val": 5,
            },
            num_workers=15,
            packets_in_flight=15,
        ),
        PipelineTask(
            group_numbers,
            constants={"num_groups": 10},
            num_workers=1,
            packets_in_flight=1,
        ),
        PipelineTask(
            sum_numbers,
            num_workers=16,
            packets_in_flight=20,
        ),
        PipelineTask(save_results, constants=dict(tmpdir=tmpdir)),
    ]
    execute(tasks, parallelism)
    actual_result = sum(load_results(tmpdir))
    expected_result = 450135000
    assert actual_result == expected_result


@pytest.mark.parametrize("parallelism", all_parallelism_options)
def test_many_packets_correctness(tmpdir, parallelism: ParallelismStrategy):
    """
    Tests that many workers working on lots of data
    eventually returns the correct result, without packet loss or exceptions
    """
    tasks = [
        PipelineTask(
            generate_many,
            packets_in_flight=10,
        ),
        PipelineTask(
            add_const,
            constants={
                "add_val": 5,
            },
            num_workers=4,
            packets_in_flight=40,
        ),
        PipelineTask(
            group_numbers,
            constants={"num_groups": 10},
            num_workers=4,
            packets_in_flight=10,
        ),
        PipelineTask(
            sum_numbers,
            num_workers=4,
            packets_in_flight=100,
        ),
        PipelineTask(save_results, constants=dict(tmpdir=tmpdir)),
    ]
    execute(tasks, parallelism)
    results = load_results(tmpdir)
    actual_result = sum(results)
    expected_result = 450135000
    assert actual_result == expected_result


N_BIG_MESSAGES = 100
BIG_MESSAGE_SIZE = 200000
BIG_MESSAGE_BYTES = 4 * BIG_MESSAGE_SIZE + 5000


def generate_large_messages() -> Iterable[Dict[str, Any]]:
    for i in range(N_BIG_MESSAGES):
        val1 = np.arange(BIG_MESSAGE_SIZE, dtype="int32").reshape(100, -1) + i
        yield {
            "message_type": "big",
            "message_1_contents": val1,
            "val1_ref": val1,
            "message_2_contents": (np.arange(500, dtype="int64") * i),
        }


def process_message(messages: Iterable[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    for msg in messages:
        msg["processed"] = True
        # adds 1 to every element in this and its reference in `val1_ref`
        msg["message_1_contents"] += 1
        yield msg


def sum_arrays(messages: Iterable[Dict[str, Any]]) -> Iterable[int]:
    for msg in messages:
        yield (
            msg["message_1_contents"].astype("int64").sum()
            + msg["val1_ref"].astype("int64").sum()
            + msg["message_2_contents"].astype("int64").sum()
        )


@pytest.mark.parametrize("parallelism", all_parallelism_options)
@pytest.mark.parametrize("n_procs,packets_in_flight", [(1, 1), (1, 4), (4, 16)])
@pytest.mark.parametrize("shared_buffer", [True, False])
def test_many_large_packets_correctness(
    tmpdir,
    n_procs: int,
    packets_in_flight: int,
    shared_buffer: bool,
    parallelism: ParallelismStrategy,
):
    tasks = [
        PipelineTask(
            generate_large_messages,
            max_message_size=BIG_MESSAGE_BYTES,
            shared_buffer=shared_buffer,
        ),
        PipelineTask(
            process_message,
            max_message_size=BIG_MESSAGE_BYTES,
            num_workers=n_procs,
            packets_in_flight=packets_in_flight,
            shared_buffer=shared_buffer,
        ),
        PipelineTask(
            process_message,
            # process with piped messages
            num_workers=n_procs,
            packets_in_flight=packets_in_flight,
        ),
        PipelineTask(
            sum_arrays,
            num_workers=n_procs,
            packets_in_flight=packets_in_flight,
        ),
        PipelineTask(save_results, constants=dict(tmpdir=tmpdir)),
    ]
    execute(tasks, parallelism)
    actual_result = sum(load_results(tmpdir))
    expected_result = 4002657512500
    assert actual_result == expected_result


def hang_message_passing() -> Iterable[Dict[str, Any]]:
    for i in range(8):
        val1 = np.arange(BIG_MESSAGE_SIZE, dtype="int32").reshape(100, -1) + i
        yield {
            "message_type": "big",
            "message_1_contents": val1,
            "val1_ref": val1,
            "message_2_contents": (np.arange(500, dtype="int64") * i),
        }
    exit(0)


# if it takes more than 10 seconds for a 5 second timeout to complete, something is wrong
@pytest.mark.timeout(120)
@pytest.mark.parametrize("parallelism", ["process-fork", "process-spawn"])
@pytest.mark.parametrize("max_message_size", [BIG_MESSAGE_BYTES, None])
def test_hang_message_passing_timeout(
    tmpdir,
    max_message_size: bool,
    parallelism: ParallelismStrategy,
):
    n_procs = 2
    packets_in_flight = 4
    tasks = [
        PipelineTask(
            hang_message_passing,
            max_message_size=max_message_size,
            packets_in_flight=packets_in_flight,
        ),
        PipelineTask(
            process_message,
            max_message_size=max_message_size,
            packets_in_flight=packets_in_flight,
            num_workers=n_procs,
        ),
        PipelineTask(
            sum_arrays,
            max_message_size=max_message_size,
            packets_in_flight=packets_in_flight,
            num_workers=n_procs,
        ),
        PipelineTask(save_results, constants=dict(tmpdir=tmpdir)),
    ]
    with raises_from(InactivityError):
        execute(tasks, parallelism, inactivity_timeout=5)


def generate_zero_siz_np_arrays() -> Iterable[np.ndarray]:
    for _ in range(10):
        val1 = np.zeros((7, 0, 4), dtype="int32")
        yield val1


def consume_nd(inpt: Iterable[np.ndarray]) -> None:
    for _ in inpt:
        pass


@pytest.mark.parametrize("parallelism", all_parallelism_options)
def test_zero_size_np_arrays(parallelism: bool):
    """zero size buffer passing has some edge cases, so testing that it works
    with a the buffer passing"""
    tasks = [
        PipelineTask(
            generate_zero_siz_np_arrays,
            max_message_size=100000,
        ),
        PipelineTask(consume_nd),
    ]
    execute(tasks, parallelism)


if __name__ == "__main__":
    # failed at:
    # :test_many_large_packets_correctness[4-16-process-spawn-1-10]
    test_many_large_packets_correctness("/tmp", 2, 4, False, "process-fork")
    # test_zero_size_np_arrays("process-spawn")
    # test_hang_message_passing_timeout("/tmp", BIG_MESSAGE_BYTES, "process-spawn")
