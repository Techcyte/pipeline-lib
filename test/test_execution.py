import multiprocessing as mp
import os
import pickle
import signal
import time
import typing
from contextlib import contextmanager
from typing import Any, Dict

import numpy as np
import pytest

import pipeline_executor
from pipeline_executor import PipelineTask, execute
from pipeline_executor.execution import ParallelismStrategy

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


def sleeper(vals: Iterable[int]) -> Iterable[int]:
    time.sleep(0.1)
    for i in vals:
        time.sleep(0.01)
        yield i


@pytest.mark.parametrize("parallelism", all_parallelism_options)
def test_sudden_exit_middle_sleepers(parallelism: ParallelismStrategy):
    tasks = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(sleeper),
        PipelineTask(
            sudden_exit_fn,
        ),
        PipelineTask(sleeper),
        PipelineTask(
            print_numbers,
        ),
    ]
    with raises_from(SuddenExit):
        execute(tasks, parallelism)


@pytest.mark.parametrize("parallelism", all_parallelism_options)
def test_full_contents_buffering(parallelism: ParallelismStrategy):
    tasks = [
        PipelineTask(generate_numbers, packets_in_flight=1000, max_message_size=1000),
        PipelineTask(sleeper, packets_in_flight=1000, max_message_size=1000),
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
    with raises_from(pipeline_executor.pipeline_task.TaskError):
        execute(tasks, parallelism)


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
        val1 = np.arange(BIG_MESSAGE_SIZE, dtype="int32") + i
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
def test_many_large_packets_correctness(
    tmpdir, n_procs: int, packets_in_flight: int, parallelism: ParallelismStrategy
):
    tasks = [
        PipelineTask(
            generate_large_messages,
            max_message_size=BIG_MESSAGE_BYTES,
        ),
        PipelineTask(
            process_message,
            max_message_size=BIG_MESSAGE_BYTES,
            num_workers=n_procs,
            packets_in_flight=packets_in_flight,
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


if __name__ == "__main__":
    # failed at:
    # :test_many_large_packets_correctness[4-16-process-spawn-1-10]
    # test_many_large_packets_correctness("/tmp", 2, 4, "process-spawn")
    test_full_synchronization("process-spawn")
