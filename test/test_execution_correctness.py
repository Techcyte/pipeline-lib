import multiprocessing as mp
import os
import pickle
from typing import Any, Dict

import numpy as np
import pytest

from pipeline_lib import PipelineTask, execute
from pipeline_lib.execution import ParallelismStrategy

from .example_funcs import *
from .test_utils import all_parallelism_options, sleeper

TEMP_FILENAME = "_test_pipeline_pickle.data"


def save_results(vals: Iterable[int], tmpdir: str) -> None:
    with open(os.path.join(tmpdir, TEMP_FILENAME), "wb") as file:
        pickle.dump(list(vals), file)


def load_results(tempdir: str):
    with open(os.path.join(tempdir, TEMP_FILENAME), "rb") as file:
        return pickle.load(file)


@pytest.mark.parametrize("parallelism", all_parallelism_options)
def test_execute_basic(parallelism: ParallelismStrategy):
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


@pytest.mark.parametrize("parallelism", all_parallelism_options)
def test_full_contents_buffering(tmpdir: str, parallelism: ParallelismStrategy):
    """
    Tests the case where the initial work generator fills up the buffer
    and exits, long before the pipeline has completed. Test that the pipeline doesn't
    prematurely terminate the pipeline when the first worker exits.
    """
    tasks = [
        PipelineTask(generate_numbers, packets_in_flight=1000, max_message_size=1000),
        PipelineTask(
            sleeper,
            constants={"sleep_time": 0.1},
            packets_in_flight=1000,
            max_message_size=1000,
            num_workers=2,
        ),
        PipelineTask(save_results, constants=dict(tmpdir=tmpdir)),
    ]
    execute(tasks, parallelism)
    actual_result = sum(load_results(tmpdir))
    expected_result = (101 * 100) // 2
    assert actual_result == expected_result


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
def test_full_synchronization(tmpdir: str, parallelism: ParallelismStrategy):
    """
    Tests that packets_in_flight=1 forces fully synchronous work
    with no interleaving computation
    """
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
        PipelineTask(save_results, constants=dict(tmpdir=tmpdir), packets_in_flight=1),
    ]
    execute(tasks, parallelism)
    actual_result = load_results(tmpdir)
    expected_result = [*range(101)]
    assert actual_result == expected_result


def generate_zero_size_np_arrays() -> Iterable[np.ndarray]:
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
            generate_zero_size_np_arrays,
            max_message_size=100000,
        ),
        PipelineTask(consume_nd),
    ]
    execute(tasks, parallelism)


def generate_many() -> Iterable[int]:
    yield from range(30000)


@pytest.mark.parametrize("parallelism", all_parallelism_options)
def test_many_workers_correctness(tmpdir: str, parallelism: ParallelismStrategy):
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
def test_many_packets_correctness(tmpdir: str, parallelism: ParallelismStrategy):
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
    tmpdir: str,
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


if __name__ == "__main__":
    test_full_synchronization("/tmp", "thread")
