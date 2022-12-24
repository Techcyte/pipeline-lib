import multiprocessing as mp
import pickle
from contextlib import contextmanager

import pytest

from pipeline_executor import PipelineTask, execute

from .example_funcs import *

TEMP_FILE = "/tmp/pipeline_pickle"


def save_results(vals: Iterable[int]) -> None:
    with open(TEMP_FILE, "wb") as file:
        pickle.dump(list(vals), file)


def load_results():
    with open(TEMP_FILE, "rb") as file:
        return pickle.load(file)


def test_execute():
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
    execute(tasks)


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


def test_execute_exception():
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
        execute(tasks)


class SuddenExit(RuntimeError):
    pass


def sudden_exit_fn(arg: Iterable[int]) -> Iterable[int]:
    # start up input generator/process
    next(iter(arg))
    # thread raises exception so that python does not know about it
    raise SuddenExit("sudden exit")


def test_sudden_exit_middle():
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
        execute(tasks)


def test_sudden_exit_end():
    tasks = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            sudden_exit_fn,
        ),
        PipelineTask(save_results),
    ]
    with raises_from(SuddenExit):
        execute(tasks)


def only_error_if_second_proc(
    arg: Iterable[int], started_event: mp.Event
) -> Iterable[int]:
    """
    only exits if it is the first worker process to start up.
    """
    is_second_proc = started_event.is_set()
    started_event.set()
    if is_second_proc:
        raise TestExpectedException()
    else:
        yield from arg


def test_single_worker_error():
    """
    if one process dies and the others do not, then it should still raise an exception,
    as the dead process might have consumed an important message
    """
    started_event = mp.Event()
    tasks = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            only_error_if_second_proc,
            constants={
                "started_event": started_event,
            },
            num_workers=2,
            packets_in_flight=2,
        ),
        PipelineTask(print_numbers, num_workers=2, packets_in_flight=2),
    ]
    with raises_from(TestExpectedException):
        execute(tasks)


def generate_many() -> Iterable[int]:
    yield from range(30000)


def test_many_workers_correctness():
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
        PipelineTask(save_results),
    ]
    execute(tasks)
    actual_result = sum(load_results())
    expected_result = 450135000
    assert actual_result == expected_result


def test_many_packets_correctness():
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
        PipelineTask(save_results),
    ]
    execute(tasks)
    actual_result = sum(load_results())
    expected_result = 450135000
    assert actual_result == expected_result
