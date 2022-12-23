import multiprocessing as mp
import pickle

import pytest

from pipeline_executor import PipelineTask, execute

from .example_funcs import *

TEMP_FILE = "/tmp/pipeline_pickle"


def print_vals(vals: Iterable[int]) -> Iterable[int]:
    for i in vals:
        print(i)
        yield i


def save_results(vals: Iterable[int]) -> None:
    vals = print_vals(vals)
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
    with pytest.raises(TestExpectedException):
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
    with pytest.raises(SuddenExit):
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
    with pytest.raises(SuddenExit):
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
            num_threads=2,
            packets_in_flight=2,
        ),
        PipelineTask(print_numbers, num_threads=2, packets_in_flight=2),
    ]
    with pytest.raises(TestExpectedException):
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
            num_threads=4,
            packets_in_flight=4,
        ),
        PipelineTask(
            group_numbers,
            constants={"num_groups": 10},
            num_threads=1,
        ),
        PipelineTask(
            sum_numbers,
            num_threads=4,
            packets_in_flight=4,
        ),
        PipelineTask(save_results),
    ]
    execute(tasks)
    actual_result = sum(load_results())
    expected_result = 450135000
    assert actual_result == expected_result


if __name__ == "__main__":
    try:
        test_execute()
    except Exception as err:
        print("err")
        print(err)
        print(type(err))
