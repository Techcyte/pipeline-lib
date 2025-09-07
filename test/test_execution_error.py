import multiprocessing as mp
import os
import signal
import time
from multiprocessing import synchronize
from typing import Any, Dict

import numpy as np
import psutil
import pytest

import pipeline_lib
from pipeline_lib import PipelineTask, execute
from pipeline_lib.execution import ParallelismStrategy
from pipeline_lib.pipeline_task import InactivityError

from .example_funcs import *
from .test_utils import (
    all_parallelism_options,
    process_parallelism_options,
    raises_from,
    sleeper,
    thread_parallelism_options,
)


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


@pytest.mark.parametrize("parallelism", thread_parallelism_options)
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


@pytest.mark.parametrize("parallelism", thread_parallelism_options)
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
    execute(tasks, parallelism, inactivity_timeout=5)


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


@pytest.mark.parametrize("parallelism", all_parallelism_options)
@pytest.mark.parametrize(
    "sig,returnval",
    [
        (signal.SIGTERM, -15),
        (signal.SIGINT, 1),
    ],
)
def test_main_process_signal(
    sig: signal.Signals, returnval: int, parallelism: ParallelismStrategy
):
    """
    If main process receives a sigterm signal,
    all the other processes should exit cleanly and quickly
    """
    ctx = mp.get_context("spawn")
    proc = ctx.Process(target=start_pipeline, args=(parallelism,))
    proc.start()
    # wait for worker processes to start up
    time.sleep(10.0)

    # collect all living child processes
    child_procs = psutil.Process(proc.pid).children()

    process_parallelism = set(process_parallelism_options)
    if parallelism in process_parallelism:
        # there are 3 child processes we expect
        # 1 generate_infinite process, 2 consume_infinite_ints processes
        assert len(child_procs) == 3
    else:
        # in thread or coroutine, there won't be any child processes, that is expected
        assert len(child_procs) == 0

    # send a sigterm signal to the main process
    os.kill(proc.pid, sig)

    # waits for all the processes to shut down
    proc.join(10.0)
    assert (
        proc.exitcode is not None
    ), "join timed out, main process did not exist promptly after signterm"
    assert (
        proc.exitcode == returnval
    ), f"main process should return a {returnval} error code, returned {proc.exitcode}"

    for proc in child_procs:
        assert not psutil.pid_exists(
            proc.pid
        ), "main process didn't exit and join children during its shutdown process"


def only_error_if_second_proc(
    arg: Iterable[int], started_event: synchronize.Event
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


@pytest.mark.parametrize("parallelism", thread_parallelism_options)
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
    arg: Iterable[int], started_event: synchronize.Event
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


@pytest.mark.parametrize("parallelism", process_parallelism_options)
def test_single_worker_unexpected_exit(parallelism: ParallelismStrategy):
    """
    if one process dies and the others do not, then it should still raise an exception,
    as the dead process might have consumed an important message
    """
    process_context_map: Dict[ParallelismStrategy, str] = {
        "process-fork": "fork",
        "process-spawn": "spawn",
    }
    ctx = mp.get_context(process_context_map[parallelism])
    started_event = ctx.Event()
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


def hang_message_passing() -> Iterable[int]:
    for i in range(8):
        yield i
    exit(0)


# if it takes more than 120 seconds for a 5 second timeout to complete, something is wrong
@pytest.mark.timeout(120)
@pytest.mark.parametrize("parallelism", process_parallelism_options)
@pytest.mark.parametrize("max_message_size", [10000, None])
def test_hang_message_passing_timeout(
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
            group_numbers,
            constants={"num_groups": 3},
            max_message_size=max_message_size,
            packets_in_flight=packets_in_flight,
            num_workers=n_procs,
        ),
        PipelineTask(
            sum_numbers,
            max_message_size=max_message_size,
            packets_in_flight=packets_in_flight,
            num_workers=n_procs,
        ),
        PipelineTask(print_numbers),
    ]
    with raises_from(InactivityError):
        execute(tasks, parallelism, inactivity_timeout=5)


if __name__ == "__main__":
    test_hang_message_passing_timeout(1000, 'process-fork')
