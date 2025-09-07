import time
import typing
from contextlib import contextmanager
from typing import Iterable, List

import pytest

from pipeline_lib.execution import ParallelismStrategy

all_parallelism_options: List[ParallelismStrategy] = typing.get_args(
    ParallelismStrategy
)
thread_parallelism_options: List[ParallelismStrategy] = [
    "thread",
    "process-fork",
    "process-spawn",
]
process_parallelism_options: List[ParallelismStrategy] = [
    "process-fork",
    "process-spawn",
]


def sleeper(vals: Iterable[int], sleep_time: float) -> Iterable[int]:
    time.sleep(0.1)
    for i in vals:
        time.sleep(sleep_time)
        yield i


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
