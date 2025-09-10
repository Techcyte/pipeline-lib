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
