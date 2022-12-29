from typing import List, Literal

from .mp_execution import execute_mp
from .pipeline_task import PipelineTask
from .seq_execution import execute_seq
from .tr_execution import execute_tr

ParallelismStrategy = Literal["thread", "process", "coroutine"]


def execute(tasks: List[PipelineTask], parallelism: ParallelismStrategy = "thread"):
    if parallelism == "thread":
        execute_tr(tasks)
    elif parallelism == "process":
        execute_mp(tasks)
    elif parallelism == "coroutine":
        execute_seq(tasks)
    else:
        raise ValueError(
            '`execute`\'s parallelism argument must be one of ["thread", "process", "coroutine"]'
        )
