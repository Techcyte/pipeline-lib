from typing import List, Literal, Tuple, get_args

from .mp_execution import execute_mp
from .pipeline_task import PipelineTask
from .seq_execution import execute_seq
from .tr_execution import execute_tr

ParallelismStrategy = Literal["thread", "process-fork", "process-spawn", "coroutine"]

# list of strings in ParallelismStrategy
PARALLELISM_STRATEGIES: Tuple[str, ...] = get_args(ParallelismStrategy)


def execute(tasks: List[PipelineTask], parallelism: ParallelismStrategy = "thread", inactivity_timeout: float | None = None):
    """
    execute tasks until final task completes.
    Raises error if tasks are inconsistently specified or if
    one of the tasks raises an error.

    Also raises an error if no message passing is observed in any task for
    at least `inactivity_timeout` seconds.
    (useful to kill any stuck jobs in a larger distributed system)
    """
    if parallelism == "thread":
        assert inactivity_timeout is None, "'thread' parallelism does not support inactivity timeout, please only choose "
        execute_tr(tasks)
    elif parallelism == "process-spawn":
        execute_mp(tasks, "spawn")
    elif parallelism == "process-fork":
        execute_mp(tasks, "fork", inactivity_timeout=inactivity_timeout)
    elif parallelism == "coroutine":
        assert inactivity_timeout is None, "'coroutine' parallelism does not support inactivity timeout, please choose parallelism='process-fork' or parallelism='process-spawn'"
        execute_seq(tasks)
    else:
        raise ValueError(
            f"`execute`'s parallelism argument must be one of {PARALLELISM_STRATEGIES}"
        )
