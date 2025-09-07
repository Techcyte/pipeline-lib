from typing import List, Literal, Optional, Tuple, get_args

from pipeline_lib.type_checking import sanity_check_mp_params, type_check_tasks

from .mp_execution import execute_mp
from .pipeline_task import PipelineTask
from .seq_execution import execute_seq
from .tr_execution import execute_tr

ParallelismStrategy = Literal["thread", "process-fork", "process-spawn", "coroutine"]

# list of strings in ParallelismStrategy
PARALLELISM_STRATEGIES: Tuple[str, ...] = get_args(ParallelismStrategy)


def execute(
    tasks: List[PipelineTask],
    parallelism: ParallelismStrategy = "thread",
    type_check_pipeline: bool = True,
    inactivity_timeout: Optional[float] = None,
):
    """
    execute tasks until final task completes.
    Raises exception if
    one of the tasks raises an exception or crashes/segfaults in multiprocessing mode.

    if `type_check_pipeline` is specified, then raises an exception
    if tasks are inconsistently typed or untyped (similar to a harsh mypy analysis).

    Also raises an error if no message passing is observed in any task for
    at least `inactivity_timeout` seconds.
    (useful to kill any stuck jobs in a larger distributed system)
    """

    if not tasks:
        return

    sanity_check_mp_params(tasks)

    if type_check_pipeline:
        type_check_tasks(tasks)

    if parallelism == "thread":
        execute_tr(tasks, inactivity_timeout=inactivity_timeout)
    elif parallelism == "process-spawn":
        execute_mp(tasks, "spawn", inactivity_timeout=inactivity_timeout)
    elif parallelism == "process-fork":
        execute_mp(tasks, "fork", inactivity_timeout=inactivity_timeout)
    elif parallelism == "coroutine":
        assert (
            inactivity_timeout is None
        ), "'coroutine' parallelism does not support inactivity timeout, please choose parallelism='process-fork' or parallelism='process-spawn'"
        execute_seq(tasks)
    else:
        raise ValueError(
            f"`execute`'s parallelism argument must be one of {PARALLELISM_STRATEGIES}"
        )
