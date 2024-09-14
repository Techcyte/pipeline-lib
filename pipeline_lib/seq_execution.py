import warnings
from typing import List

from .pipeline_task import (
    DEFAULT_BUF_SIZE,
    DEFAULT_NUM_WORKERS,
    DEFAULT_PACKETS_IN_FLIGHT,
    PipelineTask,
)


def _warn_parameter_overrides(tasks: List[PipelineTask]):
    for task in tasks:
        if (
            task.max_message_size != DEFAULT_BUF_SIZE
            or task.num_workers != DEFAULT_NUM_WORKERS
            or task.packets_in_flight != DEFAULT_PACKETS_IN_FLIGHT
        ):
            warnings.warn(
                f"Task '{task.name}' overrode default value of max_message_size, num_workers, or packets_in_flight, and this override is ignored by 'coroutine' parallelism strategy."
            )


def execute_seq(tasks: List[PipelineTask]):
    # pylint: disable=too-many-branches,too-many-locals
    """
    execute tasks until final task completes.
    Raises error if tasks are inconsistently specified or if
    one of the tasks raises an error.
    """
    if not tasks:
        return

    _warn_parameter_overrides(tasks)

    cur_result = tasks[0].generator(**tasks[0].constants_dict)
    for task in tasks[1:]:
        cur_result = task.generator(cur_result, **task.constants_dict)
