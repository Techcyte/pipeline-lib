import inspect
import typing
from typing import Iterable, List, Type

from .pipeline_task import PipelineTask

MAX_NUM_WORKERS = 128


class PipelineTypeError(RuntimeError):
    pass


def _type_error_if(condition, message):
    if not condition:
        raise PipelineTypeError(message)


def is_iterable(type_: Type):
    return typing.get_origin(type_) is typing.get_origin(Iterable)


def get_func_args(func, extract_first=True):
    arguments = inspect.getfullargspec(func)
    _type_error_if(arguments.varargs is None, "varargs not supported")
    _type_error_if(arguments.varkw is None, "varkw not supported")
    _type_error_if(arguments.defaults is None, "default arguments not supported")
    _type_error_if(arguments.kwonlydefaults is None, "default arguments not supported")
    _type_error_if(
        set(arguments.args + arguments.kwonlyargs).issubset(arguments.annotations),
        "all arguments must have annotations",
    )
    _type_error_if(
        "return" in arguments.annotations,
        "function return type must have type annotation, if not returning, please specify ->None",
    )

    base_input_type = (
        None
        if not arguments.args or not extract_first
        else arguments.annotations[arguments.args[0]]
    )
    base_return_type = arguments.annotations["return"]

    _type_error_if(
        base_input_type is None
        or (
            is_iterable(base_input_type) and len(typing.get_args(base_input_type)) == 1
        ),
        "First argument must be an Iterable[input_type], if defined",
    )
    _type_error_if(
        base_return_type is None
        or (
            is_iterable(base_return_type)
            and len(typing.get_args(base_return_type)) == 1
        ),
        f"Return type annotation must be an Iterable[input_type] or None, was {base_return_type}",
    )

    input_type = (
        None if base_input_type is None else typing.get_args(base_input_type)[0]
    )
    return_type = (
        None if base_return_type is None else typing.get_args(base_return_type)[0]
    )

    # these are guarentteed to be mutually exclusive
    other_argument_names = arguments.args + arguments.kwonlyargs
    # remove input argument
    if arguments.args and extract_first:
        other_argument_names.remove(arguments.args[0])

    return input_type, return_type, other_argument_names


def type_check_tasks(tasks: List[PipelineTask]):
    prev_type = None
    for task_idx, task in enumerate(tasks):
        input_type, return_type, other_args = get_func_args(
            task.generator, extract_first=(task_idx != 0)
        )
        if prev_type != input_type:
            raise PipelineTypeError(
                f"In task {task.name}, expected input {input_type}, received input {prev_type}."
            )

        if task_idx != len(tasks) - 1 and return_type is None:
            raise PipelineTypeError(
                "None return type not allowed in any task except final task of pipe"
            )

        task_consts = {} if task.constants is None else task.constants
        task_const_names = list(task_consts.keys())
        if set(task_consts) != set(other_args):
            raise PipelineTypeError(
                f"In task {task.name}, expected constants {other_args}, received constants {task_const_names}."
            )

        prev_type = return_type

    if prev_type is not None:
        raise PipelineTypeError(
            f"In final task {tasks[-1].name}, expected output type None, actual type {prev_type}."
        )


def sanity_check_mp_params(tasks: List[PipelineTask]):
    for task in tasks:
        _sanity_check_mp_params(task)


def _sanity_check_mp_params(task: PipelineTask):
    if task.num_workers <= 0:
        raise PipelineTypeError(
            f"In task {task.name}, num_workers value {task.num_workers} needs to be positive"
        )

    if task.num_workers > MAX_NUM_WORKERS:
        raise PipelineTypeError(
            f"In task {task.name}, num_workers value {task.num_workers} was greater than hard limit {MAX_NUM_WORKERS} for number of threads per task"
        )

    if task.num_workers > task.packets_in_flight:
        raise PipelineTypeError(
            f"In task {task.name}, packets_in_flight {task.packets_in_flight} is less than num_workers {task.num_workers}, which can lead to deadlocks."
        )

    if task.shared_buffer and task.max_message_size is None:
        raise PipelineTypeError(
            f"In task {task.name}, shared_buffer={task.shared_buffer}, but max_message_size is None, which has no effect, as shared_buffer only operates when the max_message_size is set"
        )
