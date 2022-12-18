import inspect
import multiprocessing as mp
import typing
import warnings
from typing import Iterable, List, Type

from .pipeline_task import PipelineTask


class PipelineTypeError(RuntimeError):
    pass


def _type_error_if(condition, message):
    if not condition:
        raise PipelineTypeError(message)


def is_iterable(type: Type):
    return typing.get_origin(type) is typing.get_origin(Iterable)


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
        "Return type annotation must be an Iterable[input_type] or None",
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

        if task_idx == 0 and task.num_procs != 1:
            raise PipelineTypeError(
                f"Only supports 1 process for the first task in a pipeline, "
                "due to difficulties reasoning about exit conditions, but "
                " {task.num_procs} processes requested. "
            )

        if task_idx != len(tasks) - 1 and return_type is None:
            raise PipelineTypeError(
                f"None return type only allowed in final task of pipe"
            )

        task_consts = {} if task.constants is None else task.constants
        task_const_names = list(task_consts.keys())
        if set(task_consts) != set(other_args):
            raise PipelineTypeError(
                f"In task {task.name}, expected constants {other_args}, received constants {task_const_names}."
            )

        _sanity_check_mp_params(task)

        prev_type = return_type

    if prev_type is not None:
        raise PipelineTypeError(
            f"In final task {task.name}, expected output type None, actual type {prev_type}."
        )


def _sanity_check_mp_params(task: PipelineTask):
    if task.num_procs <= 0:
        raise PipelineTypeError(
            f"In task {task.name}, num_procs value {task.num_procs} needs to be positive"
        )

    if task.num_procs > mp.cpu_count():
        warnings.warn(
            f"In task {task.name}, num_procs value {task.num_procs} was greater than number of cpus on machine {mp.cpu_count()}"
        )

    if task.out_buffer_size <= 0:
        raise PipelineTypeError(
            f"In task {task.name}, out_buffer_size {task.num_procs} needs to be positive"
        )
