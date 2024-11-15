from typing import Dict, Optional, Union

import pytest

from pipeline_lib.pipeline_task import PipelineTask
from pipeline_lib.type_checking import (
    PipelineTypeError,
    get_func_args,
    is_iterable,
    type_check_tasks,
)

from .example_funcs import *


def test_get_func_args():
    assert get_func_args(generate_numbers) == (None, int, [])
    assert get_func_args(group_numbers) == (int, List[int], ["num_groups"])
    assert get_func_args(print_numbers) == (int, None, [])

    # test kw only arguments
    def kwarg_func(x: Iterable[int], arg2: float, *, arg3: str) -> Iterable[str]:
        pass

    assert get_func_args(kwarg_func) == (int, str, ["arg2", "arg3"])
    assert get_func_args(kwarg_func, extract_first=False) == (
        None,
        str,
        ["x", "arg2", "arg3"],
    )

    # errors
    with pytest.raises(PipelineTypeError):

        def no_return_type():
            pass

        get_func_args(no_return_type)

    with pytest.raises(PipelineTypeError):

        def first_argument_not_iterable(x: Union[str, float]) -> Iterable[str]:
            pass

        get_func_args(first_argument_not_iterable)

    with pytest.raises(PipelineTypeError):

        def return_not_iterable(x: Iterable[str]) -> Optional[str]:
            pass

        get_func_args(return_not_iterable)


def test_is_iterable():
    assert is_iterable(Iterable[str])
    assert not is_iterable(Optional[str])
    assert not is_iterable(float)


def test_type_checks_valid():
    tasks = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(group_numbers, constants={"num_groups": 5}),
        PipelineTask(
            sum_numbers,
        ),
        PipelineTask(
            print_numbers,
        ),
    ]
    type_check_tasks(tasks)


def test_mismatched_task_types():
    mismatched_tasks = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            sum_numbers,
        ),
        PipelineTask(
            print_numbers,
        ),
    ]
    with pytest.raises(PipelineTypeError):
        type_check_tasks(mismatched_tasks)


def test_needed_none_start():
    needed_none_start = [
        PipelineTask(
            print_numbers,
        )
    ]
    with pytest.raises(PipelineTypeError):
        type_check_tasks(needed_none_start)


def start_with_consts(beginning: Dict[int, str]) -> Iterable[int]:
    pass


def sink_ints(vals: Iterable[int]) -> None:
    list(vals)


def test_start_with_consts():
    # check that first function can accept constant arguments in its first argument
    start_with_consts_t = [
        PipelineTask(start_with_consts, constants={"beginning": {1: "bob"}}),
        PipelineTask(sink_ints),
    ]
    type_check_tasks(start_with_consts_t)


def test_non_in_middle():
    none_in_middle = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            print_numbers,
        ),
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            print_numbers,
        ),
    ]
    with pytest.raises(PipelineTypeError):
        type_check_tasks(none_in_middle)


def test_last_is_not_none_task_check():
    last_is_not_none_tasks = [
        PipelineTask(
            generate_numbers,
        )
    ]
    with pytest.raises(PipelineTypeError):
        type_check_tasks(last_is_not_none_tasks)


def test_last_is_none_task_check():
    last_is_none_tasks = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            print_numbers,
        ),
    ]
    type_check_tasks(last_is_none_tasks)


def test_consts_present_check():
    consts_present = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            add_const,
            constants={
                "add_val": 5,
            },
        ),
        PipelineTask(
            print_numbers,
        ),
    ]
    type_check_tasks(consts_present)


def test_consts_missing_check():
    consts_missing = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(add_const, constants={}),
        PipelineTask(
            print_numbers,
        ),
    ]
    with pytest.raises(PipelineTypeError):
        type_check_tasks(consts_missing)


def test_consts_added_check():
    consts_added = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            add_const,
            constants={
                "add_val": 5,
                "extra_val": 6,
            },
        ),
        PipelineTask(
            print_numbers,
        ),
    ]
    with pytest.raises(PipelineTypeError):
        type_check_tasks(consts_added)
