"""

"""

from dataclasses import dataclass
from typing import Callable, Type, Optional, Dict, Any, List, Iterable, Union
from multiprocessing import Queue
import inspect
import typing
import pytest

@dataclass
class PipelineTask:
    generator: Callable
    constants: Optional[Dict[str, Any]]=None

    @property
    def name(self):
        return self.generator.__name__


def generate_numbers()->Iterable[int]:
    for i in range(101):
        yield i

def group_numbers(int_iterator: Iterable[int], num_groups: int)->Iterable[List[int]]:
    assert num_groups > 0
    cur_nums = []
    for num in int_iterator:
        cur_nums.append(num)
        if len(cur_nums) == num_groups:
            yield cur_nums
            cur_nums = []
    if cur_nums:
        yield cur_nums

def sum_numbers(group_iterator: Iterable[List[int]])->Iterable[int]:
    for nums in group_iterator:
        yield sum(nums)

def add_const(int_iter: Iterable[int], add_val: int)->Iterable[int]:
    for i in int_iter:
        yield i + add_val

def print_numbers(num_iterator: Iterable[int])->None:
    for n  in num_iterator:
        print(n)

# nums = generate_numbers(None)
# num_groups = group_numbers(nums, 5)
# sums = sum_numbers(num_groups)
# print_numbers(sums)


class PipelineTypeError(RuntimeError):
    pass


def type_error_if(condition, message):
    if not condition:
        raise PipelineTypeError(message)


@dataclass 
class PipelineTaskType:
    input_type: Type
    output_type: Type
    other_names: List[str]


def is_iterable(type: Type):
    return typing.get_origin(Iterable[str]) is typing.get_origin(Iterable)


def get_func_args(func):
    arguments = inspect.getfullargspec(func)
    type_error_if(arguments.varargs is None, "varargs not supported")
    type_error_if(arguments.varkw is None, "varkw not supported")
    type_error_if(arguments.defaults is None, "default arguments not supported")
    type_error_if(arguments.kwonlydefaults is None, "default arguments not supported")
    type_error_if(set(arguments.args + arguments.kwonlyargs).issubset(arguments.annotations), "all arguments must have annotations")
    type_error_if('return' in arguments.annotations, "function return type must have type annotation")
    
    base_input_type = None if not arguments.args else arguments.annotations[arguments.args[0]]
    base_return_type = arguments.annotations['return']

    type_error_if(base_input_type is None or (is_iterable(base_input_type) and len(typing.get_args(base_input_type)) == 1), "First argument must be an Iterable[input_type], if defined")
    type_error_if(base_return_type is None or (is_iterable(base_return_type) and len(typing.get_args(base_return_type)) == 1), "Return type annotation must be an Iterable[input_type] or None")

    input_type = None if base_input_type is None else typing.get_args(base_input_type)[0]
    return_type = None if base_return_type is None else typing.get_args(base_return_type)[0]

    # these are guarentteed to be mutually exclusive
    other_argument_names = arguments.args + arguments.kwonlyargs
    # remove input argument
    if arguments.args:
        other_argument_names.remove(arguments.args[0])

    return input_type, return_type, other_argument_names


def test_get_func_args():
    assert get_func_args(generate_numbers) == (None, int, [])
    assert get_func_args(group_numbers) == (int, List[int], ['num_groups'])
    assert get_func_args(print_numbers) == (int, None, [])
    # test kw only arguments
    def kwarg_func(x: Iterable[int], arg2: float, *, arg3: str)->Iterable[str]:
        pass
    assert get_func_args(kwarg_func) == (int, str, ['arg2', 'arg3'])

    # errors
    with pytest.raises(PipelineTypeError):
        def no_return_type():
            pass
        get_func_args(no_return_type)

    with pytest.raises(PipelineTypeError):
        def first_argument_not_iterable(x: Union[str, float])->Iterable[str]:
            pass
        get_func_args(first_argument_not_iterable)

    with pytest.raises(PipelineTypeError):
        def return_not_iterable(x: Iterable[str])->Optional[str]:
            pass
        get_func_args(return_not_iterable)



def test_is_iterable():
    assert is_iterable(Iterable[str])
    assert not is_iterable(Optional[str])
    assert not is_iterable(float)


def type_check_tasks(tasks: List[PipelineTask]):
    prev_type = None
    for task in tasks:
        input_type, return_type, other_args = get_func_args(task.generator)
        if prev_type != input_type:
            raise PipelineTypeError(f"In task {task.name}, expected input {input_type}, received input {prev_type}.")   

        task_consts = {} if task.constants is None else task.constants
        task_const_names = list(task_consts.keys())
        if set(task_consts) != set(other_args):
            raise PipelineTypeError(f"In task {task.name}, expected constants {other_args}, received constants {task_const_names}.")    

        prev_type = return_type
    if prev_type is not None:
        raise PipelineTypeError(f"Task {task.name} is the final task and must have output_type=None.")    

def test_type_checks_valid():
    tasks = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            group_numbers,
            constants={
                "num_groups": 5
            }
        ),
        PipelineTask(
            sum_numbers,
        ),
        PipelineTask(
            print_numbers,
        )
    ]
    type_check_tasks(tasks)
    mismatched_tasks = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            sum_numbers,
        ),
        PipelineTask(
            print_numbers,
        )
    ]
    with pytest.raises(PipelineTypeError):
        type_check_tasks(mismatched_tasks)
    needed_none_start = [
        PipelineTask(
            print_numbers,
        )
    ]
    with pytest.raises(PipelineTypeError):
        type_check_tasks(needed_none_start)
    needs_none_end = [
        PipelineTask(
            generate_numbers,
        ),
    ]
    with pytest.raises(PipelineTypeError):
        type_check_tasks(needs_none_end)

    consts_present = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            add_const,
            constants={
                "add_val": 5,
            }
        ),
        PipelineTask(
            print_numbers,
        )
    ]
    type_check_tasks(consts_present)

    consts_missing = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            add_const,
            constants={}
        ),
        PipelineTask(
            print_numbers,
        )
    ]
    with pytest.raises(PipelineTypeError):
        type_check_tasks(consts_missing)

    consts_added = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            add_const,
            constants={
                "add_val": 5,
                "extra_val": 6,
            }
        ),
        PipelineTask(
            print_numbers,
        )
    ]
    with pytest.raises(PipelineTypeError):
        type_check_tasks(consts_added)


def execute(tasks: List[PipelineTask]):
    """
    execute tasks until final task completes. Garbage collector will 
    clean up remainder of generators by raising a error
    """
    if not tasks:
        return
    type_check_tasks(tasks)
    # type checking done at this point, now don't assume types all there

    iterator = None
    for task in tasks:
        other_args = task.constants if task.constants else {}
        if iterator is None:
            iterator = task.generator(**other_args)
        else:    
            iterator = task.generator(iterator, **other_args)


def task_execute():
    tasks = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            group_numbers,
            constants={
                "num_groups": 5
            }
        ),
        PipelineTask(
            sum_numbers,
        ),
        PipelineTask(
            print_numbers,
        )
    ]
    execute(tasks)


task_execute()

