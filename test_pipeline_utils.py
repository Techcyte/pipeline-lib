import os
from pipeline_utils import *
import pytest
from typing import Union


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
        )
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
        )
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
        type_check_tasks(last_is_not_none_tasks, last_is_none=True)
    type_check_tasks(last_is_not_none_tasks, last_is_none=False)


def test_last_is_none_task_check():
    last_is_none_tasks = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            print_numbers,
        )
    ]
    with pytest.raises(PipelineTypeError):
        type_check_tasks(last_is_none_tasks, last_is_none=False)
    type_check_tasks(last_is_none_tasks, last_is_none=True)


def test_consts_present_check():
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


def test_consts_missing_check():
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
            }
        ),
        PipelineTask(
            print_numbers,
        )
    ]
    with pytest.raises(PipelineTypeError):
        type_check_tasks(consts_added)


def test_execute():
    tasks = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            group_numbers,
            constants={
                "num_groups": 5
            },
        ),
        PipelineTask(
            sum_numbers,
        ),
        PipelineTask(
            print_numbers,
        )
    ]
    execute(tasks)


def test_yield_results():
    tasks = [
        PipelineTask(
            generate_numbers,
            constants={},
        ),
        PipelineTask(
            group_numbers,
            constants={
                "num_groups": 72
            },
        ),
        PipelineTask(sum_numbers)
    ]
    results = list(yield_results(tasks))
    assert results == [2556, 2494]


class TestExpectedException(ValueError):
    pass


def raise_exception_fn(arg: Iterable[int])->Iterable[int]:
    # start up input generator/process
    i1 = next(iter(arg))
    yield i1
    raise TestExpectedException()


def test_execute_exception():
    tasks = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            raise_exception_fn,
        ),
        PipelineTask(
            print_numbers,
        )
    ]
    with pytest.raises(TestExpectedException):
        execute(tasks)


def sudden_exit_fn(arg: Iterable[int])->Iterable[int]:
    # start up input generator/process
    next(iter(arg))
    # kills process via psutil so that python does not know about it
    proc = psutil.Process(os.getpid())
    proc.kill()


def test_sudden_exit_middle():
    tasks = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            sudden_exit_fn,
        ),
        PipelineTask(
            print_numbers,
        )
    ]
    with pytest.raises(BadTaskExit):
        execute(tasks)


def test_sudden_exit_end():
    tasks = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            sudden_exit_fn,
        )
    ]
    with pytest.raises(BadTaskExit):
        list(yield_results(tasks))



def only_error_if_second_proc(arg: Iterable[int], started_event: mp.Event)->Iterable[int]:
    """
    only exits if it is the first worker process to start up.
    """
    is_second_proc = started_event.is_set()
    started_event.set()
    if is_second_proc:
        raise TestExpectedException()
    else:
        yield from arg


def test_single_worker_error():
    """
    if one process dies and the others do not, then it should still raise an exception, 
    as the dead process might have consumed an important message 
    """
    started_event = mp.Event()
    tasks = [
        PipelineTask(
            generate_numbers,
        ),
        PipelineTask(
            only_error_if_second_proc,
            constants={
                "started_event": started_event,
            },
            num_procs=2
        ),
        PipelineTask(
            print_numbers,
            num_procs=2
        )
    ]
    with pytest.raises(TestExpectedException):
        execute(tasks)


def generate_many()->Iterable[int]:
    yield from range(30000)


def test_many_workers_correctness():
    """
    Tests that many workers working on lots of data 
    eventually returns the correct result, without packet loss or exceptions
    """
    started_event = mp.Event()
    tasks = [
        PipelineTask(
            generate_many,
        ),
        PipelineTask(
            add_const,
            constants={
                "add_val": 5,
            },
            num_procs=4,
        ),
        PipelineTask(
            group_numbers,
            constants={
                "num_groups": 10
            },
            num_procs=4,
        ),
        PipelineTask(
            sum_numbers,
            num_procs=4,
        )
    ]
    actual_result = sum(yield_results(tasks))
    expected_result = 450135000
    assert actual_result == expected_result


def get_worker_pids(inpt: Iterable[int], process_set: set)->Iterable[int]:
    pid = os.getpid()
    for _ in inpt:
        yield pid


def test_many_workers_utilized():
    """
    Tests that many workers working on lots of data 
    actually uses the different processes meaningfully
    """
    n_procs = 5
    process_set = set()
    tasks = [
        PipelineTask(
            generate_many,
        ),
        PipelineTask(
            get_worker_pids,
            constants={
                "process_set": process_set,
            },
            num_procs=n_procs,
        ),
    ]
    assert len(set(yield_results(tasks))) == n_procs


if __name__ == "__main__":
    test_many_workers_utilized()
