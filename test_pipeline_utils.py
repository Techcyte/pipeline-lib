from pipeline_utils import *

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


def test_execute():
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


def test_execute_multiprocessing():
    tasks = [
        PipelineTask(
            generate_numbers,
            multiprocessing=True,
        ),
        PipelineTask(
            group_numbers,
            constants={
                "num_groups": 5
            },
            multiprocessing=False,
        ),
        PipelineTask(
            sum_numbers,
            multiprocessing=True,
        ),
        PipelineTask(
            print_numbers,
            multiprocessing=True,
        )
    ]
    execute(tasks)


class TestExpectedException(ValueError):
    pass


def raise_exception_fn(arg: Iterable[int])->Iterable[int]:
    # start up input generator/process
    i1 = next(iter(arg))
    yield i1
    raise TestExpectedException()


def test_execute_single_process_exception():
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


def test_execute_multi_process_exception():
    tasks = [
        PipelineTask(
            generate_numbers,
            multiprocessing=True,
        ),
        PipelineTask(
            raise_exception_fn,
            multiprocessing=True,
        ),
        PipelineTask(
            print_numbers,
            multiprocessing=True,
        )
    ]
    with pytest.raises(TestExpectedException):
        execute(tasks)


if __name__ == "__main__":
    test_execute_multi_process_exception()
