from typing import Iterable, List


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

