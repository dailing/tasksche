"""
require:
    data: $task3
iter_args:
    - data
"""

from typing import Iterable


def run(data: Iterable[int]):
    sum = 0
    for i in data:
        print(f"$$$ TASK4:: {i}")
        sum += i
    print("SUM VALUE IS :", sum)
    return sum

