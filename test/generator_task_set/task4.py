"""
require:
    data: $task3
iter_args:
    - data
"""

from typing import Iterable


def run(data: Iterable[int]):
    for i in data:
        print(f"TASK4:: {i}")
