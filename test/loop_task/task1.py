"""
require:
    max_sum: $task0
    result: $task2
task_type: generator
iter_args:
    - result
"""
import sys

print("TASK1 importing")


def run(max_sum, result):
    yield 1
    yield 1
    i1 = None
    for i in result:
        print(f"TASK 1 get{i}")
        if i > max_sum:
            break
        if i1 is None:
            i1 = i
        else:
            yield i1 + i
            i1 = i
