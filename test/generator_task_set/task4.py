"""
require:
    data: $task3
task_type: collector
"""

sum_of_square = 0


def run(data: int):
    print(data)
    global sum_of_square
    sum_of_square = data + sum_of_square


def collect():
    global sum_of_square
    return sum_of_square
