"""
require:
    n_iter: $task0
task_type: generator
"""


def run(n_iter):
    print(f"TASK1 running {n_iter}")
    for i in range(n_iter):
        yield i * 2, i * 2 + 1
