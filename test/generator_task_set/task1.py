"""
require:
    n_iter: $task0
task_type: generator
"""

print("TASK1 importing")


def run(n_iter):
    print(f"TASK1 running {n_iter}")
    for i in range(n_iter):
        print("$$$ TASK1", i)
        yield i * 2, i * 2 + 1
