"""
require:
    0: $task1
    1: $/task2
"""


def run(*args, **kwargs):
    raise Exception("test")
    return sum(args)
