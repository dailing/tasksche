"""
require:
    0: $task1
    1: $/task2
"""
import time


def run(*args, **kwargs):
    time.sleep(1)
    raise Exception("test")
    return sum(args)
