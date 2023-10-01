"""
require:
    0: $task1
    1: $/task2
"""
import time


def run(*args, **kwargs):
    time.sleep(1)
    return sum(args)
