"""
require:
    0: $task1
    1: $/task2
"""

def run(*args, ** kwargs):
    return sum(args)
