"""
require:
    a: 1
    b: 2
    c: 3
    d: 4
"""


def run(a, b, c, d):
    return sum((a, b, c, d))


if __name__ == '__main__':
    from tasksche import run_task
    run_task()
