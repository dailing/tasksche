"""
require:
    a: 10
"""

import time


def run(a):
    print(a)
    time.sleep(1)
    return a * 10
