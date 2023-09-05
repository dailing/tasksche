"""
require:
    a: 10
"""

import time


def run(a):
    print(a)
    time.sleep(3)
    return a * 10
