"""
require:
    a: 10
"""

import time


def run(a):
    print(a)
    time.sleep(2)
    return a * 12
