"""
require:
    a: 10
"""

import time
import random


def run(a):
    print(a)
    time.sleep(2)
    rnd = random.randint(0, 100)
    print(rnd)
    return a + rnd
