"""
require:
    a: 10
"""

import time
import random


def run(a):
    print(a)
    time.sleep(1.5)
    rnd = random.randint(0, 100)
    print(rnd)
    return a + rnd
