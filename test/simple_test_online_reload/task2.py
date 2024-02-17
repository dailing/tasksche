"""
require:
    infinite: 1
    a: $task1
"""

import time


def run(infinite, a):
    with open("output.txt", "w") as f:
        f.write(str(a))
        f.write("\n")
    while infinite:
        time.sleep(100)
    return a + 2000
