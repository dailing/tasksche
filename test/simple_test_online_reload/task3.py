"""
require:
    a: $task2
"""


def run(a):
    with open("output.txt", "w") as f:
        f.write(str(a))
        f.write("\n")
    print("test")
    return a
