"""
require:
    data: $task3
"""

from tasksche import path_for


def run(data):
    with open('output.txt', 'w') as f:
        f.write(str(data))
        f.write('\n')
        f.write(path_for('task3'))