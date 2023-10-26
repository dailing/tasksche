"""
require:
    data: $task1
task_type: generator
"""
print("___task2 importing___")


def run(data):
    print(f"___task2 running")
    for i in data:
        yield i
