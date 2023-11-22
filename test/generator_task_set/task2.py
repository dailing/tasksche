"""
require:
    data: $task1
task_type: generator
iter_args:
    - data
"""
print("___task2 importing___")


def run(data):
    print(f"___task2 running", data)
    for i in data:
        for j in i:
            print("$$$ yield task2  ", j)
            yield j
