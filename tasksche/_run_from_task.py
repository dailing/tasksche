import inspect
from .run import TaskSche2


def get_caller_file_path(n=2):
    frame = inspect.currentframe()
    for _ in range(n):
        frame = frame.f_back
    caller_file_path = frame.f_code.co_filename
    return caller_file_path


# Example usage
def run_task():
    task_path = get_caller_file_path(2)
    sche = TaskSche2([task_path])
    sche.run()
