import unittest
from pathlib import Path

from tasksche.run import (
    Status, TaskScheduler, PRunner,
    build_exe_graph, task_dict_to_pdf)


class TestTaskSche(unittest.TestCase):
    @property
    def task_path(self):
        current_file_path = Path(__file__)
        parent_path = current_file_path.parent / 'simple_task_set' / 'task.py'
        return [parent_path]

    def get_task_dict(self, clear=True):
        _, task_dict = build_exe_graph(self.task_path)
        if clear:
            for v in task_dict.values():
                v.clear()
        task_dict_to_pdf(task_dict)
        return task_dict

    def test_run_basic_Prunner(self):
        self.get_task_dict(clear=True)
        sche = TaskScheduler(self.task_path, PRunner)
        sche.run(once=True)
        sche = TaskScheduler(self.task_path, PRunner)
        self.assertFalse(sche.task_dict['/task1'].dirty)
        self.assertEqual(sche.task_dict['/task1'].status, Status.STATUS_FINISHED)
        sche.run(once=True)


if __name__ == '__main__':
    unittest.main()
