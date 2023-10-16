import unittest
from pathlib import Path

from tasksche.run import (
    Status, TaskScheduler,
    build_exe_graph)


class TestTaskSche(unittest.IsolatedAsyncioTestCase):
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
        return task_dict

    def test_build_graph(self):
        d = self.get_task_dict()
        for k, v in d.items():
            if k == '_END_':
                continue
            v.dirty = False
            self.assertFalse(v.dirty)
        d['/task1'].dirty = True
        self.assertTrue(d['/task1'].dirty)
        self.assertFalse(d['/task2'].dirty)
        self.assertTrue(d['/task3'].dirty)
        self.assertTrue(d['/task'].dirty)

    def test_status(self):
        d = self.get_task_dict()
        self.assertEqual(d['/task1'].status, Status.STATUS_READY)
        self.assertEqual(d['/task2'].status, Status.STATUS_READY)
        self.assertEqual(d['/task3'].status, Status.STATUS_PENDING)
        self.assertEqual(d['/task'].status, Status.STATUS_PENDING)

    def test_run_basic_runner(self):
        self.get_task_dict(clear=True)
        sche = TaskScheduler(self.task_path)
        sche.run(once=True)
        sche = TaskScheduler(self.task_path)
        self.assertFalse(sche.task_dict['/task1'].dirty)
        self.assertEqual(
            sche.task_dict['/task1'].status, Status.STATUS_FINISHED)
        sche.run(once=True)


if __name__ == '__main__':
    unittest.main()
