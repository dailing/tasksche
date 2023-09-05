import unittest
from pathlib import Path

from tasksche.run import (
    Runner, Status, TaskScheduler,
    build_exe_graph, task_dict_to_pdf, DumpedTypeOperation)


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

    def test_build_graph(self):
        d = self.get_task_dict()
        for k, v in d.items():
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

    def test_get_ready(self):
        sche = TaskScheduler(self.task_path, None)
        ready_task = sche.get_ready_set_running()
        self.assertIn(ready_task, ['/task1', '/task2'])
        self.assertEqual(
            sche.task_dict[ready_task].status,
            Status.STATUS_RUNNING)
        sche.set_finished(ready_task)
        self.assertEqual(
            sche.task_dict[ready_task].status, Status.STATUS_FINISHED)
        ready_task = sche.get_ready_set_running()
        self.assertIn(ready_task, ['/task1', '/task2'])
        self.assertEqual(
            sche.task_dict[ready_task].status,
            Status.STATUS_RUNNING
        )
        sche.set_finished(ready_task)
        self.assertEqual(
            sche.task_dict[ready_task].status, Status.STATUS_FINISHED)
        ready_task = sche.get_ready_set_running()
        self.assertEqual(ready_task, '/task3')
        self.assertEqual(
            sche.task_dict[ready_task].status, Status.STATUS_RUNNING
        )
        self.assertEqual(sche.task_dict['/task'].status, Status.STATUS_PENDING)
        sche.set_finished(ready_task)
        self.assertEqual(
            sche.task_dict[ready_task].status, Status.STATUS_FINISHED
        )
        ready_task = sche.get_ready_set_running()
        self.assertEqual(ready_task, '/task')
        self.assertEqual(sche.task_dict['/task'].status, Status.STATUS_RUNNING)
        sche.set_finished('/task')
        self.assertEqual(sche.task_dict['/task'].status, Status.STATUS_FINISHED)
        ready_task = sche.get_ready_set_running()
        self.assertEqual(ready_task, '_END_')

    def test_run_basic_runner(self):
        self.get_task_dict(clear=True)
        sche = TaskScheduler(self.task_path, Runner)
        sche.run()
        sche = TaskScheduler(self.task_path, Runner)
        self.assertFalse(sche.task_dict['/task1'].dirty)
        self.assertEqual(sche.task_dict['/task1'].status, Status.STATUS_FINISHED)
        sche.run()

    def test_enum_property(self):
        self.assertFalse(DumpedTypeOperation.DELETE == "DELETE")


if __name__ == '__main__':
    unittest.main()
