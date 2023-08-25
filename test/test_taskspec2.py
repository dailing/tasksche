from tasksche.run import Status, TaskSche2, build_exe_graph, task_dict_to_pdf
import os
import unittest
from pathlib import Path


class TestTaskSche(unittest.TestCase):
    @property
    def task_path(self):
        current_file_path = Path(__file__)  # Get the path of the current file
        parent_path = current_file_path.parent / 'test_task_set'  # Get the parent path

        return [
            os.path.join(parent_path, i)
            for i in os.listdir(parent_path) if i != 'task.py']

    @property
    def task_dict(self):
        current_file_path = Path(__file__)  # Get the path of the current file
        parent_path = current_file_path.parent / 'test_task_set'  # Get the parent path
        print(parent_path)
        _, task_dict = build_exe_graph(self.task_path)
        task_dict_to_pdf(task_dict)
        return task_dict

    def test_build_graph(self):
        d = self.task_dict
        self.assertEqual(len(d), 4)
        for k, v in d.items():
            v.dirty = False
            self.assertFalse(v.dirty)
        d['/task1'].dirty = True
        self.assertTrue(d['/task1'].dirty)
        self.assertFalse(d['/task2'].dirty)
        self.assertTrue(d['/task3'].dirty)
        self.assertTrue(d['/'].dirty)

    def test_status(self):
        d = self.task_dict
        self.assertEqual(d['/task1'].status, Status.STATUS_READY)
        self.assertEqual(d['/task2'].status, Status.STATUS_READY)
        self.assertEqual(d['/task3'].status, Status.STATUS_PENDING)
        self.assertEqual(d['/'].status, Status.STATUS_PENDING)

    def test_get_ready(self):
        sche = TaskSche2(self.task_path)
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
        sche.print_status()
        self.assertEqual(ready_task, '/task3')
        self.assertEqual(
            sche.task_dict[ready_task].status, Status.STATUS_RUNNING
        )
        self.assertEqual(sche.task_dict['/'].status, Status.STATUS_PENDING)
        sche.set_finished(ready_task)
        self.assertEqual(
            sche.task_dict[ready_task].status, Status.STATUS_FINISHED
        )
        ready_task = sche.get_ready_set_running()
        self.assertEqual(ready_task, '/')
        self.assertEqual(sche.task_dict['/'].status, Status.STATUS_RUNNING)
        sche.set_finished('/')
        self.assertEqual(sche.task_dict['/'].status, Status.STATUS_FINISHED)
        ready_task = sche.get_ready_set_running()
        self.assertEqual(ready_task, None)




if __name__ == '__main__':
    unittest.main()
