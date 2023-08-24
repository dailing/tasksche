from tasksche.run import TaskSpec2, build_exe_graph, task_dict_to_pdf
import os
import unittest
from pathlib import Path


class TestTaskSche(unittest.TestCase):
    @property
    def task_dict(self):

        current_file_path = Path(__file__)  # Get the path of the current file
        parent_path = current_file_path.parent / 'test_task_set' # Get the parent path
        print(parent_path)
        _, task_dict = build_exe_graph([os.path.join(
            parent_path, i) for i in os.listdir(parent_path) if i != 'task.py'])
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

if __name__ == '__main__':
    unittest.main()
