import unittest
from pathlib import Path

from tasksche.run import (
    Runner, TaskScheduler,
    build_exe_graph, task_dict_to_pdf, DumpedTypeOperation)


class TestTaskSche(unittest.TestCase):
    @property
    def task_path(self):
        current_file_path = Path(__file__)
        parent_path = (
                current_file_path.parent /
                'task_set_inherent_tasks' /
                'task3.py')
        return [parent_path]

    def get_task_dict(self, clear=True):
        _, task_dict = build_exe_graph(self.task_path)
        if clear:
            for v in task_dict.values():
                v.clear()
        task_dict_to_pdf(task_dict)
        return task_dict

    def test_run_basic_runner(self):
        self.get_task_dict(clear=True)
        sche = TaskScheduler(self.task_path)
        print(sche.task_dict['/task3']._cfg_dict)
        print(sche.task_dict['/task3'].dependent_hash)
        sche.run(once=True)

    def test_enum_property(self):
        self.assertFalse(DumpedTypeOperation.DELETE == "DELETE")


if __name__ == '__main__':
    unittest.main()
