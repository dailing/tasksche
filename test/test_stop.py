import asyncio
import time
import unittest
from pathlib import Path

from tasksche.run import (PRunner, TaskScheduler, build_exe_graph,
                          task_dict_to_pdf)


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

    # def test_run_stop_long_run(self):
    #     self.get_task_dict(clear=True)
    #     sche = TaskScheduler(self.task_path, PRunner)
    #     sche.run(once=False, daemon=True)
    #     self.assertIsNotNone(sche.main_loop_task)
    #     time.sleep(0.5)
    #     self.assertTrue(sche.main_loop_task.is_alive())
    #     sche.stop()
    #     self.assertIsNone(sche.main_loop_task)
    #     # self.assertIsNone(sche._task_event_queue)
    #
    #     sche.run(once=False, daemon=True)
    #     self.assertIsNotNone(sche.main_loop_task)
    #     self.assertTrue(sche.main_loop_task.is_alive())
    #     time.sleep(0.5)
    #     # self.assertTrue(sche.main_loop_task.is_alive())
    #     sche.stop()
    #     self.assertIsNone(sche.main_loop_task)
    #     # self.assertIsNone(sche._task_event_queue)

    def test_run_stop_once(self):
        self.get_task_dict(clear=True)
        loop = asyncio.new_event_loop()
        sche = TaskScheduler(self.task_path, loop)
        sche.run(once=True, daemon=True)

        async def stop():
            await asyncio.sleep(0.1)
            await sche.stop()

        loop.run_until_complete(stop())


if __name__ == '__main__':
    unittest.main()
