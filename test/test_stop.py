import asyncio
import unittest
from pathlib import Path

from tasksche.logger import Logger
from tasksche.run import (SchedulerEvent, Status, TaskScheduler, build_exe_graph,
                          _INVALIDATE)

logger = Logger()


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
        # task_dict_to_pdf(task_dict)
        return task_dict

    async def test_run_stop_long_run(self):
        self.get_task_dict(clear=True)
        sche = TaskScheduler(self.task_path)
        sche.run(once=False, daemon=True)
        self.assertIsNotNone(sche.main_loop_task)
        await asyncio.sleep(0.5)
        await sche.stop()
        self.assertIsNone(sche.main_loop_task)
        # self.assertIsNone(sche._task_event_queue)

        sche.run(once=False, daemon=True)
        self.assertIsNotNone(sche.main_loop_task)
        await asyncio.sleep(0.5)
        # self.assertTrue(sche.main_loop_task.is_alive())
        await sche.stop()
        self.assertIsNone(sche.main_loop_task)
        # self.assertIsNone(sche._task_event_queue)

    async def test_run_stop_once(self):
        self.get_task_dict(clear=True)
        sche = TaskScheduler(self.task_path)
        sche.run(once=True, daemon=True)
        await asyncio.sleep(0.1)
        await sche.stop()

    async def test_clear(self):
        self.get_task_dict(clear=True)
        task = str(self.task_path[0]).replace('/task.py', '/task.py')
        logger.info(task)
        logger.info(type(task))
        sche = TaskScheduler(
            [task])
        await sche._run(once=True)
        self.assertNotEqual(
            sche.task_dict['/task1']._exec_info_dump, _INVALIDATE)
        sche.task_dict['/task1'].clear()
        self.assertEqual(sche.task_dict['/task1']._exec_info_dump, _INVALIDATE)
        sche.task_dict['/task'].clear()
        self.assertEqual(sche.task_dict['/task']._exec_info_dump, _INVALIDATE)

    async def test_random_result_rerun(self):
        self.get_task_dict(clear=True)
        sche = TaskScheduler(self.task_path)
        await sche._run(once=True)
        self.assertFalse(sche.task_dict['/task1'].dirty)
        self.assertFalse(sche.task_dict['/task'].dirty)
        sche.task_dict['/task1'].clear()
        self.assertTrue(sche.task_dict['/task1'].dirty)
        self.assertTrue(sche.task_dict['/task'].dirty)
        self.assertEqual(
            sche.task_dict['/task1'].status, Status.STATUS_READY)
        self.assertEqual(
            sche.task_dict['/task2'].status, Status.STATUS_FINISHED)
        self.assertEqual(
            sche.task_dict['/task'].status, Status.STATUS_PENDING)
        while not sche.sche_event_queue.empty():
            logger.debug(await sche.sche_event_queue.get())
        await sche._run(once=True)
        output_event = set()
        while not sche.sche_event_queue.empty():
            evt: SchedulerEvent = await sche.sche_event_queue.get()
            output_event.add(evt.msg)
        self.assertIn(('/task', Status.STATUS_FINISHED), output_event)
        self.assertIn(('/task1', Status.STATUS_FINISHED), output_event)
        self.assertIn(('/task3', Status.STATUS_FINISHED), output_event)


if __name__ == '__main__':
    unittest.main()
