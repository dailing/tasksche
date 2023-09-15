import os.path
import shutil
import time
import unittest
from asyncio import Queue
import asyncio

from tasksche.run import FileWatcher, TaskEvent, TaskEventType

test_dir = '___temp_test___'


class MyTestCase(unittest.TestCase):

    async def _test_body(self):
        self.queue = Queue()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self.watcher = FileWatcher(
            test_dir,
            self.queue,
        )
        async with self.watcher:
            await asyncio.sleep(0.5)
            self.assertTrue(self.queue.empty())
            with open(os.path.join(test_dir, 'test.txt'), 'w') as f:
                f.write('test')
            e: TaskEvent = await asyncio.wait_for(self.queue.get(), 1)
            self.assertEqual(e.event_type, TaskEventType.FILE_CHANGE)
            self.assertTrue(e.task_name in os.path.abspath(
                os.path.join(test_dir, 'test.txt')))

    def test_FillerWatcher(self):
        asyncio.run(self._test_body())

    def tearDown(self) -> None:
        # print('tearDown')
        shutil.rmtree(test_dir)

    def setUp(self) -> None:
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)
        os.mkdir(test_dir)
        time.sleep(0.1)


if __name__ == '__main__':
    unittest.main()
