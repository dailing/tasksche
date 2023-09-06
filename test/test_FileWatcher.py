import os.path
import shutil
import unittest
from multiprocessing import Queue

from tasksche.run import FileWatcher, SchedulerEvent, EventType

test_dir = '___temp_test___'


class MyTestCase(unittest.TestCase):

    def test_something(self):
        self.assertTrue(self.queue.empty())
        with open(os.path.join(test_dir, 'test.txt'), 'w') as f:
            f.write('test')
        e: SchedulerEvent = self.queue.get(timeout=1)
        self.assertEqual(e.event_type, EventType.FILE_CHANGE)
        self.assertTrue(e.task_name in os.path.abspath(os.path.join(test_dir, 'test.txt')))

    def tearDown(self) -> None:
        self.watcher.stop()

    def setUp(self) -> None:
        self.queue = Queue()
        if os.path.exists(test_dir):
            shutil.rmtree(test_dir)
        os.mkdir(test_dir)
        self.watcher = FileWatcher(test_dir, self.queue).start()


if __name__ == '__main__':
    unittest.main()
