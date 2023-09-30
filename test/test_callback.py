import unittest

from tasksche.callback import CallbackBase, CallbackRunner, call_back_pipe
from tasksche.common import Status
from tasksche.task_spec import TaskSpec


class TestCallBackCollection(unittest.TestCase):
    def test_status_change(self):
        # Create a mock callback
        class CB1(CallbackBase):
            def __init__(self):
                self.status = None

            def before_status_change(
                    self, task: TaskSpec, old_status: Status, new_status: Status):
                self.status = new_status
                return call_back_pipe(new_status=Status.STATUS_ERROR)

        cb1 = CB1()
        cb2 = CB1()
        runner = CallbackRunner([cb1, cb2])
        retval = runner.before_status_change(
            task=None,
            old_status=Status.STATUS_FINISHED,
            new_status=Status.STATUS_PENDING)
        self.assertEqual(cb1.status, Status.STATUS_PENDING)
        self.assertEqual(retval['new_status'], Status.STATUS_ERROR)
        self.assertEqual(cb2.status, Status.STATUS_ERROR)


if __name__ == '__main__':
    unittest.main()
