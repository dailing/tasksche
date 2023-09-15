import unittest
# from tasksche.run import _CallBackCollection, TaskEventType, TaskEvent, TaskSpec, SchedulerCallbackBase


# class TestCallBackCollection(unittest.TestCase):
#     def test_status_change(self):
#         # Create a mock callback
#         class MockCallback(SchedulerCallbackBase):
#             def __init__(self):
#                 self.status_change_called = False

#             def status_change(self, event: TaskEvent, task_spec: TaskSpec):
#                 self.status_change_called = True

#         # Create an instance of _CallBackCollection with the mock callback
#         callback = MockCallback()
#         collection = _CallBackCollection([callback])

#         # Call the status_change method with a mock event and task_spec
#         event = TaskEvent(TaskEventType.TASK_FINISHED, 'test', None)
#         task_spec = TaskSpec('', 'test')
#         collection.status_change(event, task_spec)

#         # Assert that the status_change method of the mock callback is called
#         self.assertTrue(callback.status_change_called)


if __name__ == '__main__':
    unittest.main()
