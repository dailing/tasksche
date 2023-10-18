from ..callback import CallbackBase, CallBackEvent, InvokeSignal
from ..functional import execute_task
from ..util import call_back_event_to_runner_task


class LocalRunner(CallbackBase):
    def __init__(self):
        super().__init__()

    def on_task_start(self, event: CallBackEvent):
        spec = call_back_event_to_runner_task(event)
        execute_task(spec)
        return InvokeSignal('on_task_finish', event)
