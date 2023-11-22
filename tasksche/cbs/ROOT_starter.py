from ..callback import CallbackBase, CallBackEvent, InterruptSignal
from ..functional import ROOT_NODE


class RootStarter(CallbackBase):
    def on_task_start(self, event: CallBackEvent):
        if event.task_name != ROOT_NODE.NAME:
            return
        raise InterruptSignal('on_task_finish', event)
