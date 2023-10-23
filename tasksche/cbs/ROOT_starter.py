from ..callback import CallbackBase, CallBackEvent, InterruptSignal
from ..functional import ROOT_NODE
from ..util import call_back_event_to_runner_task


class RootStarter(CallbackBase):
    def on_task_start(self, event: CallBackEvent):
        if event.task_name != ROOT_NODE.NAME:
            return
        spec = call_back_event_to_runner_task(event)
        event.result_storage.storage.store(
            spec.output_path, value=event.value['RootStarter'])
        # del event.value['RootStarter']
        raise InterruptSignal('on_task_finish', event.new_inst())
