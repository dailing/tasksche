from ..callback import CallbackBase, CallBackEvent, InterruptSignal


class EndTask(CallbackBase):
    def on_task_run(self, event: CallBackEvent):
        if event.task_name == "_END_":
            raise InterruptSignal("on_run_finish", event)
