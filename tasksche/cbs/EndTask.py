from ..callback import CallbackBase, CallBackEvent, InterruptSignal


class EndTask(CallbackBase):

    def on_task_ready(self, event: CallBackEvent):
        if event.task_name == '_END_':
            raise InterruptSignal(
                'on_run_finish',
                event
            )

