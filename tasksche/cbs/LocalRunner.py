from collections import defaultdict
from tasksche.callback import CallBackEvent
from tasksche.storage.storage import IterRecord

from ..callback import (
    CallbackBase,
    CallBackEvent,
    InvokeSignal,
    InterruptSignal,
)
from ..functional import (
    BaseTaskExecutor,
    RunnerTaskSpec,
    execute_task,
    IteratorTaskExecutor,
    CollectorTaskExecutor,
)
from ..logger import Logger
from ..util import call_back_event_to_runner_task

logger = Logger()


class LocalRunner(CallbackBase):
    def __init__(self):
        super().__init__()
        self.iterators = defaultdict(IteratorTaskExecutor)
        self.tasks = defaultdict(CollectorTaskExecutor)

    def _execute(self, executer: BaseTaskExecutor, event: CallBackEvent):
        spec = call_back_event_to_runner_task(event)
        out = executer.call_run(spec)
        if isinstance(out, Exception):
            raise InterruptSignal("on_task_error", event)
        elif isinstance(out, str):
            return InvokeSignal("on_task_finish", event)
        elif out is None:
            return
        else:
            raise TypeError(f"output type {type(out)} {out} is not supported")

    def on_task_run(self, event: CallBackEvent):
        executer = self.tasks[event.task_name]
        return self._execute(executer, event)

    def on_task_iterate(self, event: CallBackEvent):
        executer = self.iterators[event.task_name]
        return self._execute(executer, event)
