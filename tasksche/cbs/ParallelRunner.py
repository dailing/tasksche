import asyncio
from concurrent.futures import ProcessPoolExecutor

from ..callback import CallbackBase, CallBackEvent, InvokeSignal
from ..functional import execute_task, RunnerTaskSpec
from ..util import call_back_event_to_runner_task


async def execute_task_async(spec: RunnerTaskSpec):
    execute_task(spec)


class ParallelRunner(CallbackBase):
    def __init__(self, max_workers=None):
        super().__init__()
        self.processPool = ProcessPoolExecutor(
            max_workers=max_workers, max_tasks_per_child=1)

    async def on_task_start(self, event: CallBackEvent):
        spec = call_back_event_to_runner_task(event)
        await asyncio.get_event_loop().run_in_executor(
            self.processPool, execute_task, spec)
        return InvokeSignal('on_task_finish', event)
