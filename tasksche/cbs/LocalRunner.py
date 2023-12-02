import asyncio
import concurrent.futures
from collections import defaultdict

from ..callback import (
    CallbackBase,
    CallBackEvent,
    InterruptSignal,
    InvokeSignal,
)
from ..functional import (
    BaseTaskExecutor,
)
from ..logger import Logger

logger = Logger()


class LocalRunner(CallbackBase):
    def __init__(self) -> None:
        super().__init__()
        self.executor = defaultdict(BaseTaskExecutor)
        self.thread_pool = concurrent.futures.ThreadPoolExecutor()

    async def on_task_start(self, event: CallBackEvent):
        assert event.task_spec is not None
        executor = self.executor[event.task_name]
        task_id, output = await asyncio.get_event_loop().run_in_executor(
            self.thread_pool, executor.call, event.task_spec
        )
        cb = CallBackEvent(
            task_id=task_id,
            task_name="",
            task_spec=None,
        )
        # output = executor.call(event.task_spec)
        if isinstance(output, StopIteration):
            # logger.info(f"task {event.task_name} finished")
            return InvokeSignal("on_gen_finish", cb)
        if isinstance(output, Exception):
            logger.error(f"task {output}")
            raise InterruptSignal("on_task_error", cb)
        # logger.info(f"task {event.task_name} {event.task_id} ready")
        return InvokeSignal("on_task_finish", cb)

    def on_run_finish(self, event: CallBackEvent):
        for k, v in self.executor.items():
            v.task_queue.put(None)
        for k, v in self.executor.items():
            if v.process is not None:
                v.process.join()
