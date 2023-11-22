import asyncio
import concurrent.futures
from collections import defaultdict
from typing import Dict

from ..callback import (
    CallbackBase,
    CallBackEvent,
    InterruptSignal,
    InvokeSignal,
)
from ..functional import (
    BaseTaskExecutor,
    GeneratorTaskExecutor,
    MapperTaskExecutor,
)
from ..logger import Logger

logger = Logger()


class LocalRunner(CallbackBase):
    def __init__(self):
        super().__init__()
        # self.iterators = dict()
        self.tasks: Dict[
            str, BaseTaskExecutor | GeneratorTaskExecutor
        ] = defaultdict(MapperTaskExecutor)
        self.threadpool = concurrent.futures.ThreadPoolExecutor()
        self.waiting_futures = dict()

    async def _execute(self, executor: BaseTaskExecutor, event: CallBackEvent):
        spec = event.task_spec
        assert spec is not None
        out = await asyncio.get_event_loop().run_in_executor(
            self.threadpool, executor.call_run, spec
        )
        if isinstance(out, Exception):
            raise InterruptSignal("on_task_error", event)
        elif out is None:
            return InvokeSignal("on_task_finish", event)
        else:
            raise TypeError(f"output type {type(out)} {out} is not supported")

    async def on_task_run(self, event: CallBackEvent):
        executor = self.tasks[event.task_name]
        return await self._execute(executor, event)

    async def on_gen_start(self, event: CallBackEvent):
        executor = GeneratorTaskExecutor()
        self.tasks[event.task_name] = executor
        output = await asyncio.get_event_loop().run_in_executor(
            self.threadpool, executor.call_start, event.task_spec
        )
        if isinstance(output, Exception):
            raise InterruptSignal("on_task_error", event)
        elif output is None:
            return InvokeSignal("on_task_finish", event)
        raise NotImplementedError

    async def on_per_start(self, event: CallBackEvent):
        executor = self.tasks[event.task_name]
        output = asyncio.get_event_loop().run_in_executor(
            self.threadpool, executor.call_run, event.task_spec
        )
        assert event.task_name not in self.waiting_futures
        self.waiting_futures[event.task_name] = output
        return InvokeSignal("on_task_finish", event)

    async def on_task_pull(self, event: CallBackEvent):
        future = self.waiting_futures[event.task_name]
        output = await future
        if isinstance(output, Exception):
            raise InterruptSignal("on_task_error", event)
        elif output is None:
            return InvokeSignal("on_task_finish", event)
        raise NotImplementedError

    async def on_task_push(self, event: CallBackEvent):
        executor = self.tasks[event.task_name]
        assert event.task_spec is not None
        executor.call_push(event.task_spec)
        return InvokeSignal("on_task_finish", event)

    async def on_task_iterate(self, event: CallBackEvent):
        assert event.task_name in self.tasks, event.task_name
        executor = self.tasks[event.task_name]
        assert isinstance(executor, GeneratorTaskExecutor)
        spec = event.task_spec
        assert spec is not None
        output = await asyncio.get_event_loop().run_in_executor(
            self.threadpool, executor.call_iter, spec
        )
        if isinstance(output, StopIteration):
            logger.info(f"task {event.task_name} finished")
            return InvokeSignal("on_gen_finish", event)
        if isinstance(output, Exception):
            raise InterruptSignal("on_task_error", event)
        return InvokeSignal("on_task_finish", event)
