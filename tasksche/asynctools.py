import asyncio
from typing import Callable, AsyncGenerator, Generator, List, Coroutine

from .logger import Logger

logger = Logger()


class iter_to_async_callable:
    def __init__(self, func: Callable | AsyncGenerator | Generator) -> None:
        self.iter_obj = None
        self.func = func
        self.lock = asyncio.Lock()
        self.has_next = True

    async def __call__(self):
        async with self.lock:
            if not self.has_next:
                return None
            if self.iter_obj is None:
                self.iter_obj = self.func()
                if isinstance(self.iter_obj, Coroutine):
                    self.iter_obj = await self.iter_obj
                    logger.debug(f"iter_obj: {self.iter_obj}")
                logger.debug(f"iter_obj: {self.iter_obj}")
            if isinstance(self.iter_obj, AsyncGenerator):
                try:
                    return await self.iter_obj.__anext__()
                except StopAsyncIteration:
                    self.has_next = False
                    return None
            elif isinstance(self.iter_obj, Generator):
                try:
                    return self.iter_obj.__next__()
                except StopIteration:
                    self.func = None
                    self.has_next = False
                    return None
            else:
                logger.debug(f"iter_obj: {self.iter_obj} {self.func}")
                self.has_next = False
                return self.iter_obj


class iter_over_async_gens:
    def __init__(
        self, callables: List[Callable | AsyncGenerator | Generator]
    ) -> None:
        self.callables = [iter_to_async_callable(x) for x in callables]
        self.tasks = None

    async def __aiter__(self):
        tasks: List[asyncio.Task] = [
            asyncio.create_task(x()) for x in self.callables
        ]
        while len(tasks) > 0:
            finished, pending = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for feat in finished:
                idx = tasks.index(feat)
                if feat.result() is None:
                    tasks.pop(idx)
                else:
                    tasks[idx] = asyncio.create_task(self.callables[idx]())
                    yield feat.result()
