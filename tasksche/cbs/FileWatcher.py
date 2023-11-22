import asyncio
from typing import Optional

from watchfiles import awatch

from ..callback import CallbackBase, CallBackEvent, InvokeSignal
from ..logger import Logger

logger = Logger()


class FileWatcher(CallbackBase):
    def __init__(self):
        super().__init__()
        self.stop_event = asyncio.Event()
        self.async_task: Optional[asyncio.Task] = None
        self.first_call = True
        self.event_queue = asyncio.Queue()
        self.task = None
        self.watching = False

    async def _func(self, root):
        logger.debug(f"watching {root} ...")
        async for event in awatch(root, stop_event=self.stop_event):
            files = set([x[1] for x in event])
            await self.event_queue.put((files, "None"))

    async def on_feed(self, event: CallBackEvent):
        if self.task is None:
            logger.info("init should be called once")
            self.task = asyncio.create_task(self._func(event.graph.root))
        if not self.watching:
            self.watching = True
            return InvokeSignal("on_feed", event)
        changed_file = await self.event_queue.get()
        logger.info(changed_file)
        self.watching = False
        return InvokeSignal("on_interrupt", event)
