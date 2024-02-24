import asyncio

from watchfiles import awatch

from ..callback import CallbackBase, CallBackEvent, InvokeSignal
from ..logger import Logger

logger = Logger()


class FileWatcher(CallbackBase):
    def __init__(self, root: str):
        super().__init__()
        self.stop_event = asyncio.Event()
        self.root = root

    async def on_init(self, _):
        logger.info(f"start watching {self.root}")
        async for e in awatch(self.root, stop_event=self.stop_event):
            files = set([x[1] for x in e])
            for file in files:
                assert file.startswith(self.root)
                file = file[len(self.root) :]
                assert file.startswith("/")
                if file.startswith("/__"):
                    continue
                logger.info(f"changed:  {file}")
                yield InvokeSignal(
                    "on_file_change",
                    event=CallBackEvent(
                        task_id="", task_name="", task_spec=None
                    ),
                )
        logger.info(f"stop watching {self.root}")
