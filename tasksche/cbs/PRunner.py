import asyncio
import os
import signal
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Queue, Process

from ..callback import CallbackBase, CallBackEvent
from ..functional import RunnerTaskSpec
from ..logger import Logger

# from ..util import call_back_event_to_runner_task

logger = Logger()


def _start_process(p: Process, q: Queue):
    p.start()
    p.join()
    if not q.empty():
        err_msg = q.get()
        assert q.empty()
        assert isinstance(err_msg, Exception) or isinstance(
            err_msg, KeyboardInterrupt
        )


class RunerProcessPool:
    def __init__(self, max_workers=None):
        if max_workers is None:
            max_workers = os.cpu_count()
        self.threadPool = ThreadPoolExecutor(
            max_workers=max_workers,  # must larger than the thread pool size
        )
        self.processes = set()
        self.task_queue = Queue()
        self.result_queue = Queue()
        self.cnt = 0
        self.waiting_queue = asyncio.Queue(maxsize=max_workers)
        self.stop = False

    async def run(self, task: RunnerTaskSpec):
        await self.waiting_queue.put(self.cnt)
        if self.stop:
            await self.waiting_queue.get()
            return KeyboardInterrupt()
        q = Queue()
        p = Process(target=_execute_task, args=(task, q))
        self.processes.add(p)
        result = await asyncio.get_running_loop().run_in_executor(
            self.threadPool, _start_process, p, q
        )
        self.processes.remove(p)
        await self.waiting_queue.get()
        return result

    async def shutdown(self):
        self.stop = True
        for p in self.processes:
            try:
                os.kill(p.pid, signal.SIGINT)
            except ProcessLookupError:
                pass
        self.threadPool.shutdown(wait=True, cancel_futures=True)


class PRunner(CallbackBase):
    def __init__(self, max_workers=None):
        super().__init__()
        self.max_workers = max_workers
        self.pool = RunerProcessPool(self.max_workers)

    async def on_task_start(self, spec: RunnerTaskSpec):
        if self.pool is None:
            self.pool = RunerProcessPool(self.max_workers)
        result = await self.pool.run(spec)
        if isinstance(result, KeyboardInterrupt):
            logger.info(f"Interrupted {spec.task}")
        elif isinstance(result, Exception):
            logger.error(
                f"exception: {type(result)}", exc_info=result, stack_info=True
            )

    async def on_interrupt(self, event: CallBackEvent):
        if self.pool is None:
            return
        await self.pool.shutdown()
        self.pool = None
