import asyncio
import os
from typing import List, Optional

from .callback import (
    CallbackBase,
    CallBackEvent,
    CallbackRunner,
    InvokeSignal,
)
from .cbs.FileWatcher import FileWatcher
from .cbs.LocalRunner import LocalRunner
from .functional import (
    RunnerTaskSpec,
    search_for_root,
)
from .logger import Logger
from .new_sch import Graph
from .new_sch import N_Scheduler
from .storage.storage import storage_factory

logger = Logger()


class Scheduler(CallbackBase):
    def __init__(
        self,
        graph: Graph,
        result_storage_path: str,
        work_dir_base: str,
    ) -> None:
        self.graph = graph
        self.runner = LocalRunner()
        self.result_storage_path = result_storage_path
        self.result_storage = storage_factory(result_storage_path)
        self.sc = N_Scheduler(graph, self.result_storage)
        self.work_dir_base = work_dir_base

    def dump(self):
        self.sc.dump()

    def load(self):
        self.sc.load()

    def on_init(self, _):
        event = CallBackEvent(
            task_id="",
            task_name="ROOT",
            task_spec=None,
        )
        self.sc.sche_init()
        yield InvokeSignal("on_task_finish", event)

    def _issue_new(self):
        pending_tasks = list(self.sc.get_issue_tasks())
        # self.sc.event_log_to_md(os.path.join(self.graph.root, "event_log.md"))
        for t in pending_tasks:
            node = self.graph.node_map[t.task_name]
            work_dir = os.path.join(
                self.work_dir_base, node.node.task_name[1:]
            )
            event = CallBackEvent(
                task_id=t.command_id,
                task_name=t.task_name,
                task_spec=RunnerTaskSpec(
                    task=t.task_name.split(":")[1],
                    root=self.graph.root,
                    requires=t.args,
                    storage_path=self.result_storage_path,
                    output_path=t.output,
                    work_dir=work_dir,
                    task_type=t.cmd_type,
                    task_id=t.command_id,
                    process_id=t.process_id,
                ),
            )
            yield InvokeSignal("on_task_start", event)

    def on_task_finish(self, event: CallBackEvent):
        if event.task_spec is not None:
            logger.info(
                f"{event.task_name} {event.task_id} "
                f"{event.task_spec.process_id}"
            )
            self.sc.set_finish_command(event.task_id)
        logger.info(f"running tasks: {self.sc.running_task_id}")
        new_tasks = list(self._issue_new())
        logger.info(f"new tasks: {new_tasks}")
        yield from new_tasks

    def on_iter_stop(self, event: CallBackEvent):
        self.sc.set_finish_command(event.task_id, generate=False)
        logger.info(f"running tasks: {self.sc.running_task_id}")
        yield from self._issue_new()

    def on_task_error(self, event: CallBackEvent):
        logger.error(
            f"{event.task_name} {event.task_id}",
        )
        self.sc.set_error_command(event.task_id)
        yield from self._issue_new()

    def on_file_change(self, event: CallBackEvent):
        self.sc.reload()
        for cmds in self._issue_new():
            logger.info(f"issuing: {cmds.signal} {cmds.event}")
            yield cmds


def run(
    tasks: List[str],
    storage_path: str,
    work_dir: str,
    watch_root: bool = False,
) -> None:
    tasks = [os.path.abspath(task) for task in tasks]
    assert os.path.exists(work_dir), work_dir
    assert os.path.isdir(work_dir)
    assert work_dir.startswith("/")
    root = search_for_root(tasks[0])
    if root is None:
        logger.error(f"ROOT NOT FOUND {tasks[0]}")
        return
    assert isinstance(root, str)
    for task in tasks:
        assert task.endswith(".py")
        assert task.startswith(root)
    task_names = [task[len(root) : -3] for task in tasks]
    scheduler = Scheduler(
        Graph(root, task_names),
        storage_path,
        work_dir,
    )
    cb_caller: List[CallbackBase] = []
    cb_caller.append(LocalRunner())
    if watch_root:
        cb_caller.append(FileWatcher(root=root))
    cb_caller.append(scheduler)
    cb = CallbackRunner(cb_caller)
    scheduler.load()
    asyncio.run(cb.run())
    logger.info("run end")
    scheduler.dump()

    return
