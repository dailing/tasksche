import asyncio
import os
from typing import Any, List, Optional

from .callback import (
    CALLBACK_TYPE,
    CallbackBase,
    CallBackEvent,
    CallbackRunner,
    InvokeSignal,
)
from .cbs.LocalRunner import LocalRunner
from .cbs.FileWatcher import FileWatcher

from .functional import (
    ARG_TYPE,
    EVENT_TYPE,
    RunnerArgSpec,
    RunnerTaskSpec,
    search_for_root,
)
from .logger import Logger
from .new_sch import Graph, ScheEvent
from .new_sch import N_Scheduler
from .storage.storage import storage_factory

logger = Logger()


class Scheduler(CallbackBase):
    def __init__(
        self,
        graph: Graph,
        result_storage: str,
        # cbs: List[CALLBACK_TYPE],
    ) -> None:
        self.graph = graph
        # self.cb = CallbackRunner(cbs + [self])
        self.runner = LocalRunner()
        self.result_storage_path = result_storage
        self.result_storage = storage_factory(result_storage)
        self.sc = N_Scheduler(graph)

    def dump(self):
        # raise NotImplementedError
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
        self.sc.event_log_to_md(os.path.join(self.graph.root, "event_log.md"))
        self.sc.graph.to_markdown(os.path.join(self.graph.root, "graph.md"))
        yield InvokeSignal("on_task_finish", event)

    def _transfer_arg_all(self, task: ScheEvent):
        reqs = {}
        for k, arg in task.args.items():
            path = arg
            if arg.arg_type == ARG_TYPE.TASK_OUTPUT and path is None:
                continue
            reqs[k] = RunnerArgSpec(
                arg_type=arg.arg_type,
                value=arg.value,
                storage_path=path,
            )
        return reqs

    def _issue_new(self):
        pending_tasks = list(self.sc.get_issue_tasks())
        self.sc.event_log_to_md(os.path.join(self.graph.root, "event_log.md"))
        # pprint.pprint(pending_tasks)
        for t in pending_tasks:
            node = self.graph.node_map[t.task_name]
            work_dir = os.path.join(
                self.result_storage.path, node.node.task_name[1:]
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
            self.sc.set_finish_command(event.task_id)
        logger.info(f"running tasks: {self.sc.running_task_id}")
        yield from self._issue_new()

    def on_iter_stop(self, event: CallBackEvent):
        self.sc.set_finish_command(event.task_id, generate=False)
        logger.info(f"running tasks: {self.sc.running_task_id}")
        yield from self._issue_new()

    def on_task_error(self, event: CallBackEvent):
        logger.error(
            f"{event.task_name} {event.task_id}",
        )
        # TODO add error handling, disable infected tasks
        self.sc.set_error_command(event.task_id)
        yield from self._issue_new()
        # raise NotImplementedError

    def on_file_change(self, event: CallBackEvent):
        logger.info(f"file change: {event.task_name}")
        logger.info(f"running tasks: {self.sc.running_task_id}")
        self.sc.reload()
        logger.info(f"running tasks: {self.sc.running_task_id}")


def run(
    tasks: List[str],
    storage_path: Optional[str] = None,
) -> None:
    tasks = [os.path.abspath(task) for task in tasks]
    root = search_for_root(tasks[0])
    if root is None:
        logger.error(f"ROOT NOT FOUND {tasks[0]}")
        return
    if storage_path is None:
        storage_path = f"{root}/__default"
    assert isinstance(root, str)
    for task in tasks:
        assert task.endswith(".py")
        assert task.startswith(root)
    task_names = [task[len(root) : -3] for task in tasks]
    scheduler = Scheduler(
        Graph(root, task_names),
        storage_path,
    )
    cb = CallbackRunner(
        [
            LocalRunner(),
            FileWatcher(root=root),
            scheduler,
        ]
    )
    scheduler.load()
    asyncio.run(cb.run())
    logger.info("run end")
    scheduler.dump()

    return
