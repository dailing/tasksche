import abc
import asyncio
import os
import uuid
from typing import Any, Dict, List, Optional, Callable

from .callback import CALLBACK_TYPE, CallbackRunner, CallbackBase, CallBackEvent, InvokeSignal
from .cbs.EndTask import EndTask
from .cbs.FinishChecker import FinishChecker
from .cbs.LocalRunner import LocalRunner
from .cbs.ParallelRunner import ParallelRunner
from .common import Status
from .functional import (
    Graph, RunnerTaskSpec, search_for_root)
from .logger import Logger
from .storage.storage import ResultStorage, StatusStorage

logger = Logger()


class RunnerBase(abc.ABC):
    @abc.abstractmethod
    def add_task(self, spec: RunnerTaskSpec, cb: Callable):
        """
        Add a task to the runner.

        Args:
            spec (RunnerTaskSpec): The specification of the task.
            cb (Callable): The callback function to be executed when the task is
                completed.

        Raises:
            NotImplementedError: This method needs to be implemented by a subclass.
        """
        raise NotImplementedError


class MultiProcessRunner(RunnerBase):

    def add_task(self, spec: RunnerTaskSpec, cb: Callable):
        pass


# class LocalRunner(RunnerBase):
#
#     def add_task(self, spec: RunnerTaskSpec, cb: Callable):
#         execute_task(spec)
#         cb()


class Scheduler(CallbackBase):
    def __init__(
            self,
            graph: Graph,
            result_storage: ResultStorage,
            status_storage: StatusStorage,
            cbs: List[CALLBACK_TYPE]) -> None:
        self.graph = graph
        self.cb = CallbackRunner(cbs + [self])
        self.runner = LocalRunner()
        self.task_status = {}
        self.result_storage = result_storage
        self.status_storage = status_storage

    def feed(self, run_id=None, **kwargs):
        if run_id is None:
            run_id = uuid.uuid4().hex
        event = CallBackEvent(
            graph=self.graph, run_id=run_id,
            result_storage=self.result_storage,
            status_storage=self.status_storage)
        asyncio.run(self.cb.on_feed(event, kwargs))
        # asyncio.get_event_loop().run_until_complete(self.cb.on_feed(event, kwargs))

    def on_feed(
            self,
            event: CallBackEvent,
            kwargs: Dict[str, Any]):
        for n in event.graph.node_map.keys():
            if n in event.graph.source_nodes:
                self.status_storage.store(n, event.run_id, value=Status.STATUS_READY)
            else:
                self.status_storage.store(n, event.run_id, value=Status.STATUS_PENDING)
        for n in event.graph.source_nodes:
            yield InvokeSignal('on_task_ready', event.new_inst(task_name=n))

    def on_task_ready(self, event: CallBackEvent):
        task_name, run_id, graph = event.task_name, event.run_id, event.graph
        self.status_storage.store(task_name, run_id, value=Status.STATUS_RUNNING)
        return InvokeSignal('on_task_start', event)

    def on_task_finish(self, event: CallBackEvent):
        task_name, run_id, graph = event.task_name, event.run_id, event.graph
        self.status_storage.store(task_name, run_id, value=Status.STATUS_FINISHED)
        for k, v in self.graph.node_map.items():
            if task_name not in v.depend_on:
                continue
            ready = True
            for n in v.depend_on:
                if self.status_storage.get(n, run_id) != Status.STATUS_FINISHED:
                    ready = False
                    break
            if ready:
                return InvokeSignal('on_task_ready', event.new_inst(task_name=k))
                # self.cb.on_task_ready(event.new_inst(task_name=k))

    def on_run_finish(self, event: CallBackEvent):
        pass


def run(
        task: str,
        run_id: Optional[str] = None,
        storage_result: str = 'file:default',
        storage_status: str = 'mem:default',
) -> None:
    task = os.path.abspath(task)
    assert task.endswith('.py')
    root = search_for_root(task)
    task_name = task[len(root):-3]
    scheduler = Scheduler(
        Graph(root, [task_name]),
        ResultStorage(storage_result),
        StatusStorage(storage_status),
        [
            EndTask(),
            FinishChecker(storage_result),
            # LocalRunner(),
            ParallelRunner(),
        ]
    )
    scheduler.feed(run_id=str(run_id))


if __name__ == '__main__':
    # sche = Scheduler(
    #     Graph('test/simple_task_set', ['/task']),
    #     ResultStorage('file:default'),
    #     StatusStorage('mem:default'), [
    #         FinishChecker()
    #     ])
    # from pprint import pprint
    # pprint(sche.graph.requirements_map)
    # sche.feed()
    run('test/simple_task_set/task.py', run_id='test')
    # import fire
    #
    # fire.Fire(run)
