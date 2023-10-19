import abc
import asyncio
import os
import uuid
from typing import Any, List, Optional, Callable

from .callback import CALLBACK_TYPE, CallbackRunner, CallbackBase, CallBackEvent, InvokeSignal
from .cbs.EndTask import EndTask
from .cbs.FinishChecker import FinishChecker
from .cbs.LocalRunner import LocalRunner
from .cbs.ParallelRunner import ParallelRunner
from .cbs.ROOT_starter import RootStarter
from .functional import (
    Graph, RunnerTaskSpec, search_for_root, Status, ROOT_NODE)
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

    def feed(self, run_id: str = None, payload: Any = None):
        if run_id is None:
            run_id = uuid.uuid4().hex
        event = CallBackEvent(
            graph=self.graph, run_id=run_id,
            result_storage=self.result_storage,
            status_storage=self.status_storage)
        asyncio.run(self.cb.on_feed(event, payload))

    def on_feed(self, event: CallBackEvent, payload: Any = None):
        for n in event.graph.node_map.keys():
            self.status_storage.store(n, event.run_id, value=Status.STATUS_PENDING)
        return InvokeSignal('on_task_start', event.new_inst(task_name=ROOT_NODE.NAME, value=payload))

    def on_task_ready(self, event: CallBackEvent):
        task_name, run_id, graph = event.task_name, event.run_id, event.graph
        self.status_storage.store(task_name, run_id, value=Status.STATUS_RUNNING)
        return InvokeSignal('on_task_start', event)

    def on_task_finish(self, event: CallBackEvent):
        task_name, run_id, graph = event.task_name, event.run_id, event.graph
        self.status_storage.store(task_name, run_id, value=Status.STATUS_FINISHED)
        # logger.info(f'run_id: {run_id}, task_name: {task_name}')
        # logger.info(f'graph: {graph.node_map["_ROOT_"].depend_on}')
        # sys.exit(0)
        for k, v in self.graph.node_map.items():
            if task_name not in v.depend_on:
                continue
            # logger.info(f'k: {k}, v: {v}')
            ready = True
            for n in v.depend_on:
                if self.status_storage.get(n, run_id) != Status.STATUS_FINISHED:
                    ready = False
                    break
            if ready:
                yield InvokeSignal('on_task_ready', event.new_inst(task_name=k))
                # self.cb.on_task_ready(event.new_inst(task_name=k))

    def on_run_finish(self, event: CallBackEvent):
        pass


def run(
        tasks: List[str],
        run_id: Optional[str] = None,
        storage_result: str = 'file:default',
        storage_status: str = 'mem:default',
        payload: Any = None
) -> None:
    tasks = [os.path.abspath(task) for task in tasks]
    root = search_for_root(tasks[0])
    for task in tasks:
        assert task.endswith('.py')
        assert task.startswith(root)
    task_names = [task[len(root):-3] for task in tasks]
    scheduler = Scheduler(
        Graph(root, task_names),
        ResultStorage(storage_result),
        StatusStorage(storage_status),
        [
            EndTask(),
            FinishChecker(storage_result),
            RootStarter(),
            ParallelRunner(),
        ]
    )
    scheduler.feed(run_id=str(run_id), payload=payload)


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
    run(['test/simple_task_set/task.py'], run_id='test', payload={})
    # import fire
    #
    # fire.Fire(run)
