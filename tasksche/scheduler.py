import asyncio
import os
import uuid
from pprint import pprint
from typing import Any, List, Optional

from .callback import (
    CALLBACK_TYPE, CallbackRunner, CallbackBase, CallBackEvent, InvokeSignal)
from .cbs.EndTask import EndTask
from .cbs.FinishChecker import FinishChecker
from .cbs.LocalRunner import LocalRunner
from .cbs.ROOT_starter import RootStarter
from .functional import (
    Graph, search_for_root, Status, ROOT_NODE)
from .logger import Logger
from .storage.storage import ResultStorage, StatusStorage

logger = Logger()


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
            status_storage=self.status_storage,
            value={'RootStarter': payload}
        )
        asyncio.run(self.cb.on_feed(event))

    def on_feed(self, event: CallBackEvent):
        for n in event.graph.node_map.keys():
            self.status_storage.store(
                n, event.run_id, event.n_iter,
                value=Status.STATUS_PENDING)
        return InvokeSignal(
            'on_task_start', event.new_inst(task_name=ROOT_NODE.NAME))

    def on_task_ready(self, event: CallBackEvent):
        task_name, run_id, graph = event.task_name, event.run_id, event.graph
        self.status_storage.store(
            task_name,
            run_id, i_iter=event.n_iter, value=Status.STATUS_RUNNING)
        return InvokeSignal('on_task_start', event)

    def on_task_finish(self, event: CallBackEvent):
        task_name, run_id, graph = event.task_name, event.run_id, event.graph
        self.status_storage.store(task_name, run_id, event.n_iter, value=Status.STATUS_FINISHED)
        for k, v in self.graph.node_map.items():
            if task_name not in v.depend_on:
                continue
            ready = True
            for n in v.depend_on:
                if self.status_storage.get(n, run_id, event.n_iter) != Status.STATUS_FINISHED:
                    ready = False
                    break
            if ready:
                yield InvokeSignal('on_task_ready', event.new_inst(task_name=k))

    def on_run_finish(self, event: CallBackEvent):
        pass

    def on_task_error(self, event: CallBackEvent):
        raise NotImplementedError

    def on_interrupt(self, event: CallBackEvent):
        self.graph = Graph(self.graph.root, self.graph.target_tasks)
        return InvokeSignal('on_feed', CallBackEvent(
            graph=self.graph,
            run_id=event.run_id,
            result_storage=self.result_storage,
            status_storage=self.status_storage,
            value={'RootStarter': event.value['RootStarter']}
        ))


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
            # FinishChecker(storage_result),
            RootStarter(),
            # PRunner(),
            LocalRunner(),
            # FileWatcher(),
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
    run(['test/generator_task_set/task4.py'], run_id='test', payload={})
    # run(['test/simple_task_set/task.py'], run_id='test', payload={})
    # import fire
    #
    # fire.Fire(run)
