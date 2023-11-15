import asyncio
import os
import uuid
from collections import defaultdict
from itertools import chain
from typing import Any, Dict, List, Optional, Tuple

from .callback import (
    CALLBACK_TYPE,
    CallbackBase,
    CallBackEvent,
    CallbackRunner,
    InvokeSignal,
)
from .cbs.EndTask import EndTask
from .cbs.LocalRunner import LocalRunner
from .cbs.ROOT_starter import RootStarter
from .functional import ROOT_NODE, Graph, Status, search_for_root
from .logger import Logger
from .storage.storage import ResultStorage, StatusStorage

logger = Logger()


class SchedulerTaskGenerator:
    """
    generate tasks for scheduler
    Iterator is source task:
        generate next if finished
    Iter_Parameter is sink node for Iterator
    other nodes, pass thourgh

    generate [(task_name, run_num)...] -> task_name tuple

    generate two types of tasks:
    start_task: start and run with full parameter set
    iter_task: run with partial parameter when new output ready

    start_task:
        ((t1, 0), ...) -> (tn, None) if tn is generator
        ((t1, 0), ...) -> (tn, None) if any of ((t1, i1), ...) is iter_param
    iter_task:
        ((t_k, i_k)) -> (tn, None) if t_k is iter_param,
            issue only if ((t_k),(i_k-1)) finished
    normal_task:
        ((t1, 0), ...) -> (t_n, i_n)
    """

    def __init__(self, graph: Graph) -> None:
        self.graph = graph
        self.finished_set = set()
        self.pending_set = set()
        # contains (output_task, n_iter, current_task)
        self.pushed_set = set()
        self.pending_events = []
        self.issued_events = set()
        # record last pushed args, task_name, arg_from --> arg_niter
        self.last_pushed_args: Dict[Tuple[str, str], int] = defaultdict(
            lambda: -1
        )
        self.scheduele_for(ROOT_NODE.NAME)
        self.set_finished(ROOT_NODE.NAME, 0)

    def max_iter_finished_task(self, taskname: str):
        max_i = -1
        for t, i in chain(self.finished_set, self.pending_set):
            if t == taskname:
                max_i = max(max_i, i)
        return max_i

    def graph_str(self):
        s = "```mermaid\n"
        s += "graph TD\n"
        for reqs, name, output, dep in self.pending_events:
            if output is None:
                output = f"{name}_0"
            else:
                output = f"{output[0]}_{output[1]}"
            for req in reqs:
                s += f"{req[0]}_{req[1]} --> " f" |{name}| {output}\n"
                for dd in dep:
                    if dd[0] == req[0]:
                        s += f"{dd[0]}_{dd[1]} -.->{req[0]}_{req[1]}\n"
        s += "```\n"
        s.replace("/", "_")
        return s

    def set_finished(self, task_name: str, n_iter: int):
        task = (task_name, n_iter)
        assert task not in self.finished_set
        assert task in self.pending_set
        self.pending_set.remove(task)
        self.finished_set.add(task)
        node = self.graph.node_map[task_name]
        if node.is_generator:
            self.scheduele_for(task_name)

    def set_pushed(self, task_name: str, from_task: str, n_iter: int):
        assert self.last_pushed_args[(task_name, from_task)] == (n_iter - 1)
        assert (from_task, n_iter) in self.finished_set
        self.last_pushed_args[(task_name, from_task)] = n_iter

    def in_pending(self, task_name: str):
        for t, _ in self.pending_set:
            if t == task_name:
                return True
        return False

    def task_started(self, task_name: str):
        if (task_name, 0) in self.finished_set:
            return True
        if (task_name, 0) in self.pending_set:
            return True
        return False

    def scheduele_for(self, task_name_: str, ready_dict=None, parent=None):
        # logger.info(f"Schedule for {task_name_}")
        # check dependency of this task,
        # if ready, generate running schedules
        ready_dict = {}
        # new_item_to_run = []
        node = self.graph.node_map[task_name_]

        for task_name in [task_name_] + self.graph.all_child_map[task_name_]:
            output = (task_name, self.max_iter_finished_task(task_name) + 1)
            node = self.graph.node_map[task_name]
            if not self.task_started(task_name):
                # starter of node instance
                logger.debug(f"Start {task_name}")
                ready_dict[task_name] = output
                self.pending_set.add(output)
                if node.is_persistent_node:
                    output = None
                req = tuple(ready_dict[t] for t in node.TASK_OUTPUT_DEPEND_ON)
                self.pending_events.append(
                    (
                        req,
                        task_name,
                        output,
                        (),
                    )
                )
            else:
                if node.is_generator and not self.in_pending(task_name):
                    # generate new output
                    ready_dict[task_name] = output
                    self.pending_set.add(output)
                    self.pending_events.append(
                        (
                            (),
                            task_name,
                            output,
                            (),
                        )
                    )
            if len(node.TASK_ITER_DEPEND_ON) > 0:
                # push new value to iterator
                for t in node.TASK_OUTPUT_DEPEND_ON:
                    assert ready_dict.get(t, None) is None
                for t in node.TASK_ITER_DEPEND_ON:
                    req = ready_dict.get(t, None)
                    if req is None:
                        continue
                    self.pending_events.append(
                        (
                            (req,),
                            task_name,
                            None,
                            ((req[0], req[1] - 1),) if req[1] > 0 else (),
                        )
                    )

    def set_issued(self, events: List[Any]):
        self.issued_events.update(events)

    def get_ready_to_issue(self):
        for event in self.pending_events:
            if event in self.issued_events:
                continue
            reqs, name, output, dep = event
            if output in self.finished_set:
                self.issued_events.add(event)
                continue
            # logger.info(name)
            if not all(map(lambda x: x in self.finished_set, dep)):
                # logger.info(
                #     f"{all(map(lambda x: x in self.finished_set, dep))}"
                # )
                # logger.debug(f"{name} dep  {dep} {self.finished_set}")
                continue
            if not all(
                map(
                    lambda x: x[1] == (self.last_pushed_args[(name, x[0])]),
                    dep,
                )
            ):
                # logger.debug(f"{event} dep  {dep} {self.last_pushed_args}")
                # for x in dep:
                #     logger.info(
                #         f"{self.last_pushed_args[(name, x[0])]} {x[1]}"
                #     )
                continue

            if not all(map(lambda x: x in self.finished_set, reqs)):
                # logger.debug(f"issue req {self.finished_set}")
                # logger.debug(f"{name} req {reqs}, {self.finished_set}")
                continue
            # logger.info(f"issue {reqs} {name} {output}")
            self.issued_events.add(event)
            yield event


class Scheduler(CallbackBase):
    def __init__(
        self,
        graph: Graph,
        result_storage: ResultStorage,
        status_storage: StatusStorage,
        cbs: List[CALLBACK_TYPE],
    ) -> None:
        self.graph = graph
        self.cb = CallbackRunner(cbs + [self])
        self.runner = LocalRunner()
        self.task_status = {}
        self.result_storage = result_storage
        self.status_storage = status_storage
        self.iter_issue_order = defaultdict(list)
        self.finished_idx_in_issue = defaultdict(lambda: -1)
        self.sc = SchedulerTaskGenerator(graph)

        # map_from (task_name, run_num) --> arg_deps
        self.dep_map = defaultdict(dict)
        # set (task_name run_num)
        self.finished_task = set()

    def feed(self, run_id: Optional[str] = None, payload: Any = None):
        if run_id is None:
            run_id = uuid.uuid4().hex
        event = CallBackEvent(
            graph=self.graph,
            run_id=run_id,
            result_storage=self.result_storage,
            status_storage=self.status_storage,
        )
        self.result_storage.store(
            ROOT_NODE.NAME,
            run_id=run_id,
            i_iter=0,
            value=None,
        )
        asyncio.run(self.cb.on_feed(event))

    def on_feed(self, event: CallBackEvent):
        for n in event.graph.node_map.keys():
            self.status_storage.store(
                n,
                event.run_id,
                event.n_iter,
                value=Status.STATUS_PENDING,
            )
        return InvokeSignal(
            "on_task_finish", event.new_inst(task_name=ROOT_NODE.NAME)
        )

    def on_task_check(self, event: CallBackEvent):
        task_name = event.task_name
        assert task_name is not None
        assert event.status_storage is not None
        v = event.graph.node_map[task_name]
        if event.status_storage.has(task_name, event.run_id, event.n_iter):
            current_status = event.status_storage.get(
                task_name, event.run_id, event.n_iter
            )
            if current_status in {
                Status.STATUS_FINISHED,
                Status.STATUS_RUNNING,
            }:
                return
        for n in v.depend_on:
            if (
                event.status_storage.get(n, event.run_id, event.n_iter)
                != Status.STATUS_FINISHED
            ):
                return

        return InvokeSignal("on_task_ready", event.new_inst())

    def on_task_ready(self, event: CallBackEvent):
        task_name, run_id = event.task_name, event.run_id
        assert task_name is not None
        # TODO: change to set no exist
        self.status_storage.store(
            task_name,
            run_id,
            event.n_iter,
            value=Status.STATUS_RUNNING,
        )
        return InvokeSignal("on_task_start", event)

    def on_task_start(self, event: CallBackEvent):
        if event.is_generator:
            if event.n_iter != 0:
                logger.warning(
                    f"on_task_start {event.task_name}, iter: {event.n_iter}"
                )
            return InvokeSignal("on_task_iterate", event.new_inst())
        return InvokeSignal("on_task_run", event)

    def on_task_finish(self, event: CallBackEvent):
        task_name, run_id = event.task_name, event.run_id
        self.finished_task.add((task_name, event.n_iter))

        assert task_name is not None
        self.status_storage.store(
            task_name,
            run_id,
            event.n_iter,
            value=Status.STATUS_FINISHED,
        )

        for k, v in self.graph.node_map.items():
            if task_name in v.depend_on:
                yield InvokeSignal(
                    "on_task_check", event.new_inst(task_name=k)
                )
        if event.is_generator:
            yield InvokeSignal("on_task_iterate", event)

    def on_task_error(self, event: CallBackEvent):
        logger.error(f"{event.task_name} {event.run_id} {str(event.n_iter)}")
        raise NotImplementedError

    def on_task_iterend(self, event: CallBackEvent):
        assert event.task_name is not None
        assert event.result_storage is not None
        assert event.n_iter is not None

    def on_interrupt(self, event: CallBackEvent):
        self.graph = Graph(self.graph.root, self.graph.target_tasks)
        return InvokeSignal(
            "on_feed",
            CallBackEvent(
                graph=self.graph,
                run_id=event.run_id,
                result_storage=self.result_storage,
                status_storage=self.status_storage,
            ),
        )


def run(
    tasks: List[str],
    run_id: Optional[str] = None,
    storage_result: str = "file:default",
    storage_status: str = "mem:default",
    payload: Any = None,
) -> None:
    tasks = [os.path.abspath(task) for task in tasks]
    root = search_for_root(tasks[0])
    assert isinstance(root, str)
    for task in tasks:
        assert task.endswith(".py")
        assert task.startswith(root)
    task_names = [task[len(root) : -3] for task in tasks]
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
        ],
    )

    from pprint import pprint

    pprint(scheduler.graph.all_child_map)

    # pprint(scheduler.graph.node_map)
    # for k, v in scheduler.graph.node_map.items():
    #     print(k)
    #     pprint(v._dep_arg_parse)
    #     print("---")
    # scheduler.sc.scheduele_for(ROOT_NODE.NAME, -1)
    # scheduler.sc.scheduele_for(ROOT_NODE.NAME)
    pprint(list(scheduler.sc.get_ready_to_issue()))
    # pprint(scheduler.sc.pending_events)
    print("finished task0, 0")
    scheduler.sc.set_finished("/task0", 0)
    pprint(list(scheduler.sc.get_ready_to_issue()))
    print("finished task1, 0")
    scheduler.sc.set_finished("/task1", 0)
    pprint(list(scheduler.sc.get_ready_to_issue()))
    print("finished task1, 1")
    scheduler.sc.set_finished("/task1", 1)
    pprint(list(scheduler.sc.get_ready_to_issue()))
    print("pushed task1, 0")
    scheduler.sc.set_pushed("/task2", "/task1", 0)
    pprint(list(scheduler.sc.get_ready_to_issue()))
    print("pushed task1, 1")
    # scheduler.sc.set_finished("/task1", 1)
    scheduler.sc.set_pushed("/task2", "/task1", 1)
    pprint(list(scheduler.sc.get_ready_to_issue()))
    print("finished task1, 2")
    scheduler.sc.set_finished("/task1", 2)
    pprint(list(scheduler.sc.get_ready_to_issue()))

    # logger.info("start")
    # scheduler.sc.set_finished("/task1", 0)
    # scheduler.sc.set_finished("/task1", 1)
    # scheduler.sc.set_finished("/task2", 0)
    with open("./out.md", "w") as f:
        f.write(scheduler.sc.graph_str())
    return
    scheduler.feed(run_id=str(run_id), payload=payload)
    with open("./out.md", "w") as f:
        f.write("```mermaid\n")
        f.write("graph TD\n")
        for idx, (from_node, to_node) in enumerate(scheduler.cb.call_graph):
            from_node_ = from_node.replace("-", "<br/>")
            to_node_ = to_node.replace("-", "<br/>")
            f.write(
                f'{from_node}["{from_node_}"] '
                f"-->|{idx}|"
                f' {to_node}["{to_node_}"]\n'
            )
        f.write("```")
    from pprint import pprint

    pprint(scheduler.iter_issue_order)
    print(scheduler.graph.node_map["/task4"])


if __name__ == "__main__":
    # sche = Scheduler(
    #     Graph('test/simple_task_set', ['/task']),
    #     ResultStorage('file:default'),
    #     StatusStorage('mem:default'), [
    #         FinishChecker()
    #     ])
    # from pprint import pprint
    # pprint(sche.graph.requirements_map)
    # sche.feed()
    run(
        ["test/generator_task_set/task4.py"],
        run_id="test",
        payload={},
    )
    # run(['test/simple_task_set/task.py'], run_id='test', payload={})
    # import fire
    #
    # fire.Fire(run)
