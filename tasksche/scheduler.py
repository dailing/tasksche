import asyncio
import os
import queue
from tracemalloc import start
import uuid
from typing import Any, Dict, List, Optional, Tuple

from tasksche.cbs.EndTask import EndTask
from tasksche.cbs.PRunner import PRunner

from .callback import (
    CALLBACK_TYPE,
    CallbackBase,
    CallBackEvent,
    CallbackRunner,
    InvokeSignal,
)
from .cbs.LocalRunner import LocalRunner
from .functional import (
    ARG_TYPE,
    END_NODE,
    ISSUE_TASK_TYPE,
    ROOT_NODE,
    Graph,
    RunnerArgSpec,
    RunnerTaskSpec,
    TaskIssueInfo,
    search_for_root,
)
from .logger import Logger
from .storage.storage import storage_factory

logger = Logger()


class Sc2:
    def __init__(self, graph: Graph) -> None:
        self.graph = graph
        self.finished_id = set()
        self.pending_events: Dict[str, TaskIssueInfo] = {}
        self.issued_task_id = set()
        self._latest_output_dict = {}  # record the latest output of each task
        self._last_push: Dict[Tuple[str, str], str] = {}
        # record last push task id
        self._last_iter: Dict[str, str] = {}
        # record last iter task id
        self.finished_generator = set()
        self.schedule_for()

    def get_latest_output_of_task(self, task_name: str):
        return self._latest_output_dict[task_name]

    def get_issue_info(self, task_id: str):
        return self.pending_events[task_id]

    def graph_str(self):
        s = "```mermaid\n"
        s += "graph TD\n"
        for task in self.pending_events.values():
            for dep in task.wait_for:
                s += (
                    f"{dep}{self.pending_events[dep].node_repr} "
                    f"--->|{task.task_name}|{task.task_id}"
                    f"{task.node_repr}\n"
                )
        s += "```\n"
        s.replace("/", "_")
        return s

    def set_finished(self, task_id: str, generate=True):
        assert task_id not in self.finished_id, (task_id, self.finished_id)
        assert task_id in self.issued_task_id
        self.finished_id.add(task_id)
        task_info = self.get_issue_info(task_id)
        node = self.graph.node_map[task_info.task_name]
        if not generate:
            self.finished_generator.add(task_info.task_name)
        if node.is_generator and generate:
            self.schedule_for()
        logger.info(f"finished {str(task_info)}")

    def in_pending(self, task_name: str):
        for t in self.pending_events.values():
            if (
                (t.task_name == task_name)
                and (t.task_id not in self.finished_id)
                and (t.output is not None)
            ):
                return True
        return False

    def task_started(self, task_name: str):
        for t in self.pending_events.values():
            if t.task_name == task_name:
                return True
        return False

    def schedule_for(
        self, task_name_: Optional[str] = None, ready_dict=None
    ) -> Optional[TaskIssueInfo]:
        if ready_dict is None:
            ready_dict = {}
        if task_name_ is None:
            task_name_ = END_NODE.NAME
        if task_name_ in ready_dict:
            return ready_dict[task_name_]

        def pend_task(task_: TaskIssueInfo):
            self.pending_events[task_.task_id] = task_
            if task_.output is not None:
                ready_dict[task_.task_name] = task_
                self._latest_output_dict[task_.task_name] = task_
            if task_.task_type is ISSUE_TASK_TYPE.PUSH_TASK:
                from_task = self.pending_events[task_.wait_for[0]].task_name
                push_pair = (task_.task_name, from_task)
                if push_pair in self._last_push:
                    task_.wait_for = task_.wait_for + (
                        self._last_push[push_pair],
                    )
                self._last_push[push_pair] = task_.task_id
            if task_.task_type is ISSUE_TASK_TYPE.ITER_TASK:
                task_.wait_for = task_.wait_for + (
                    self._last_iter[task_.task_name],
                )
            if task_.task_type in (
                ISSUE_TASK_TYPE.START_TASK,
                ISSUE_TASK_TYPE.ITER_TASK,
            ):
                self._last_iter[task_.task_name] = task_.task_id
            if task_.task_type == ISSUE_TASK_TYPE.PULL_RESULT:
                task_.wait_for = task_.wait_for + (
                    self._last_iter[task_.task_name],
                )
            requirements = {}
            for arg_key, arg in node.dep_arg_parse.items():
                if arg.arg_type in (ARG_TYPE.VIRTUAL, ARG_TYPE.RAW):
                    continue
                tt = arg.from_task
                if tt in ready_dict:
                    requirements[arg_key] = ready_dict[tt].output
            task_.reqs = requirements
            logger.info(f"Pending  {str(task_)}")
            return task_

        node = self.graph.node_map[task_name_]
        starter = None

        if not self.task_started(task_name_):
            wait_for = []
            for task in node.depend_on_no_virt:
                w = self.schedule_for(task, ready_dict)
                if w is not None:
                    wait_for.append(w)
            task_info = TaskIssueInfo(
                task_name=task_name_,
                task_type=ISSUE_TASK_TYPE.START_TASK,
                wait_for=tuple(
                    t.task_id
                    for t in wait_for
                    if t.task_name in node.TASK_OUTPUT_DEPEND_ON
                ),
            )
            if node.is_generator:
                task_info.output = None
                starter = pend_task(task_info)
            elif node.is_persistent_node:
                task_info.output = None
                starter = pend_task(task_info)
                starter = pend_task(
                    TaskIssueInfo(
                        task_name=task_name_,
                        task_type=ISSUE_TASK_TYPE.PULL_RESULT,
                        wait_for=(starter.task_id,),
                    )
                )
            else:
                starter = pend_task(task_info)
                return starter
            assert starter is not None

        for k in node.TASK_ITER_DEPEND_ON:
            task = self.schedule_for(k, ready_dict)
            if task is not None:
                pend_task(
                    TaskIssueInfo(
                        task_name=task_name_,
                        task_type=ISSUE_TASK_TYPE.PUSH_TASK,
                        wait_for=(task.task_id,),
                        output=None,
                    )
                )
        if starter is None:
            for k in node.TASK_OUTPUT_DEPEND_ON:
                task = self.schedule_for(k, ready_dict)
                if task is not None:
                    assert not node.is_persistent_node, node
                    return pend_task(
                        TaskIssueInfo(
                            task_name=task_name_,
                            task_type=ISSUE_TASK_TYPE.NORMAL_TASK,
                            wait_for=(task.task_id,),
                        )
                    )
        if (
            node.is_generator
            and not self.in_pending(task_name_)
            and (task_name_ not in self.finished_generator)
        ):
            return pend_task(
                TaskIssueInfo(
                    task_name=task_name_,
                    task_type=ISSUE_TASK_TYPE.ITER_TASK,
                )
            )

    def get_ready_to_issue(self):
        for event in self.pending_events.values():
            if event.task_name == END_NODE.NAME:
                continue
            if event.task_id in self.issued_task_id:
                continue
            if not all(map(lambda x: x in self.finished_id, event.wait_for)):
                continue
            self.issued_task_id.add(event.task_id)
            logger.info(f"issued   {str(event)}")
            yield event


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
        self.finished_id = set()
        self.pending_events: Dict[str, TaskIssueInfo] = {}
        self.issued_task_id = set()
        self._latest_output_dict = {}  # record the latest output of each task

        self._last_push: Dict[Tuple[str, str], str] = {}
        # record last push task id

        self._last_iter: Dict[str, str] = {}
        # record last iter task id

        self.schedule_for(ROOT_NODE.NAME)

    def get_latest_output_of_task(self, task_name: str):
        return self._latest_output_dict[task_name]

    def get_issue_info(self, task_id: str):
        return self.pending_events[task_id]

    def graph_str(self):
        s = "```mermaid\n"
        s += "graph TD\n"
        for task in self.pending_events.values():
            for dep in task.wait_for:
                s += (
                    f"{dep}{self.pending_events[dep].node_repr} "
                    f"--->|{task.task_name}|{task.task_id}"
                    f"{task.node_repr}\n"
                )
        s += "```\n"
        s.replace("/", "_")
        return s

    def set_finished(self, task_id: str, generate=True):
        assert task_id not in self.finished_id, (task_id, self.finished_id)
        assert task_id in self.issued_task_id
        self.finished_id.add(task_id)
        task_info = self.get_issue_info(task_id)
        node = self.graph.node_map[task_info.task_name]
        if node.is_generator and generate:
            self.schedule_for(task_info.task_name)
        logger.info(f"finished {str(task_info)}")

    def in_pending(self, task_name: str):
        for t in self.pending_events.values():
            if (
                (t.task_name == task_name)
                and (t.task_id not in self.finished_id)
                and (t.output is not None)
            ):
                return True
        return False

    def task_started(self, task_name: str):
        for t in self.pending_events.values():
            if t.task_name == task_name:
                return True
        return False

    def schedule_for(self, task_name_: str):
        # logger.info(f"Schedule for {task_name_}")
        # check dependency of this task,
        # if ready, generate running schedules

        ready_dict: Dict[str, TaskIssueInfo] = {}
        # ready_dict saves task_name, TaskInfo for tasks with output ready
        search_queue = queue.Queue()
        search_queue.put(task_name_)
        visited_task_names = set()

        def pend_task(task_: TaskIssueInfo):
            self.pending_events[task_.task_id] = task_
            if task_.output is not None:
                ready_dict[task_.task_name] = task_
                self._latest_output_dict[task_.task_name] = task_
                for t in self.graph.child_map[task_.task_name]:
                    if t not in visited_task_names:
                        search_queue.put(t)
                        visited_task_names.add(t)
            if task_.task_type is ISSUE_TASK_TYPE.PUSH_TASK:
                from_task = self.pending_events[task_.wait_for[0]].task_name
                push_pair = (task_.task_name, from_task)
                if push_pair in self._last_push:
                    task_.wait_for = task_.wait_for + (
                        self._last_push[push_pair],
                    )
                self._last_push[push_pair] = task_.task_id
            if task_.task_type is ISSUE_TASK_TYPE.ITER_TASK:
                task_.wait_for = task_.wait_for + (
                    self._last_iter[task_.task_name],
                )
            if (
                task_.task_type
                in (ISSUE_TASK_TYPE.START_TASK, ISSUE_TASK_TYPE.ITER_TASK)
                and node.is_generator
            ):
                self._last_iter[task_.task_name] = task_.task_id
            requirements = {}
            for arg_key, arg in node.dep_arg_parse.items():
                if arg.arg_type in (ARG_TYPE.VIRTUAL, ARG_TYPE.RAW):
                    continue
                tt = arg.from_task
                if tt in ready_dict:
                    requirements[arg_key] = ready_dict[tt].output
            task_.reqs = requirements
            logger.info(f"Pending  {str(task_)}")
            return task_

        while not search_queue.empty():
            # for task_name in loop_tasks:
            task_name = search_queue.get()
            node = self.graph.node_map[task_name]
            new_params = node.TASK_OUTPUT_DEPEND_ON.intersection(
                ready_dict.keys()
            )
            iter_push = node.TASK_ITER_DEPEND_ON.intersection(
                ready_dict.keys()
            )
            start_task = None
            if not self.task_started(task_name):
                # starter of node instance
                node = self.graph.node_map[task_name]
                req = tuple(
                    ready_dict[t].task_id for t in node.TASK_OUTPUT_DEPEND_ON
                )
                start_task = TaskIssueInfo(
                    task_name=task_name,
                    task_type=ISSUE_TASK_TYPE.START_TASK,
                    wait_for=req,
                )
                if node.is_persistent_node:
                    start_task.output = None
                pend_task(start_task)
                if not node.is_generator and node.is_persistent_node:
                    pend_task(
                        TaskIssueInfo(
                            task_name=task_name,
                            task_type=ISSUE_TASK_TYPE.PULL_RESULT,
                            wait_for=(start_task.task_id,),
                        )
                    )
            if len(new_params) > 0 and start_task is None:
                assert len(iter_push) == 0
                assert not node.is_generator
                # generate new map output
                for task in new_params:
                    dep = self._latest_output_dict[task].task_id
                    pend_task(
                        TaskIssueInfo(
                            task_name=task_name,
                            task_type=ISSUE_TASK_TYPE.NORMAL_TASK,
                            wait_for=(dep,),
                        )
                    )
            if node.is_generator and not self.in_pending(task_name):
                # generate new output from generator
                pend_task(
                    TaskIssueInfo(
                        task_name=task_name,
                        task_type=ISSUE_TASK_TYPE.ITER_TASK,
                    )
                )
            if len(iter_push) > 0:
                assert (len(new_params) == 0) or start_task is not None
                # push new value to iterator
                for task in iter_push:
                    dep = (ready_dict[task].task_id,)
                    if start_task is not None:
                        dep = dep + (start_task.task_id,)
                    pend_task(
                        TaskIssueInfo(
                            task_name=task_name,
                            task_type=ISSUE_TASK_TYPE.PUSH_TASK,
                            wait_for=dep,
                            output=None,
                        )
                    )

    def get_ready_to_issue(self):
        for event in self.pending_events.values():
            if event.task_name == END_NODE.NAME:
                continue
            if event.task_id in self.issued_task_id:
                continue
            if not all(map(lambda x: x in self.finished_id, event.wait_for)):
                continue
            self.issued_task_id.add(event.task_id)
            logger.info(f"issued   {str(event)}")
            yield event


class Scheduler(CallbackBase):
    def __init__(
        self,
        graph: Graph,
        result_storage: str,
        cbs: List[CALLBACK_TYPE],
    ) -> None:
        self.graph = graph
        self.cb = CallbackRunner(cbs + [self])
        self.runner = LocalRunner()
        self.result_storage_path = result_storage
        self.result_storage = storage_factory(result_storage)
        # self.sc = SchedulerTaskGenerator(graph)
        self.sc = Sc2(graph)

        self.latest_result = dict()

    def feed(self, run_id: Optional[str] = None, payload: Any = None):
        if run_id is None:
            run_id = uuid.uuid4().hex
        event = CallBackEvent(
            run_id=run_id,
            task_id="",
            task_name=ROOT_NODE.NAME,
            task_spec=None,
        )
        self.result_storage.store(
            ROOT_NODE.NAME,
            value=None,
        )
        asyncio.run(self.cb.on_task_finish(event))

    def _transfer_arg_all(self, task: TaskIssueInfo, search_past: bool = True):
        node = self.graph.node_map[task.task_name]
        reqs = {}
        for k, arg in node.dep_arg_parse.items():
            if arg.arg_type == ARG_TYPE.VIRTUAL:
                continue
            path = task.reqs.get(k, None)
            if not search_past and path is None:
                continue
            if path is None:
                path = self.latest_result.get(arg.from_task, None)
            if arg.arg_type == ARG_TYPE.TASK_ITER and search_past:
                path = None
            assert path is not None or arg.arg_type != ARG_TYPE.TASK_OUTPUT
            reqs[k] = RunnerArgSpec(
                arg_type=arg.arg_type,
                value=arg.value,
                storage_path=path,
            )
        return reqs

    def _issue_new(self, run_id):
        pending_tasks = list(self.sc.get_ready_to_issue())
        for t in pending_tasks:
            node = self.graph.node_map[t.task_name]
            if t.task_name == ROOT_NODE.NAME:
                continue
            if t.task_name == END_NODE.NAME:
                continue
            if t.task_type is ISSUE_TASK_TYPE.START_TASK:
                event = CallBackEvent(
                    run_id=run_id,
                    task_id=t.task_id,
                    task_name=t.task_name,
                    task_spec=RunnerTaskSpec(
                        task=t.task_name,
                        root=self.graph.root,
                        requires=self._transfer_arg_all(t),
                        storage_path=self.result_storage_path,
                        output_path=t.output,
                        work_dir=f"/tmp/workdir/{t.task_id}",
                    ),
                )
                if node.is_generator:
                    yield InvokeSignal("on_gen_start", event)
                elif node.is_persistent_node:
                    yield InvokeSignal("on_per_start", event)
                else:
                    yield InvokeSignal("on_task_run", event)
            elif t.task_type is ISSUE_TASK_TYPE.ITER_TASK:
                yield InvokeSignal(
                    "on_task_iterate",
                    CallBackEvent(
                        run_id=run_id,
                        task_id=t.task_id,
                        task_name=t.task_name,
                        task_spec=RunnerTaskSpec(
                            task=t.task_name,
                            root=self.graph.root,
                            requires={},
                            storage_path=self.result_storage_path,
                            output_path=t.output,
                            work_dir=f"/tmp/workdir/{t.task_id}",
                        ),
                    ),
                )
            elif t.task_type is ISSUE_TASK_TYPE.PUSH_TASK:
                logger.info(
                    f"on_task_push {t.reqs} {self._transfer_arg_all(t, False)}"
                )
                yield InvokeSignal(
                    "on_task_push",
                    CallBackEvent(
                        run_id=run_id,
                        task_id=t.task_id,
                        task_name=t.task_name,
                        task_spec=RunnerTaskSpec(
                            task=t.task_name,
                            root=self.graph.root,
                            requires=self._transfer_arg_all(t, False),
                            storage_path=self.result_storage_path,
                            output_path=t.output,
                            work_dir=f"/tmp/workdir/{t.task_id}",
                        ),
                    ),
                )
            elif t.task_type is ISSUE_TASK_TYPE.NORMAL_TASK:
                event = CallBackEvent(
                    run_id=run_id,
                    task_id=t.task_id,
                    task_name=t.task_name,
                    task_spec=RunnerTaskSpec(
                        task=t.task_name,
                        root=self.graph.root,
                        requires=self._transfer_arg_all(t),
                        storage_path=self.result_storage_path,
                        output_path=t.output,
                        work_dir=f"/tmp/workdir/{t.task_id}",
                    ),
                )
                assert not node.is_generator
                assert not node.is_persistent_node
                yield InvokeSignal("on_task_run", event)
            elif t.task_type is ISSUE_TASK_TYPE.PULL_RESULT:
                event = CallBackEvent(
                    run_id=run_id,
                    task_id=t.task_id,
                    task_name=t.task_name,
                    task_spec=RunnerTaskSpec(
                        task=t.task_name,
                        root=self.graph.root,
                        requires=self._transfer_arg_all(t),
                        storage_path=self.result_storage_path,
                        output_path=t.output,
                        work_dir=f"/tmp/workdir/{t.task_id}",
                    ),
                )
                assert not node.is_generator
                assert node.is_persistent_node
                yield InvokeSignal("on_task_pull", event)
            else:
                raise ValueError(f"{t.task_type}")

    def on_gen_finish(self, event: CallBackEvent):
        self.sc.set_finished(event.task_id, False)
        yield from self._issue_new(event.run_id)

    def on_task_finish(self, event: CallBackEvent):
        if event.task_name == ROOT_NODE.NAME:
            yield from self._issue_new(event.run_id)
            return
        assert event.task_spec is not None
        self.sc.set_finished(event.task_id)
        # if event.task_spec.output_path is not None:
        #     self.latest_result[event.task_name] = event.task_spec.output_path
        yield from self._issue_new(event.run_id)

    def on_task_error(self, event: CallBackEvent):
        logger.error(
            f"{event.task_name} {event.run_id} {event.task_id}",
            stack_info=True,
        )
        raise NotImplementedError


def run(
    tasks: List[str],
    run_id: Optional[str] = None,
    storage_result: str = "file:default",
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
        storage_result,
        [
            EndTask(),
            # FinishChecker(storage_result),
            # RootStarter(),
            PRunner(),
            LocalRunner(),
            # FileWatcher(),
        ],
    )

    scheduler.feed(run_id, payload)
    logger.info("feed end")
    #
    # # # TODO add to test
    # list(scheduler.sc.get_ready_to_issue())
    # scheduler.sc.set_finished("_tt_2")
    # list(scheduler.sc.get_ready_to_issue())
    # scheduler.sc.set_finished("_tt_3")
    # list(scheduler.sc.get_ready_to_issue())
    # scheduler.sc.set_finished("_tt_4")
    # list(scheduler.sc.get_ready_to_issue())
    # scheduler.sc.set_finished("_tt_5")
    # list(scheduler.sc.get_ready_to_issue())
    # scheduler.sc.set_finished("_tt_7")
    # list(scheduler.sc.get_ready_to_issue())
    with open("./out.md", "w") as f:
        f.write(scheduler.sc.graph_str())

    with open("run_dep.md", "w") as f:
        f.write("```mermaid\n")
        f.write("graph TD\n")
        for a, b in scheduler.cb.call_graph:
            f.write(f"{a}-->{b}\n")
        f.write("```")
    logger.info("run end")
    return


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
