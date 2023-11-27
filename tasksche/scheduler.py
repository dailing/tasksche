import asyncio
import os
import pickle
import uuid
from collections import defaultdict
from functools import cached_property
from typing import Any, Callable, Dict, List, Optional, TypeVar

from tasksche.cbs.EndTask import EndTask
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

# TypeVar to represent any return type of the function
T = TypeVar("T")


class LazyProperty:
    def __init__(
        self,
        func: Callable[[Any, T, Any], T],
        updating_member: str,
        default_factory: Optional[Callable[..., T]] = None,
    ) -> None:
        self.func = func
        self.attr_name = None
        self.updating_member = updating_member
        self.last_index = 0
        self.default_factory = default_factory

    def __set_name__(self, owner, name):
        if self.attr_name is None:
            self.attr_name = name
        elif name != self.attr_name:
            raise TypeError(
                "Cannot assign to two different names "
                f"({self.attr_name!r} and {name!r})."
            )

    @cached_property
    def _attr_name_(self):
        assert self.attr_name is not None
        return f"__{self.attr_name}__value__"

    def __get__(self, instance: Any, owner: Any):
        if instance is None:
            return self
        member = getattr(instance, self.updating_member, None)
        assert member is not None
        updated_member = member[self.last_index :]
        self.last_index = len(member)
        if hasattr(instance, self._attr_name_):
            val = getattr(instance, self._attr_name_)
        elif self.default_factory is not None:
            val = self.default_factory()
        else:
            raise AttributeError("default_factory must be set")
        if len(updated_member) == 0:
            return val
        val = self.func(instance, val, updated_member)
        setattr(instance, self._attr_name_, val)
        return val


def lazy_property(
    updating_member: str,
    default_factory: Optional[Callable[..., T]] = None,
) -> Callable[[Callable[[Any, Optional[T], Any], T]], LazyProperty]:
    def wrapper(func: Callable[[Any, Optional[T], Any], T]) -> LazyProperty:
        return LazyProperty(func, updating_member, default_factory)

    return wrapper


class Sc2:
    @lazy_property("event_log", dict)
    def _output_dict(
        self,
        payload: Optional[Dict[str, str]],
        updated: List[TaskIssueInfo],
    ) -> Dict[str, str]:
        if payload is None:
            payload = {}
        for k in updated:
            if k.output is not None:
                payload[k.task_id] = k.output
        return payload

    @lazy_property("event_log", dict)
    def pending_events(
        self,
        pending_events_: Dict[str, TaskIssueInfo],
        updated: List[TaskIssueInfo],
    ) -> Dict[str, TaskIssueInfo]:
        for k in updated:
            pending_events_[k.task_id] = k
            # logger.debug(k)
        return pending_events_

    @lazy_property("event_log", lambda: defaultdict(lambda: 0))
    def task_cnt(
        self,
        val: Optional[Dict[str, int]],
        updated: List[TaskIssueInfo],
    ) -> Dict[str, int]:
        for k in updated:
            val[k.task_name] += 1
        return val

    @lazy_property("event_log", dict)
    def _last_iter(
        self,
        val: Optional[Dict[str, str]],
        updated: List[TaskIssueInfo],
    ):
        for k in updated:
            if k.task_type in (
                ISSUE_TASK_TYPE.START_GENERATOR,
                ISSUE_TASK_TYPE.START_ITERATOR,
                ISSUE_TASK_TYPE.ITER_TASK,
            ):
                val[k.task_name] = k.task_id
        return val

    @lazy_property("event_log", dict)
    def _last_push(self, val, updated):
        for k in updated:
            if k.task_type == ISSUE_TASK_TYPE.PUSH_TASK:
                from_task = self.pending_events[k.wait_for[0]].task_name
                push_pair = (k.task_name, from_task)
                val[push_pair] = k.task_id
        return val

    def __init__(self, graph: Graph) -> None:
        self.graph = graph
        self.finished_id = set()
        self.event_log: List[TaskIssueInfo] = []
        self.issued_task_id = set()
        self.finished_task_name = set()

        self.schedule_for()

    @cached_property
    def code_hash(self) -> Dict[str, str]:
        return {k: v.code_hash for k, v in self.graph.node_map.items()}

    def dump(self) -> bytes:
        return pickle.dumps(
            (
                self.event_log,
                self.finished_task_name,
                self.code_hash,
            )
        )

    def load(self, payload: bytes):
        event_log, finished_id, finished_task_name, code_hash = pickle.loads(
            payload
        )
        dirty_map = {}

        def _dirty(task_name: str):
            if task_name not in self.graph.node_map:
                return True
            if task_name in dirty_map:
                return dirty_map[task_name]
            dirty = False
            for dep in self.graph.node_map[task_name].depend_on:
                if _dirty(dep):
                    dirty = True
                break
            if not dirty and self.code_hash[task_name] != code_hash[task_name]:
                dirty = True
            dirty_map[task_name] = dirty
            return dirty

        for task in finished_task_name:
            if not _dirty(task):
                self.finished_task_name.add(task)
        for event in event_log:
            if event.task_name in self.finished_task_name:
                self.event_log.append(event)
                self.finished_id.add(event.task_id)

    def get_issue_info(self, task_id: str):
        return self.pending_events[task_id]

    def graph_str(self):
        s = "```mermaid\n"
        s += "graph TD\n"
        for task in self.event_log:
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
            self.finished_task_name.add(task_info.task_name)
        if node.is_generator and generate:
            self.schedule_for()

    def in_pending(self, task_name: str):
        for t in self.event_log:
            if (
                (t.task_name == task_name)
                and (t.task_id not in self.finished_id)
                and (t.output is not None)
            ):
                return True
        return False

    def task_started(self, task_name: str):
        for t in self.event_log:
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

        not_specified = object()

        def pend_task(
            task_type: ISSUE_TASK_TYPE,
            wait_for_: tuple = (),
            output: Optional[Any] = not_specified,
        ) -> TaskIssueInfo:
            if task_name_ == END_NODE.NAME:
                return None
            task_name = task_name_
            if output is not_specified:
                output = f"{task_name}_{self.task_cnt[task_name]:03d}.out"
            task_ = TaskIssueInfo(
                task_name=task_name,
                task_type=task_type,
                wait_for=wait_for_,
                output=output,
                task_id=f"{task_name}_{self.task_cnt[task_name]:03d}",
            )
            if task_.output is not None:
                ready_dict[task_.task_name] = task_
            if task_.task_type in (
                ISSUE_TASK_TYPE.ITER_TASK,
                ISSUE_TASK_TYPE.PULL_RESULT,
            ):
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
                if task_type in (
                    ISSUE_TASK_TYPE.START_GENERATOR,
                    ISSUE_TASK_TYPE.START_ITERATOR,
                ) and (arg.arg_type == ARG_TYPE.TASK_ITER):
                    requirements[arg_key] = None

            task_.reqs = requirements
            logger.info(f"[Pending        ][              ]{str(task_)}")
            self.event_log.append(task_)
            return task_

        node = self.graph.node_map[task_name_]
        starter = None

        if not self.task_started(task_name_):
            wait_for = []
            for task in node.depend_on_no_virt:
                w = self.schedule_for(task, ready_dict)
                if w is not None:
                    wait_for.append(w)
            wait_for = tuple(
                t.task_id
                for t in wait_for
                if t.task_name in node.TASK_OUTPUT_DEPEND_ON
            )
            if node.is_generator:
                # task_info.output = None
                starter = pend_task(
                    task_type=ISSUE_TASK_TYPE.START_GENERATOR,
                    wait_for_=wait_for,
                    output=None,
                )
            elif node.is_persistent_node:
                # task_info.output = None
                starter = pend_task(
                    task_type=ISSUE_TASK_TYPE.START_ITERATOR,
                    wait_for_=wait_for,
                    output=None,
                )
                # starter = pend_task(task_info)
                starter = pend_task(
                    task_type=ISSUE_TASK_TYPE.PULL_RESULT,
                    wait_for_=(starter.task_id,),
                )
                # )
            else:
                starter = pend_task(
                    task_type=ISSUE_TASK_TYPE.NORMAL_TASK, wait_for_=wait_for
                )
                return starter
            assert starter is not None

        for k in node.TASK_ITER_DEPEND_ON:
            task = self.schedule_for(k, ready_dict)
            if task is not None:
                push_pair = (task_name_, task.task_name)
                wait_for_ = (task.task_id,)
                if push_pair in self._last_push:
                    wait_for_ = wait_for_ + (self._last_push[push_pair],)
                pend_task(
                    task_type=ISSUE_TASK_TYPE.PUSH_TASK,
                    wait_for_=wait_for_,
                    output=None,
                )
        if starter is None:
            for k in node.TASK_OUTPUT_DEPEND_ON:
                task = self.schedule_for(k, ready_dict)
                if task is not None:
                    assert not node.is_persistent_node, node
                    return pend_task(
                        task_type=ISSUE_TASK_TYPE.NORMAL_TASK,
                        wait_for_=(task.task_id,),
                    )
        if (
            node.is_generator
            and not self.in_pending(task_name_)
            and (task_name_ not in self.finished_task_name)
        ):
            return pend_task(
                task_type=ISSUE_TASK_TYPE.ITER_TASK,
            )

    def get_ready_to_issue(self):
        for event in self.event_log:
            if event.task_name == END_NODE.NAME:
                continue
            if event.task_id in self.issued_task_id:
                continue
            if not all(map(lambda x: x in self.finished_id, event.wait_for)):
                continue
            self.issued_task_id.add(event.task_id)
            logger.info(f"[issued         ][              ]{str(event)}")
            yield event
        if len(self.finished_id) >= len(self.event_log):
            for e in self.event_log:
                if e.task_id not in self.finished_id:
                    return
            yield TaskIssueInfo(
                task_name="END_NODE.NAME",
                task_type=ISSUE_TASK_TYPE.END_TASK,
            )


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
        self.sc = Sc2(graph)

        self.latest_result = dict()

    def dump(self) -> bytes:
        return pickle.dumps((self.sc.dump(),))

    def load(self, payload: bytes):
        (sc_dump,) = pickle.loads(payload)
        self.sc.load(sc_dump)

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

    def _transfer_arg_all(
        self, task: TaskIssueInfo, search_past: bool = False
    ):
        node = self.graph.node_map[task.task_name]
        reqs = {}
        for k, arg in node.dep_arg_parse.items():
            if arg.arg_type == ARG_TYPE.VIRTUAL:
                continue
            path = task.reqs.get(k, None)
            # if not search_past and path is None:
            #     continue
            # if path is None:
            #     path = self.latest_result.get(arg.from_task, None)
            # if arg.arg_type == ARG_TYPE.TASK_ITER and search_past:
            #     path = None
            # assert path is not None or arg.arg_type != ARG_TYPE.TASK_OUTPUT
            if arg.arg_type == ARG_TYPE.TASK_OUTPUT and path is None:
                continue
            reqs[k] = RunnerArgSpec(
                arg_type=arg.arg_type,
                value=arg.value,
                storage_path=path,
            )
        return reqs

    def _issue_new(self, run_id):
        pending_tasks = list(self.sc.get_ready_to_issue())
        for t in pending_tasks:
            # node = self.graph.node_map[t.task_name]
            if t.task_name == ROOT_NODE.NAME:
                continue
            if t.task_name == END_NODE.NAME:
                continue
            if t.task_type is ISSUE_TASK_TYPE.END_TASK:
                yield InvokeSignal(
                    "on_run_finish",
                    CallBackEvent(
                        run_id=run_id,
                        task_id=t.task_id,
                        task_name=t.task_name,
                        task_spec=None,
                    ),
                )
                return
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
                    task_type=t.task_type,
                    task_id=t.task_id,
                ),
            )
            yield InvokeSignal("on_task_start", event)

    def on_gen_finish(self, event: CallBackEvent):
        self.sc.set_finished(event.task_id, False)
        # logger.info(self.sc.finished_task_name)
        yield from self._issue_new(event.run_id)

    def on_task_finish(self, event: CallBackEvent):
        if event.task_name == ROOT_NODE.NAME:
            yield from self._issue_new(event.run_id)
            return
        # assert event.task_spec is not None
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
            # PRunner(),
            LocalRunner(),
            # FileWatcher(),
        ],
    )
    # if os.path.exists("dump.pkl"):
    #     logger.info("load dump")
    #     scheduler.load(open("dump.pkl", "rb").read())
    scheduler.feed(run_id, payload)
    logger.info("feed end")
    with open("dump.pkl", "wb") as f:
        logger.info(os.path.abspath("dump.pkl"))
        f.write(scheduler.dump())
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
    from pprint import pprint

    pprint(scheduler.sc.pending_events)
    pprint(scheduler.sc.finished_id)
    with open("./out_.md", "w") as f:
        f.write(scheduler.sc.graph_str())

    with open("run_dep.md", "w") as f:
        f.write("```mermaid\n")
        f.write("graph TD\n")
        for a, b in scheduler.cb.call_graph:
            f.write(f"{a}-->{b}\n")
        f.write("```")
    logger.info("run end")
    pprint(scheduler.sc._output_dict)

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
