import asyncio
import os
import pickle
import uuid
from collections import defaultdict
from functools import cached_property
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar

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
    GroupOfTaskInfo,
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
                payload[k.output] = k.task_id
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
        """
        records the last iteration task
        maps task_name -> task_id of last iter or start task
        this function is used to add dependency to following iter tasks
        so the iter is done in order
        """
        for k in updated:
            if k.task_type in (
                ISSUE_TASK_TYPE.START_GENERATOR,
                ISSUE_TASK_TYPE.ITER_TASK,
            ):
                val[k.task_name] = k.task_id
        return val

    @lazy_property("event_log", dict)
    def _mail_box(
        self,
        val: Dict[str, List[TaskIssueInfo]],
        updated: List[TaskIssueInfo],
    ):
        for i in updated:
            for k in self.graph.child_map[i.task_name]:
                if k not in val:
                    val[k] = [i]
                else:
                    val[k].append(i)
        return val

    @lazy_property("event_log", dict)
    def task_started(self, val: Dict[str, bool], updated: List[TaskIssueInfo]):
        for t in updated:
            val[t.task_name] = True
        return val

    @lazy_property("event_log", dict)
    def latest_parameter(
        self,
        val: Dict[str, Dict[int | str, str]],
        updated: List[TaskIssueInfo],
    ):
        """maps task_name.req_key.task_id"""
        for t in updated:
            if t.task_name not in val:
                val[t.task_name] = {}
            val[t.task_name].update(t.reqs)
        return val

    @lazy_property("event_log", dict)
    def _last_push(
        self,
        val: Dict[Tuple[str, str | int], str],
        updated: List[TaskIssueInfo],
    ):
        """
        returds latese push of (task_name, push_key) -> push task_id
        This function is used to add dependency to following push tasks
        so the push is done in order
        """
        for k in updated:
            if k.task_type == ISSUE_TASK_TYPE.PUSH_TASK:
                for req_key, req_body in k.reqs.items():
                    val[(k.task_name, req_key)] = k.task_id
            elif k.task_type in (
                ISSUE_TASK_TYPE.START_ITERATOR,
                ISSUE_TASK_TYPE.START_GENERATOR,
            ):
                # add initial dependent tasks
                for req_key in self.graph.node_map[
                    k.task_name
                ].task_iter_depend_on_.keys():
                    val[(k.task_name, req_key)] = k.task_id
        return val

    def __init__(self, graph: Graph) -> None:
        self.graph = graph
        self.finished_id = set()
        self.event_log: List[TaskIssueInfo] = []
        self.issued_task_id = set()
        self.finished_task_name = set()

        # self.schedule_for()

    @cached_property
    def code_hash(self) -> Dict[str, str]:
        return {k: v.code_hash for k, v in self.graph.node_map.items()}

    def check_task_finished(self, task_name):
        """check if this task is finished and no more sub-tasks tobe run"""
        if task_name in self.finished_task_name:
            return True
        for t in self.graph.node_map[task_name].depend_on_no_virt:
            if not self.check_task_finished(t):
                return False
        task_cnt = 0
        for tasks in self.event_log:
            if tasks.task_name == task_name:
                task_cnt += 1
                if tasks.task_id not in self.finished_id:
                    return False
        return False

    def dump(self):
        for t in self.graph.node_map.keys():
            self.check_task_finished(t)
        return (
            self.event_log,
            self.finished_task_name,
            self.code_hash,
        )

    def load(self, payload):
        event_log, finished_task_name, code_hash = payload
        # logger.info(finished_task_name)
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
                logger.info(f"{task_name} code changed code hash not match")
                dirty = True
            dirty_map[task_name] = dirty
            return dirty

        _dirty(END_NODE.NAME)
        for task, is_dirty in dirty_map.items():
            if is_dirty:
                logger.info(f"{task} is dirty reruning")

        self.finished_task_name.update(
            filter(lambda x: not dirty_map[x], dirty_map.keys())
        )
        # logger.info(self.finished_task_name)
        self.event_log = list(
            filter(lambda x: x.task_name in self.finished_task_name, event_log)
        )
        self.finished_id.update((x.task_id for x in self.event_log))
        self.issued_task_id.update(self.finished_id)
        # logger.info(self.finished_id)
        self.schedule_for()
        self.get_ready_to_issue()
        # sys.exit(0)

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

    def schedule_for(
        self, task_name_: Optional[str] = None, ready_dict=None, req=None
    ) -> Optional[TaskIssueInfo]:
        if ready_dict is None:
            ready_dict = {}
        if task_name_ is None:
            task_name_ = END_NODE.NAME
        if task_name_ in ready_dict:
            return
        if self.check_task_finished(task_name_):
            # logger.info(f"{task_name_} finished")
            return
        not_specified = object()
        ready_dict[task_name_] = True
        for i in self.graph.node_map[task_name_].depend_on:
            self.schedule_for(i, ready_dict)
        if task_name_ == END_NODE.NAME or task_name_ == ROOT_NODE.NAME:
            return

        def pend_task(
            task_type: ISSUE_TASK_TYPE,
            wait_for_: tuple = (),
            require_: Dict[str | int, str] = not_specified,
            output: Optional[Any] = not_specified,
        ) -> TaskIssueInfo:
            if require_ is not_specified:
                require_ = {}
            if task_name_ == END_NODE.NAME:
                return None
            assert task_name_ is not None
            if output is not_specified:
                output = f"{task_name_}_{self.task_cnt[task_name_]:03d}.out"
            wait_for_ = wait_for_ + tuple(
                self._output_dict[x] for x in require_.values()
            )
            task_ = TaskIssueInfo(
                task_name=task_name_,
                task_type=task_type,
                wait_for=wait_for_,
                output=output,
                reqs=require_,
                task_id=f"{task_name_}_{self.task_cnt[task_name_]:03d}",
            )
            logger.info(f"[Pending        ][              ]{str(task_)}")
            self.event_log.append(task_)
            return task_

        node = self.graph.node_map[task_name_]
        starter = None
        mails: List[TaskIssueInfo] = self._mail_box.get(task_name_, [])
        mail_group = GroupOfTaskInfo(mails)
        mails.clear()

        if not self.task_started.get(task_name_, False):
            reqs = mail_group.pop_reqs(node.task_output_depend_on_)

            if node.is_generator:
                # task_info.output = None
                starter = pend_task(
                    task_type=ISSUE_TASK_TYPE.START_GENERATOR,
                    require_=reqs,
                    output=None,
                )
            elif node.is_persistent_node:
                # task_info.output = None
                starter = pend_task(
                    task_type=ISSUE_TASK_TYPE.START_ITERATOR,
                    require_=reqs,
                    output=None,
                )
                starter = pend_task(
                    task_type=ISSUE_TASK_TYPE.PULL_RESULT,
                    wait_for_=(starter.task_id,),
                )
            else:
                starter = pend_task(
                    task_type=ISSUE_TASK_TYPE.NORMAL_TASK, require_=reqs
                )
            assert starter is not None

        for req_key, req_body, from_task_spec in mail_group.items(
            node.task_output_depend_on_
        ):
            assert not node.is_persistent_node
            assert from_task_spec.output is not None
            assert req_body.arg_type == ARG_TYPE.TASK_OUTPUT
            pend_task(
                task_type=ISSUE_TASK_TYPE.NORMAL_TASK,
                require_={req_key: from_task_spec.output},
            )

        for req_key, req_body, from_task_spec in mail_group.items(
            node.task_iter_depend_on_
        ):
            assert from_task_spec.output is not None
            assert req_body.arg_type == ARG_TYPE.TASK_ITER
            pend_task(
                task_type=ISSUE_TASK_TYPE.PUSH_TASK,
                require_={req_key: from_task_spec.output},
                wait_for_=(self._last_push[(task_name_, req_key)],),
                output=None,
            )
        if (
            node.is_generator
            and not self.in_pending(task_name_)
            and (task_name_ not in self.finished_task_name)
        ):
            pend_task(
                task_type=ISSUE_TASK_TYPE.ITER_TASK,
                wait_for_=(self._last_iter[task_name_],),
            )

    def get_ready_to_issue(self):
        if len(self.event_log) <= 0:
            self.schedule_for()
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

    def dump(self):
        self.result_storage.store("__LSAT_DUMP__", value=(self.sc.dump(),))

    def load(self):
        if "__LSAT_DUMP__" in self.result_storage:
            (sc_dump,) = self.result_storage.get("__LSAT_DUMP__")
            self.sc.load(sc_dump)

    def feed(self, payload: Any = None):
        event = CallBackEvent(
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

    def _issue_new(self):
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
                        task_id=t.task_id,
                        task_name=t.task_name,
                        task_spec=None,
                    ),
                )
                return
            event = CallBackEvent(
                task_id=t.task_id,
                task_name=t.task_name,
                task_spec=RunnerTaskSpec(
                    task=t.task_name,
                    root=self.graph.root,
                    requires=self._transfer_arg_all(t),
                    storage_path=self.result_storage_path,
                    output_path=t.output,
                    work_dir=f"{self.result_storage.storage_path}/{t.task_name}",
                    task_type=t.task_type,
                    task_id=t.task_id,
                ),
            )
            yield InvokeSignal("on_task_start", event)

    def on_gen_finish(self, event: CallBackEvent):
        self.sc.set_finished(event.task_id, False)
        # logger.info(self.sc.finished_task_name)
        yield from self._issue_new()

    def on_task_finish(self, event: CallBackEvent):
        if event.task_name != ROOT_NODE.NAME:
            self.sc.set_finished(event.task_id)
        yield from self._issue_new()

    def on_task_error(self, event: CallBackEvent):
        logger.error(
            f"{event.task_name} {event.task_id}",
            stack_info=True,
        )
        raise NotImplementedError


def run(
    tasks: List[str],
    storage_path: Optional[str] = None,
) -> None:
    if storage_path is None:
        storage_path = f"file:{os.getcwd()}/__default"
    tasks = [os.path.abspath(task) for task in tasks]
    root = search_for_root(tasks[0])
    assert isinstance(root, str)
    for task in tasks:
        assert task.endswith(".py")
        assert task.startswith(root)
    task_names = [task[len(root) : -3] for task in tasks]
    scheduler = Scheduler(
        Graph(root, task_names),
        storage_path,
        [
            # EndTask(),
            # FinishChecker(storage_result),
            # RootStarter(),
            # PRunner(),
            LocalRunner(),
            # FileWatcher(),
        ],
    )
    # if os.path.exists("dump.pkl"):
    #     logger.info("load dump")
    scheduler.load()
    scheduler.feed({})
    # logger.info("feed end")
    scheduler.dump()
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
    # from pprint import pprint

    # pprint(scheduler.sc.pending_events)
    # pprint(scheduler.sc.finished_id)
    with open("./out_.md", "w") as f:
        f.write(scheduler.sc.graph_str())

    with open("run_dep.md", "w") as f:
        f.write("```mermaid\n")
        f.write("graph TD\n")
        for a, b in scheduler.cb.call_graph:
            f.write(f"{a}-->{b}\n")
        f.write("```")
    logger.info("run end")
    # pprint(scheduler.sc._output_dict)

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
    )
