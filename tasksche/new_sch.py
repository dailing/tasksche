from collections import defaultdict
from dataclasses import dataclass
import hashlib
import os.path
import pickle
from enum import auto, Enum
from functools import cached_property
from typing import Any, Dict, Generator, List, Optional, Self, Set

from pydantic import BaseModel, Field, model_validator

from .logger import Logger

from .functional import (
    ARG_TYPE,
    task_name_to_file_path,
    TaskSpecFmt,
    RequirementArg,
    process_path,
    parse_task_specs,
    TASK_TYPE,
    _Counter,
    lazy_property,
    EVENT_TYPE,
)

logger = Logger()


class TaskSpec:
    def __init__(
        self,
        task_name: str,
        task_root: str,
        task_spec: Optional[TaskSpecFmt] = None,
    ) -> None:
        assert task_name is not None
        self._task_spec = task_spec
        self.task_name = task_name
        self.task_root = task_root

    def _parse_arg_str(self, arg) -> RequirementArg:
        if isinstance(arg, str) and arg.startswith("$"):
            return RequirementArg(
                arg_type=ARG_TYPE.TASK_OUTPUT,
                from_task=process_path(self.task_name, arg[1:]),
            )
        return RequirementArg(arg_type=ARG_TYPE.RAW, value=arg)

    @cached_property
    def requires(self) -> Dict[int | str, RequirementArg]:
        # if self.task_spec is list, convert it to dict
        arg_dict = self.task_spec.require
        if isinstance(arg_dict, list):
            arg_dict = {i: arg for i, arg in enumerate(arg_dict)}
        assert isinstance(arg_dict, dict)
        arg_dict = {k: self._parse_arg_str(v) for k, v in arg_dict.items()}
        # add iter type from iter_args to arg_dict
        if self.task_spec.iter_args is not None:
            for k in self.task_spec.iter_args:
                assert k in arg_dict
                arg_dict[k].arg_type = ARG_TYPE.TASK_ITER
        return arg_dict

    @cached_property
    def args(self) -> List[RequirementArg]:
        max_length = (
            max([k for k in self.requires.keys() if isinstance(k, int)] + [-1])
            + 1
        )
        return [self.requires[k] for k in range(max_length)]

    @cached_property
    def kwargs(self):
        return {k: v for k, v in self.requires.items() if isinstance(k, str)}

    @cached_property
    def iter_args(self) -> Dict[int | str, RequirementArg]:
        return {
            k: v
            for k, v in self.requires.items()
            if v.arg_type == ARG_TYPE.TASK_ITER
        }

    @cached_property
    def task_spec(self) -> TaskSpecFmt:
        if self._task_spec is None:
            return parse_task_specs(self.task_name, self.task_root)
        return self._task_spec

    @cached_property
    def is_generator(self) -> bool:
        return self.task_spec.task_type == TASK_TYPE.GENERATOR

    @cached_property
    def depend_on(self) -> List[str]:
        return [
            val.from_task
            for val in self.requires.values()
            if (
                val.arg_type
                in [
                    ARG_TYPE.TASK_OUTPUT,
                    ARG_TYPE.TASK_ITER,
                ]
                and val.from_task is not None
            )
        ]

    @cached_property
    def is_persistent(self):
        return self.is_generator or len(self.iter_args) > 0

    @cached_property
    def code_file(self) -> str:
        return task_name_to_file_path(self.task_name, self.task_root)

    @cached_property
    def code_hash(self) -> str:
        with open(self.code_file, "rb") as f:
            return hashlib.md5(f.read()).hexdigest()

    def __repr__(self):
        return f"<TaskSpec: {self.task_name} <<-- {self.depend_on}>"


@dataclass
class GraphNodeBase:
    node: TaskSpec
    depend_on: Dict[str | int, str]
    node_type: EVENT_TYPE


_command_id_cnt = _Counter(prefix="command_")
_output_id_cnt = _Counter(prefix="output_")


class ScheEvent(BaseModel):
    cmd_type: EVENT_TYPE
    task_name: str
    args: Dict[str | int, str]  # kward-> output_id
    output: Optional[str] = Field(default_factory=_output_id_cnt.get_counter)
    command_id: str = Field(default_factory=_command_id_cnt.get_counter)
    exec_after: List[str] = Field(default_factory=list)
    process_id: str

    @model_validator(mode="after")
    def _check_fields_for_finish_event(self):
        if self.cmd_type == EVENT_TYPE.FINISH:
            assert self.command_id is not None

    def __repr__(self) -> str:
        base_str = (
            f"<ScheEvent> {self.command_id:15s} [{self.task_name:20s}] ->"
            f" {self.output if self.output is not None else ''}"
        )
        if len(self.args) > 0:
            base_str += "\n"
            base_str += "\n".join(
                " " * 55 + f"{k}:{v}" for k, v in self.args.items()
            )
        if len(self.exec_after) > 0:
            base_str += "\n" + " " * 55 + ",".join(self.exec_after)
        return base_str


class Graph:
    def __init__(self, root: str, target_tasks: List[str]):
        self.root = os.path.abspath(root)
        self.target_tasks: List[str] = target_tasks

    def _build_node(self, task_name: str, node_map: Dict[str, GraphNodeBase]):
        if f"call:{task_name}" in node_map:
            return node_map[f"call:{task_name}"]
        node = TaskSpec(task_name, self.root)
        call_dep = {}
        for k, v in node.requires.items():
            if v.arg_type == ARG_TYPE.TASK_OUTPUT:
                assert v.from_task is not None
                self._build_node(v.from_task, node_map)
                call_dep[k] = f"pop:{v.from_task}"
        node_map[f"call:{task_name}"] = GraphNodeBase(
            node, call_dep, node_type=EVENT_TYPE.CALL
        )
        node_map[f"pop:{task_name}"] = GraphNodeBase(
            node,
            {"-start": f"call:{task_name}"},
            node_type=EVENT_TYPE.POP_AND_NEXT
            if node.is_generator
            else EVENT_TYPE.POP,
        )
        for k, v in node.requires.items():
            if v.arg_type == ARG_TYPE.TASK_ITER:
                assert v.from_task is not None
                self._build_node(v.from_task, node_map)
                pop_task = f"pop:{v.from_task}"
                node_map[f"push_{k}:{task_name}"] = GraphNodeBase(
                    node,
                    {k: pop_task, "-start": f"call:{task_name}"},
                    EVENT_TYPE.PUSH,
                )
        return node_map[f"call:{task_name}"]

    @cached_property
    def node_map(self) -> Dict[str, GraphNodeBase]:
        nmap: Dict[str, GraphNodeBase] = {}
        for task_name in self.target_tasks:
            self._build_node(task_name, nmap)
        return nmap

    def _secq(self, nodes=None, name=None):
        if nodes is None:
            nodes = []
            for k in self.node_map:
                self._secq(nodes, k)
            return nodes
        if name in nodes:
            return
        node = self.node_map[name]
        for k in node.depend_on.values():
            self._secq(nodes, k)
        nodes.append(name)

    @cached_property
    def handle_secquence(self):
        return self._secq()

    @cached_property
    def child_node_map(self):
        child_nodes = defaultdict(list)
        for child_name, node in self.node_map.items():
            for key, parent_node in node.depend_on.items():
                if isinstance(key, str) and key.startswith("-"):
                    continue
                child_nodes[parent_node].append(child_name)
        return child_nodes


class Storage:
    def __init__(self, storage_path):
        self.path = storage_path
        if not os.path.exists(self.path):
            os.makedirs(self.path, exist_ok=True)
        assert os.path.exists(self.path)

    def set(self, key, value):
        assert isinstance(key, str)
        with open(os.path.join(self.path, key), "wb") as f:
            pickle.dump(value, f)

    def get(self, key):
        assert isinstance(key, str)
        with open(os.path.join(self.path, key), "rb") as f:
            return pickle.load(f)


class N_Scheduler:
    def __init__(self, graph: Graph):
        self.graph = graph
        self.storage = Storage(os.path.join(graph.root, "__default"))
        self.event_log: List[ScheEvent] = []
        self.pending_task_id: Set[str] = set()
        self.runnint_task_id: Set[str] = set()

    def event_log_to_md(self, output_file):
        new_line = "\n"
        with open(output_file, "w") as f:
            f.write("```mermaid\n")
            f.write("graph TD\n")
            for event in self.event_log:
                f.write(
                    f"{event.command_id}"
                    f'["{event.task_name}\n'
                    f"{event.command_id}\n"
                    f"{event.cmd_type}\n"
                    f'{(str(self.storage.get(event.output))+new_line) if event.output is not None and event.command_id not in (self.runnint_task_id|self.pending_task_id) else ""}'
                    f"PID {event.process_id}\n"
                    f'"]\n'
                )
                for dep in event.exec_after:
                    f.write(f"{dep} -->{event.command_id}\n")
            f.write("```\n")

    @lazy_property(updating_member="event_log", default_factory=dict)
    def output_map(
        self, payload: Optional[Dict[str, bool]], updated: List[ScheEvent]
    ):
        """
        maps output_id --> commandObject
        """
        assert payload is not None
        for event in updated:
            if event.output is not None:
                payload[event.output] = event
        return payload

    @lazy_property(updating_member="event_log", default_factory=dict)
    def task_id_map(
        self, payload: Optional[Dict[str, ScheEvent]], updated: List[ScheEvent]
    ) -> Dict[str, ScheEvent]:
        assert payload is not None
        for event in updated:
            if event.command_id is not None:
                payload[event.command_id] = event
        return payload

    @lazy_property(updating_member="event_log", default_factory=dict)
    def last_event(
        self, payload: Optional[Dict[str, ScheEvent]], updated: List[ScheEvent]
    ):
        assert payload is not None
        for event in updated:
            if event.cmd_type == EVENT_TYPE.POP:
                assert (
                    event.task_name not in payload
                    or payload[event.task_name].cmd_type == EVENT_TYPE.POP
                )
            elif event.cmd_type in (
                EVENT_TYPE.GENERATOR_END,
                EVENT_TYPE.FINISH,
            ):
                assert (
                    event.task_name in payload
                    and payload[event.task_name].cmd_type == EVENT_TYPE.POP
                )
            payload[event.task_name] = event
        return payload

    @lazy_property(updating_member="event_log", default_factory=dict)
    def started_map(
        self, payload: Optional[Dict[str, bool]], updated: List[ScheEvent]
    ):
        assert payload is not None
        for event in updated:
            payload[event.task_name] = True
        return payload

    def pend_task(
        self,
        event_type: EVENT_TYPE,
        task_name: str,
        args: Dict[str | int, str],
        depend_on: List[str],
        process_id: str = "",
    ) -> ScheEvent:
        event = ScheEvent(
            cmd_type=event_type,
            task_name=task_name,
            args=args,
            process_id=process_id,
        )
        if process_id == "":
            event.process_id = event.command_id
        if event_type not in (
            EVENT_TYPE.POP,
            EVENT_TYPE.POP_AND_NEXT,
        ):
            event.output = None
        event.exec_after.extend(depend_on)
        self.event_log.append(event)
        self.pending_task_id.add(event.command_id)
        return event

    def sche_init(self):
        """
        generate initial starter tasks
        TODO: handle if laoding finished tasks by initial stater from the
            first output
        """
        self.sche_once()

    def sche_once(self, _output_dict=None):
        output_dict: Dict[str, List[ScheEvent]] = defaultdict(list)
        if _output_dict is not None:
            output_dict.update(_output_dict)
        for task_name in self.graph.handle_secquence:
            node = self.graph.node_map[task_name]
            started = self.started_map.get(task_name, False)
            args = {}
            depend_on = []
            start_index = 0
            if not started:
                for k, v in node.depend_on.items():
                    assert v in output_dict
                    assert len(output_dict[v]) > 0
                    if isinstance(k, int) or not k.startswith("-"):
                        assert output_dict[v][0].output is not None
                        args[k] = output_dict[v][0].output
                    depend_on.append(output_dict[v][0].command_id)
                # process_id =
                t = self.pend_task(
                    node.node_type,
                    task_name,
                    args,
                    depend_on=depend_on,
                    process_id=node.node.task_name,
                )
                output_dict[task_name].append(t)
                start_index = 1

            for k, req_arg in node.node.requires.items():
                v = node.depend_on.get(k, None)
                if v not in output_dict:
                    continue
                for from_event in output_dict[v][start_index:]:
                    if req_arg.arg_type == ARG_TYPE.TASK_OUTPUT:
                        assert not node.node.is_generator
                        assert len(node.node.iter_args) == 0
                        init_call = self.pend_task(
                            EVENT_TYPE.CALL,
                            task_name,
                            args={k: from_event.output},
                            depend_on=[from_event.command_id],
                        )
                        task_name = task_name.replace("call:", "pop:")
                        new_evt = self.pend_task(
                            EVENT_TYPE.POP,
                            task_name,
                            args={},
                            depend_on=[init_call.command_id],
                            process_id=init_call.command_id,
                        )
                    elif req_arg.arg_type == ARG_TYPE.TASK_ITER:
                        new_evt = self.pend_task(
                            EVENT_TYPE.PUSH,
                            task_name,
                            args={k: from_event.output},
                            depend_on=[
                                from_event.command_id,
                                self.last_event[task_name].command_id,
                            ],
                            process_id=node.node.task_name,
                        )
                    else:
                        raise NotImplemented
                    if new_evt.output is not None:
                        output_dict[task_name].append(new_evt)

    def set_finish_command(self, task_id: str, generate=True):
        assert task_id in self.pending_task_id
        self.pending_task_id.remove(task_id)
        self.runnint_task_id.remove(task_id)
        event = self.task_id_map[task_id]
        if event.cmd_type != EVENT_TYPE.POP_AND_NEXT or not generate:
            return
        new_event = self.pend_task(
            EVENT_TYPE.POP_AND_NEXT,
            event.task_name,
            args={},
            depend_on=[event.command_id],
            process_id=self.graph.node_map[event.task_name].node.task_name,
        )
        output_dict = defaultdict(list)
        output_dict[new_event.task_name].append(new_event)
        self.sche_once(output_dict)

    def get_issue_tasks(self) -> Generator[ScheEvent, None, None]:
        for task_id_to_check in list(
            self.pending_task_id - self.runnint_task_id
        ):
            task = self.task_id_map[task_id_to_check]
            for dep in task.exec_after:
                if dep in self.pending_task_id:
                    break
            else:
                yield self.task_id_map[task_id_to_check]
                self.runnint_task_id.add(task_id_to_check)


if __name__ == "__main__":
    from pprint import pprint

    print("food")
    sche = N_Scheduler(
        # Graph("/home/d/Sync/tasksche/test/simple_task_set", ["/task4"])
        # Graph("/home/d/Sync/tasksche/test/generator_task_set", ["/task4"])
        Graph("test/loop_task", ["/task4"])
    )
    pprint(sche.graph.node_map)
    # print(sche.graph.handle_secquence)
    sche.sche_init()
    # pprint(sche.event_log)
    print(list(sche.get_issue_tasks()))
    print("-----------------------")
    sche.set_finish_command("command__003")
    # pprint(sche.event_log)
    pprint(list(sche.get_issue_tasks()))
    sche.set_finish_command("command__004")
    sche.event_log_to_md("event_log.md")

    # print("-----------------------")
    # sche.set_finish_command("command__003")
    # pprint(sche.event_log)
    # pprint(list(sche.get_issue_tasks()))
    # print("-----------------------")
    # sche.set_finish_command("command__004")
    # pprint(sche.event_log)
    # pprint(list(sche.get_issue_tasks()))
