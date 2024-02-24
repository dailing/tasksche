import hashlib
import os.path
import pickle
import uuid
from collections import defaultdict
from dataclasses import dataclass
from functools import cached_property
from typing import Dict, Generator, List, Optional, Set

from pydantic import BaseModel, Field, model_validator

from .functional import (
    ARG_TYPE,
    EVENT_TYPE,
    TASK_TYPE,
    RequirementArg,
    TaskSpecFmt,
    lazy_property,
    parse_task_specs,
    process_path,
    task_name_to_file_path,
)
from .logger import Logger

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
        self.code_hash = hashlib.md5(
            open(self.code_file, "rb").read()
        ).hexdigest()

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
    def call_args(self) -> Dict[int | str, RequirementArg]:
        return {
            k: v
            for k, v in self.requires.items()
            if v.arg_type == ARG_TYPE.TASK_OUTPUT
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

    # @cached_property
    # def code_hash(self) -> str:
    #     with open(self.code_file, "rb") as f:
    #         return hashlib.md5(f.read()).hexdigest()

    def __repr__(self):
        return f"<TaskSpec: {self.task_name} <<-- {self.depend_on}>"


@dataclass
class GraphNodeBase:
    node: TaskSpec
    depend_on: Dict[str | int, str]
    node_type: EVENT_TYPE


# _command_id_cnt = _Counter(prefix="command_")
# _output_id_cnt = _Counter(prefix="output_")


class ScheEvent(BaseModel):
    cmd_type: EVENT_TYPE
    task_name: str
    args: Dict[str | int, str]  # kwards-> output_id
    output: Optional[str] = Field(default_factory=lambda: uuid.uuid4().hex)
    command_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
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

    def to_markdown(self, file_path: str):
        with open(file_path, "w") as f:
            f.write("```mermaid\n")
            f.write("graph TD\n")
            for node_name, node in self.node_map.items():
                f.write(f"{node_name}['{node_name}']\n")
                for dep in node.depend_on.values():
                    f.write(f"{dep} -->{node_name}\n")
            f.write("```\n")

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
            node_type=(
                EVENT_TYPE.POP_AND_NEXT
                if node.is_generator
                else EVENT_TYPE.POP
            ),
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

    @cached_property
    def handle_sequence(self):
        visist_order = []

        def _dfs_search(name: str):
            if name in visist_order:
                return
            node = self.node_map[name]
            for k in node.depend_on.values():
                _dfs_search(k)
            visist_order.append(name)

        for k in self.node_map:
            _dfs_search(k)
        return visist_order

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

    def __contains__(self, key):
        return os.path.exists(os.path.join(self.path, key))


class N_Scheduler:
    def __init__(self, graph: Graph, storage: Storage):
        self.graph = graph
        self.storage = storage
        self.event_log: List[ScheEvent] = []
        self.pending_task_id: Set[str] = set()
        self.running_task_id: Set[str] = set()
        self.error_task_id: Set[str] = set()
        self.fixed_tasks: Set[str] = set()

        self.check_for_term: Set[str] = set()
        self.issued_for_term: Set[str] = set()
        self.term_task_id_map: Dict[str, ScheEvent] = {}

    def dump(self):
        finished_tasks = self.finished_tasks()
        finished_events = []
        file_hash = {}
        for event in self.event_log:
            if event.task_name in finished_tasks:
                finished_events.append(event)
            node = self.graph.node_map[event.task_name]
            file_hash[node.node.task_name] = node.node.code_hash
        self.storage.set("__finished_events", finished_events)
        self.storage.set("__finished_tasks", finished_tasks)
        self.storage.set("__task_hash", file_hash)

    def reload(self):
        graph = Graph(self.graph.root, self.graph.target_tasks)
        # self.graph = graph
        # remove anything changed from the current record
        invalid_tasks = set()
        invalid_task_raw = set()
        for task_name in self.graph.handle_sequence:
            raw = task_name.split(":")[-1]
            if raw in invalid_task_raw:
                # already defined as invalid
                continue
            if task_name not in graph.node_map:
                # task deleted
                invalid_task_raw.add(raw)
                continue
            if (
                self.graph.node_map[task_name].node.code_hash
                != graph.node_map[task_name].node.code_hash
            ):
                # code changed
                invalid_task_raw.add(raw)
                continue
            for dep in self.graph.node_map[task_name].depend_on.values():
                task_raw = dep.split(":")[-1]
                if task_raw in invalid_task_raw:
                    # dependency changed
                    invalid_task_raw.add(raw)
                    break
        for i in self.graph.handle_sequence:
            raw_name = i.split(":")[-1]
            # logger.info(f"RELOAD {raw_name} {i}..")
            if raw_name in invalid_task_raw:
                # logger.info(f"reload {raw_name} invalid")
                invalid_tasks.add(i)
        if len(invalid_tasks) == 0:
            logger.info("check finished, no change")
            return
        logger.info(f"RELOAD invalid tasks: {invalid_tasks}")
        pending_task_names = set()
        running_task_names = set()
        for cmd_id in self.pending_task_id:
            cmd = self.task_id_map[cmd_id]
            assert cmd is not None
            pending_task_names.add(cmd.task_name)
            if cmd_id in self.running_task_id:
                running_task_names.add(cmd.task_name)
        finished_tasks = self.finished_tasks()
        logger.info(f"current finished tasks: {finished_tasks}")
        self.event_log = [
            e for e in self.event_log if e.task_name not in invalid_tasks
        ]
        self.reset_cached_lazy_property()
        self.pending_task_id = {
            i for i in self.pending_task_id if i in self.task_id_map
        }
        self.running_task_id = {
            i for i in self.running_task_id if i in self.task_id_map
        }
        self.error_task_id = {
            i for i in self.error_task_id if i in self.task_id_map
        }
        self.fixed_tasks = {
            i for i in self.fixed_tasks if i not in invalid_tasks
        }
        self.graph = graph
        # init new tasks
        self.fixed_tasks = finished_tasks - invalid_tasks
        logger.info(
            f"using finished tasks: {self.fixed_tasks} {invalid_tasks}"
        )
        self.sche_init()

        self.check_for_term.update(invalid_tasks)

        logger.info(f"checking for term: {self.check_for_term}")

    def load(self):
        if "__finished_events" not in self.storage:
            return
        finished_events = self.storage.get("__finished_events")
        finished_tasks = self.storage.get("__finished_tasks")
        file_hash = self.storage.get("__task_hash")
        for task_name in self.graph.handle_sequence:
            if task_name not in finished_tasks:
                continue
            node = self.graph.node_map[task_name]
            if node.node.code_hash != file_hash.get(node.node.task_name, None):
                finished_tasks.remove(task_name)
                continue
            for dep in node.depend_on.values():
                if dep not in finished_tasks:
                    finished_tasks.remove(task_name)
                    break
        finished_events = [
            e for e in finished_events if e.task_name in finished_tasks
        ]
        self.event_log.extend(finished_events)
        self.fixed_tasks.update(finished_tasks)

    def event_log_to_md(self, output_file):
        with open(output_file, "w") as f:
            f.write("```mermaid\n")
            f.write("graph TD\n")
            for event in self.event_log:
                output = None
                if event.output is not None and event.command_id not in (
                    self.running_task_id | self.pending_task_id
                ):
                    output = self.storage.get(event.output)
                output_str = str(output)
                f.write(
                    f"{event.command_id}"
                    f'["{event.task_name}\n'
                    f"{event.command_id}\n"
                    f"{event.cmd_type}\n"
                    f"OUTPUT: {output_str}\n"
                    f"PID {event.process_id}\n"
                    f'"]\n'
                )
                color = "#04cf41"
                if event.command_id in self.pending_task_id:
                    color = "#cccccc"
                elif event.command_id in self.running_task_id:
                    color = "#d4c604"
                elif event.command_id in self.error_task_id:
                    color = "#d44204"
                elif event.task_name in self.fixed_tasks:
                    color = "#ffffff"
                f.write(f"style {event.command_id} fill:{color}\n")
                for dep in event.exec_after:
                    f.write(f"{dep} -->{event.command_id}\n")
            f.write("```\n")

    def finished_tasks(self) -> Set[str]:
        retval = set()
        pending_tasks = set()
        for i in (
            self.pending_task_id | self.running_task_id | self.error_task_id
        ):
            pending_tasks.add(self.task_id_map[i].task_name)
        pending_task_suffix = set([k.split(":")[-1] for k in pending_tasks])
        for node_name in self.graph.handle_sequence:
            if node_name.split(":")[-1] in pending_task_suffix:
                continue
            node = self.graph.node_map[node_name]
            for dep in node.depend_on.values():
                if dep.split(":")[-1] in pending_task_suffix:
                    break
                if dep not in retval:
                    break
            else:
                retval.add(node_name)
        return retval

    @lazy_property(updating_member="event_log", default_factory=dict)
    def output_map(
        self, payload: Optional[Dict[str, ScheEvent]], updated: List[ScheEvent]
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

    @started_map.reset
    @last_event.reset
    @task_id_map.reset
    @output_map.reset
    def reset_cached_lazy_property(self):
        pass

    def pend_task(
        self,
        event_type: EVENT_TYPE,
        task_name: str,
        args: Dict[str | int, str],
        depend_on: List[str],
        process_id: str = "",
    ) -> ScheEvent:
        assert len(self.check_for_term) == 0
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
        """
        output_dict = defaultdict(list)
        for event in self.event_log:
            assert event.task_name in self.fixed_tasks
            if event.output is not None:
                output_dict[event.task_name].append(event)
        self.sche_once(output_dict)

    def sche_once(self, _output_dict=None):
        output_dict: Dict[str, List[ScheEvent]] = defaultdict(list)
        if _output_dict is not None:
            output_dict.update(_output_dict)
        for task_name in self.graph.handle_sequence:
            if task_name in self.fixed_tasks:
                # skip finished tasks
                continue
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
        if task_id in self.term_task_id_map:
            evt = self.term_task_id_map[task_id]
            assert evt.task_name in self.issued_for_term, (
                evt.task_name,
                self.issued_for_term,
            )
            assert evt.task_name in self.check_for_term
            self.issued_for_term.remove(evt.task_name)
            self.check_for_term.remove(evt.task_name)
            return
        if task_id not in self.pending_task_id:
            logger.warning(f'task "{task_id}" is not in pending tasks')
        self.pending_task_id.remove(task_id)
        self.running_task_id.remove(task_id)
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

    def set_error_command(self, task_id: str):
        self.error_task_id.add(task_id)
        self.set_finish_command(task_id, generate=False)

    def get_issue_tasks(self) -> Generator[ScheEvent, None, None]:
        for t in self.check_for_term - self.issued_for_term:
            evt = ScheEvent(
                cmd_type=EVENT_TYPE.TERM,
                task_name=t,
                args={},
                output=None,
                process_id=self.graph.node_map[t].node.task_name,
            )
            self.term_task_id_map[evt.command_id] = evt
            self.issued_for_term.add(t)
            yield evt
        for task_id_to_check in list(
            self.pending_task_id - self.running_task_id
        ):
            task = self.task_id_map[task_id_to_check]
            for dep in task.exec_after:
                if dep in self.pending_task_id:
                    break
            else:
                self.running_task_id.add(task_id_to_check)
                yield self.task_id_map[task_id_to_check]
