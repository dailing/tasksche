import contextlib
import hashlib
import importlib
import io
import multiprocessing
import os.path
import queue
import sys
import threading
from collections import defaultdict
from enum import Enum, auto
from functools import cached_property
from io import BytesIO
from itertools import chain, groupby
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple, Union

import yaml.scanner
from pydantic import BaseModel, Field, ValidationError

from .logger import Logger
from .storage.storage import storage_factory

logger = Logger()


class __INVALIDATE__:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(__INVALIDATE__, cls).__new__(cls)

        return cls._instance

    def __repr__(self) -> str:
        return "<INVALID>"


class ARG_TYPE(Enum):
    RAW = auto()
    TASK_OUTPUT = auto()
    TASK_ITER = auto()
    VIRTUAL = auto()


class RequirementArg(BaseModel):
    arg_type: ARG_TYPE
    from_task: Optional[str] = Field(default=None)
    value: Optional[Any] = Field(default=None)


class RunnerArgSpec(BaseModel):
    arg_type: ARG_TYPE
    value: Optional[Any] = Field(default=None)
    storage_path: Optional[str] = Field(default=None)


class ISSUE_TASK_TYPE(str, Enum):
    # START_TASK = "start_task"
    START_GENERATOR = "start_G"
    START_ITERATOR = "start_I"
    ITER_TASK = "iter_task"
    NORMAL_TASK = "map_task"
    PUSH_TASK = "push_task"
    PULL_RESULT = "pull_result"
    END_TASK = "end_task"


# define a singleton class and return counter, that add 1 automically
class _Counter:
    def __init__(self, prefix="") -> None:
        self._counter = 0
        self.prefix = prefix

    def get_counter(self):
        self._counter += 1
        return f"{self.prefix}_{self._counter:03d}"


_task_counter = _Counter("_tt")
_output_counter = _Counter("_oo")


class TaskIssueInfo(BaseModel):
    reqs: Dict[str | int, str] = Field(default_factory=dict)
    wait_for: Tuple[str, ...] = Field(default_factory=lambda: ())
    task_name: str
    output: Optional[str] = Field(default_factory=_output_counter.get_counter)
    task_type: ISSUE_TASK_TYPE
    task_id: str = Field(default_factory=_task_counter.get_counter)

    def __repr__(self) -> str:
        output = self.output
        if output is None:
            output = ""
        return (
            f"{self.task_type.value:^10s}"
            f"{self.task_name:>8s}:{self.task_id:10s}->{output:18s}"
            f"[{' '.join(map(str, self.reqs.values())):^28s}]"
            f"[{' '.join(self.wait_for):^28s}]"
        )

    def __str__(self) -> str:
        return self.__repr__()

    @property
    def node_repr(self) -> str:
        return (
            f'["{self.task_name}<br/>'
            f"{self.task_id}<br/>"
            f'{str(self.task_type.value)}"]'
        )


def group_task_issue_info_by_name(
    task_issue_infos: Iterable[TaskIssueInfo],
) -> Dict[str, List[TaskIssueInfo]]:
    return {
        k: list(v) for k, v in groupby(task_issue_infos, lambda x: x.task_name)
    }


class GroupOfTaskInfo:
    def __init__(self, task_issue_infos: List[TaskIssueInfo]):
        self.gp = group_task_issue_info_by_name(
            filter(lambda x: x.output is not None, task_issue_infos)
        )

    def pop_reqs(
        self, req: Dict[int | str, RequirementArg]
    ) -> Dict[int | str, str]:
        output = {}
        for k, v in req.items():
            tks = self.gp[v.from_task]
            tk_ = None
            for i in tks:
                if i.output is not None:
                    tk_ = i
                    break
            if tk_ is None:
                raise Exception(f"can't find task {v.from_task}")
            output[k] = tk_.output
            tks.remove(tk_)
        return output

    def items(self, req: Dict[int | str, RequirementArg]):
        for k, r in req.items():
            assert r.from_task is not None
            for tk in self.gp.get(r.from_task, []):
                yield k, r, tk


class Status(Enum):
    STATUS_READY = "ready"
    STATUS_FINISHED = "finished"
    STATUS_ERROR = "error"
    STATUS_PENDING = "pending"
    STATUS_RUNNING = "running"


def process_path(task_name: str, path: str):
    task_name = os.path.dirname(task_name)
    if not path.startswith("/"):
        path = os.path.join(task_name, path)
    path_filtered = []
    for sub_path in path.split("/"):
        if sub_path == ".":
            continue
        if sub_path == "..":
            path_filtered.pop()
            continue
        path_filtered.append(sub_path)
    if path_filtered[0] != "":
        path_filtered = task_name.split("/") + path_filtered
    path_new = "/".join(path_filtered)
    return path_new


def search_for_root(base):
    path = os.path.abspath(base)
    while path != "/":
        if os.path.exists(os.path.join(path, ".root")):
            return path
        path = os.path.dirname(path)
    return None


class TASK_TYPE(Enum):
    NORMAL = "normal"
    GENERATOR = "generator"


class TaskSpecFmt(BaseModel):
    """
    Task Specification header definition.
    """

    require: Optional[Union[List[str], Dict[Union[str, int], Any]]] = Field(
        default=None
    )
    inherent: Optional[str] = Field(default=None)
    task_type: Optional[TASK_TYPE] = Field(default=TASK_TYPE.NORMAL)
    iter_args: Optional[List[int | str]] = Field(default=None)


class RunnerTaskSpec(BaseModel):
    """
    define how a single task should be run by a task runner,
    """

    task: str
    root: str
    requires: Dict[str | int, RunnerArgSpec]
    storage_path: str
    output_path: Optional[str]
    work_dir: Optional[str]
    task_type: ISSUE_TASK_TYPE
    task_id: str


def task_name_to_file_path(task_name: str, root: str) -> str:
    assert task_name.startswith("/"), f"task name is: {task_name}"
    return os.path.join(root, task_name[1:] + ".py")


def file_path_to_task_name(
    file_path: str, root: Optional[str] = None
) -> Tuple[str, str]:
    """
    Generate a task name from a file path.

    Args:
        file_path (str): The path to the file.
        root (Optional[str], optional): The root directory to search for.
        Defaults to None will be searched.

    Returns:
        Tuple[str, str]: A tuple containing the rootpath and the task name.

    Raises:
        Exception: If the task root cannot be found.

    """
    if root is None:
        root = search_for_root(file_path)
    if root is None:
        raise Exception(f"Cannot find task root! {file_path}")
    file_path = os.path.abspath(file_path)
    assert file_path.startswith(root)
    return file_path[len(root) : -len(".py")], file_path


def process_inherent(child: TaskSpecFmt, parent: TaskSpecFmt) -> TaskSpecFmt:
    new_fmt = child.model_copy(deep=True)
    if new_fmt.require is None:
        new_fmt.require = parent.require
    elif isinstance(parent.require, dict) and isinstance(
        new_fmt.require, dict
    ):
        new_fmt.require.update(parent.require)
    else:
        logger.warning("incompatible requirement")
    return new_fmt


def parse_task_specs(task_name: str, root: str) -> TaskSpecFmt:
    payload = bytearray()
    file_path = task_name_to_file_path(task_name, root)
    with open(file_path, "rb") as f:
        f.readline()
        while True:
            line = f.readline()
            if line == b'"""\n' or line == b'"""':
                break
            payload.extend(line)
    try:
        task_info = TaskSpecFmt.model_validate(
            yaml.safe_load(BytesIO(payload))
        )
    except ValidationError as e:
        logger.error(f"Error parsing {task_name}")
        raise e
    if task_info is None:
        raise ValueError()
    if task_info.inherent is not None:
        inh_task_name = process_path(task_name, task_info.inherent)
        inh_task = parse_task_specs(inh_task_name, root)
        if task_info.require is None:
            task_info.require = inh_task.require
        elif isinstance(inh_task.require, dict) and isinstance(
            task_info.require, dict
        ):
            updated_dep = inh_task.require.copy()
            updated_dep.update(task_info.require)
            task_info.require = updated_dep
    return task_info


class FlowNode:
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
    def dep_arg_parse(self) -> Dict[int | str, RequirementArg]:
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
        dep_cnt = 0
        for arg in arg_dict.values():
            if arg.arg_type in (
                ARG_TYPE.TASK_OUTPUT,
                ARG_TYPE.TASK_ITER,
            ):
                dep_cnt += 1
        if dep_cnt == 0:
            arg_dict["__VIRTUAL__"] = RequirementArg(
                arg_type=ARG_TYPE.VIRTUAL,
                from_task=ROOT_NODE.NAME,
            )
        return arg_dict

    @cached_property
    def _arg_kwarg_parse(
        self,
    ) -> Tuple[List[RequirementArg], Dict[str, RequirementArg]]:
        args = []
        int_keys = [k for k in self.dep_arg_parse.keys() if isinstance(k, int)]
        int_keys.sort()
        if len(int_keys) > 0 and int_keys[0] == -1:
            int_keys = int_keys[1:]
        for k in range(len(int_keys)):
            assert k == int_keys[k], (k, int_keys)
        args = [self.dep_arg_parse[k] for k in int_keys]
        kwargs = {
            k: v for k, v in self.dep_arg_parse.items() if isinstance(k, str)
        }
        return args, kwargs

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
            for val in self.dep_arg_parse.values()
            if (
                val.arg_type
                in [
                    ARG_TYPE.TASK_OUTPUT,
                    ARG_TYPE.VIRTUAL,
                    ARG_TYPE.TASK_ITER,
                ]
                and val.from_task is not None
            )
        ]

    @cached_property
    def depend_on_no_virt(self) -> List[str]:
        return [
            val.from_task
            for val in self.dep_arg_parse.values()
            if (
                val.arg_type
                in [
                    ARG_TYPE.TASK_OUTPUT,
                    ARG_TYPE.TASK_ITER,
                ]
                and val.from_task is not None
                and val.from_task != ROOT_NODE.NAME
            )
        ]

    @cached_property
    def task_output_depend_on_(self) -> Dict[int | str, RequirementArg]:
        return {
            k: val
            for k, val in self.dep_arg_parse.items()
            if val.from_task in self.TASK_OUTPUT_DEPEND_ON
        }

    @cached_property
    def task_iter_depend_on_(self) -> Dict[int | str, RequirementArg]:
        return {
            k: val
            for k, val in self.dep_arg_parse.items()
            if val.from_task in self.TASK_ITER_DEPEND_ON
        }

    @cached_property
    def TASK_OUTPUT_DEPEND_ON(self) -> set[str]:
        return set(
            [
                val.from_task
                for val in self.dep_arg_parse.values()
                if (
                    val.arg_type in (ARG_TYPE.TASK_OUTPUT,)
                    and val.from_task is not None
                )
            ]
        )

    @cached_property
    def TASK_ITER_DEPEND_ON(self) -> set[str]:
        return set(
            [
                val.from_task
                for val in self.dep_arg_parse.values()
                if (val.arg_type == ARG_TYPE.TASK_ITER)
                and val.from_task is not None
            ]
        )

    @cached_property
    def code_file(self) -> str:
        return task_name_to_file_path(self.task_name, self.task_root)

    @cached_property
    def code_hash(self) -> str:
        with open(self.code_file, "rb") as f:
            return hashlib.md5(f.read()).hexdigest()

    @cached_property
    def is_persistent_node(self):
        return self.is_generator or len(self.TASK_ITER_DEPEND_ON) > 0

    def __repr__(self):
        return f"<FlowNode: {self.task_name} <<-- {self.depend_on}>"


class END_NODE(FlowNode):
    NAME = "_END_"

    def __init__(self, depend_on: List[str]) -> None:
        super().__init__(
            self.NAME,
            "",
            TaskSpecFmt(require=[f"${k}" for k in depend_on]),
        )

    def __repr__(self) -> str:
        return f"<END_NODE depend_on: {self.depend_on}>"

    @cached_property
    def is_generator(self) -> bool:
        return False

    @property
    def TASK_OUTPUT_DEPEND_ON(self) -> set[str]:
        return set(self.depend_on)

    @cached_property
    def code_hash(self) -> str:
        return "_END_"


class ROOT_NODE(FlowNode):
    NAME = "_ROOT_"

    def __init__(self) -> None:
        super().__init__(self.NAME, "", TaskSpecFmt(require=[]))

    def __repr__(self) -> str:
        return "<ROOT_NODE>"

    @cached_property
    def depend_on(self) -> List[str]:
        return []

    @cached_property
    def code_hash(self) -> str:
        return "_ROOT_"

    @cached_property
    def hash_code(self) -> str:
        return ""


class Graph:
    def __init__(self, root: str, target_tasks: List[str]):
        self.root = os.path.abspath(root)
        self.target_tasks: List[str] = target_tasks

    @cached_property
    def node_map(self) -> Dict[str, FlowNode]:
        to_add = set(self.target_tasks)
        nmap: Dict[str, FlowNode] = {ROOT_NODE.NAME: ROOT_NODE()}
        while len(to_add) > 0:
            task_name = to_add.pop()
            node = FlowNode(task_name, self.root)
            nmap[task_name] = node
            for new_task in node.depend_on:
                if new_task not in nmap:
                    to_add.add(new_task)
        sink_node = {k: True for k in nmap.keys()}
        for node in nmap.values():
            for n in node.depend_on:
                sink_node[n] = False
        sink_node = [k for k, v in sink_node.items() if v]
        nmap[END_NODE.NAME] = END_NODE(sink_node)
        # nmap[ROOT_NODE.NAME] = root_node
        # for k, v in self.node_map.items():
        #     if v.is_source_node and ROOT_NODE.NAME not in v.depend_on:
        #         v.args.append()
        return nmap

    def _agg(
        self,
        map_func: Callable[[FlowNode], Any],
        reduce_func: Callable[[List[Any]], Any],
        target: str,
        result: Optional[Dict[str, Any]] = None,
    ):
        if result is None:
            result = {}
        elif target in result:
            return result[target]
        reduce_list = []
        for n in self.node_map[target].depend_on:
            reduce_list.append(self._agg(map_func, reduce_func, n, result))
        reduce_list.append(map_func(self.node_map[target]))
        result[target] = reduce_func(reduce_list)
        return result[target]

    def aggregate(
        self,
        map_func: Callable,
        reduce_func: Callable,
        targets: Optional[List[str] | str] = None,
    ):
        result = {}
        if targets is None:
            targets = list(self.node_map.keys())
        for target in targets:
            self._agg(map_func, reduce_func, target, result)
        return result

    @cached_property
    def requirements_map(self) -> Dict[str, List[str]]:
        """
        record all tasks that need to be run in value as list
        before running this key task
        """

        def map_func(node: FlowNode) -> List[str]:
            return node.depend_on

        def reduce_func(nodes: List[List[str]]) -> List[str]:
            return sorted(list(set().union(*nodes)))

        return self.aggregate(
            map_func=map_func,
            reduce_func=reduce_func,
            targets=self.target_tasks,
        )

    @cached_property
    def child_map(self) -> Dict[str, List[str]]:
        cmap = defaultdict(list)
        for k, v in self.node_map.items():
            for n in v.depend_on:
                cmap[n].append(k)
        return cmap


class ExecEnv:
    def __init__(self, pythonpath, cwd):
        self.pythonpath = pythonpath
        self.cwd = cwd
        self.previous_dir = os.getcwd()
        self.stdout_file: Optional[io.TextIOWrapper] = None
        self.redirect_stdout: Optional[contextlib.redirect_stdout] = None

    def __enter__(self):
        if self.pythonpath:
            sys.path.insert(0, self.pythonpath)
        if not os.path.exists(self.cwd):
            os.makedirs(self.cwd, exist_ok=True)
        if self.cwd:
            os.chdir(self.cwd)
        assert self.cwd is not None
        self.stdout_file = open(os.path.join(self.cwd, "stdout.txt"), "w")
        self.redirect_stdout = contextlib.redirect_stdout(self.stdout_file)
        self.redirect_stdout.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        assert self.redirect_stdout is not None
        self.redirect_stdout.__exit__(exc_type, exc_value, traceback)
        assert self.stdout_file is not None
        self.stdout_file.close()
        if self.cwd:
            os.chdir(self.previous_dir)
        if self.pythonpath:
            sys.path.remove(self.pythonpath)


class IteratorArg:
    def __init__(self, iter_items: Optional[queue.Queue] = None) -> None:
        if iter_items is None:
            iter_items = queue.Queue()
        self.iter_items = iter_items

    def put_payload(self, payload: Any):
        self.iter_items.put(payload)

    def __iter__(self):
        while True:
            output = self.iter_items.get()
            if isinstance(output, StopIteration):
                return output.value
            else:
                yield output


def int_iterator(t: Dict[str | int, Any]):
    int_keys = sorted([k for k in t.keys() if isinstance(k, int)])
    if len(int_keys) == 0:
        return
    max_val = max(int_keys)
    for i in range(max_val + 1):
        if i in t:
            yield t[i]
        else:
            yield None


class BaseTaskExecutorWorker(multiprocessing.Process):
    def __init__(
        self,
        task_queue: multiprocessing.Queue,
        output_queue: multiprocessing.Queue,
    ):
        super().__init__()
        self._task_spec_first: Optional[RunnerTaskSpec] = None
        self.iter_map: Dict[str | int, IteratorArg] = defaultdict(IteratorArg)
        self.task_queue = task_queue
        self.output_queue = output_queue
        self.generator = None
        self.to_pool_thread = None
        self.iter_output = None

    @property
    def spec(self) -> RunnerTaskSpec:
        assert self._task_spec_first is not None, "spec should not be None"
        return self._task_spec_first

    @cached_property
    def env(self) -> ExecEnv:
        return ExecEnv(self.spec.root, self.spec.work_dir)

    @property
    def mod_name(self):
        return self.spec.task.replace("/", ".")[1:]

    @property
    def mod(self):
        return importlib.import_module(self.spec.task.replace("/", ".")[1:])

    @property
    def storage(self):
        return storage_factory(self.spec.storage_path)

    def load_input(self, key: str | int, arg: Optional[RunnerArgSpec]):
        if arg is None:
            logger.warning(f"arg is None for {key}")
            return None
        assert arg is not None
        if arg.arg_type == ARG_TYPE.RAW:
            return arg.value
        elif arg.arg_type == ARG_TYPE.TASK_OUTPUT:
            assert self.storage is not None
            assert arg.storage_path is not None
            return self.storage.get(arg.storage_path)
        elif arg.arg_type == ARG_TYPE.TASK_ITER:
            iter_obj = self.iter_map[key]
            if arg.storage_path is not None:
                iter_obj.put_payload(self.storage.get(arg.storage_path))
            return iter_obj
        else:
            logger.error(f"unknown arg type {arg.arg_type}")
            raise ValueError(f"unknown arg type {arg.arg_type}")

    def exec_func(self, spec: RunnerTaskSpec, reload: bool = False):
        """
        Execute the task by importing the task module and calling the
        'run' function. return the raw value

        NOTE: this function should only be called when all dependent
        task finished. No dirty check or dependency check will be invoked
        in this function.

        Returns:
            anything returned by 'run' function
        """
        task_module_path = self.mod_name
        if task_module_path in sys.modules and reload:
            logger.info(f"reloading module {task_module_path}")
            importlib.reload(sys.modules[task_module_path])

        args = [
            self.load_input(*x) for x in enumerate(int_iterator(spec.requires))
        ]
        kwargs = {
            k: self.load_input(k, v)
            for k, v in spec.requires.items()
            if isinstance(k, str)
        }
        # logger.info(
        #     f"executing task {spec.task} with args {args} and kwargs {kwargs}"
        # )
        for v in chain(args, kwargs.values()):
            if isinstance(v, StopIteration):
                return v
        # with self.env:
        mod = self.mod
        if not hasattr(mod, "run"):
            raise NotImplementedError("task must have 'run' function")
        output = mod.run(*args, **kwargs)
        return output

    def handle_input(self, spec: RunnerTaskSpec):
        if spec.task_type == ISSUE_TASK_TYPE.START_GENERATOR:
            # print(f"executing generator {spec.task}")
            self.generator = self.exec_func(spec)
            # print(f"generator {spec.task} started")
        elif spec.task_type == ISSUE_TASK_TYPE.START_ITERATOR:
            # print(f"executing iterator {spec.task}")
            output = self.exec_func(spec)
            self.iter_output = output
            return
        elif spec.task_type == ISSUE_TASK_TYPE.NORMAL_TASK:
            output = self.exec_func(spec)
            assert spec.output_path is not None
            self.storage.store(spec.output_path, value=output)
        elif spec.task_type == ISSUE_TASK_TYPE.ITER_TASK:
            assert self.generator is not None
            try:
                output = next(self.generator)
            except StopIteration as e:
                output = e
            self.storage.store(spec.output_path, value=output)
            self.output_queue.put((spec.task_id, output))
            return
        elif spec.task_type == ISSUE_TASK_TYPE.PUSH_TASK:
            for key, arg in spec.requires.items():
                if arg.arg_type == ARG_TYPE.TASK_ITER:
                    assert self.storage is not None
                    assert arg.storage_path is not None
                    to_push = self.storage.get(arg.storage_path)
                    self.iter_map[key].put_payload(to_push)
                    # logger.info(f"pushed {to_push} to {key}")
        elif spec.task_type == ISSUE_TASK_TYPE.PULL_RESULT:
            assert self.to_pool_thread is not None
            self.to_pool_thread.join()
            self.storage.store(spec.output_path, value=self.iter_output)
        else:
            # print(f"===========unknown task type {spec.task_type}")
            raise NotImplementedError(f"unknown task type {spec.task_type}")
        self.output_queue.put((spec.task_id, None))

    def run(self):
        spec = self.task_queue.get()
        if spec is None:
            return
        self._task_spec_first = spec
        with self.env:
            while True:
                try:
                    t = threading.Thread(
                        target=self.handle_input, args=(spec,)
                    )
                    t.start()
                    # print("757 started")
                    if spec.task_type == ISSUE_TASK_TYPE.START_ITERATOR:
                        assert self.to_pool_thread is None
                        self.to_pool_thread = t
                        self.output_queue.put((spec.task_id, None))
                except Exception as e:
                    print("ERROR", spec)
                    print("ERROR", e)
                    print(e)
                    print("-----------------------------------------")
                    break
                spec = self.task_queue.get()
                # print(f"746handling {spec.task}----------")
                if spec is None:
                    break
                self._task_spec_first = spec
        # print("exit-------------------------------\n", end="")


class BaseTaskExecutor:
    def __init__(self) -> None:
        super().__init__()
        self.process = None
        self.task_queue: multiprocessing.Queue = multiprocessing.Queue()
        self.output_queue: multiprocessing.Queue = multiprocessing.Queue()

    def call(self, spec: RunnerTaskSpec):
        if self.process is None:
            self.process = BaseTaskExecutorWorker(
                self.task_queue,
                self.output_queue,
            )
            self.process.start()
            logger.info(f"worker process started")
        # assert self.process.is_alive()
        self.task_queue.put(spec)
        result = self.output_queue.get()
        return result
