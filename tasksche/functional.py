import abc
from audioop import reverse
from collections import defaultdict
import contextlib
import hashlib
import importlib
import io
import os.path
import queue
import sys
import threading
import types
from enum import Enum, auto
from functools import cache, cached_property, lru_cache
from io import BytesIO
from itertools import chain
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

import yaml.scanner
from pydantic import BaseModel, Field, ValidationError

from .logger import Logger
from .storage.storage import KVStorageBase, storage_factory

logger = Logger()


class __INVALIDATE__:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(__INVALIDATE__, cls).__new__(cls)

        return cls._instance

    def __repr__(self) -> str:
        return "<INVALID>"


_INVALIDATE = __INVALIDATE__()


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


class RunnerTaskSpec(BaseModel):
    """
    define how a single task should be run by a task runner,
    """

    task: str
    root: str
    args: List[RunnerArgSpec]
    kwargs: Dict[str, RunnerArgSpec]
    storage_path: str
    output_path: str
    work_dir: Optional[str]


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
    def _dep_arg_parse(self) -> Dict[int | str, RequirementArg]:
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
        int_keys = [
            k for k in self._dep_arg_parse.keys() if isinstance(k, int)
        ]
        int_keys.sort()
        if len(int_keys) > 0 and int_keys[0] == -1:
            int_keys = int_keys[1:]
        for k in range(len(int_keys)):
            assert k == int_keys[k], (k, int_keys)
        args = [self._dep_arg_parse[k] for k in int_keys]
        kwargs = {
            k: v for k, v in self._dep_arg_parse.items() if isinstance(k, str)
        }
        return args, kwargs

    @cached_property
    def args(self) -> List[RequirementArg]:
        return self._arg_kwarg_parse[0]

    @cached_property
    def kwargs(self) -> Dict[str, RequirementArg]:
        return self._arg_kwarg_parse[1]

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
            for val in chain(self.args, self.kwargs.values())
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
            for val in chain(self.args, self.kwargs.values())
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
    def TASK_OUTPUT_DEPEND_ON(self) -> set[str]:
        return set(
            [
                val.from_task
                for val in chain(self.args, self.kwargs.values())
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
                for val in chain(self.args, self.kwargs.values())
                if (val.arg_type == ARG_TYPE.TASK_ITER)
                and val.from_task is not None
            ]
        )

    @cached_property
    def code_file(self) -> str:
        return task_name_to_file_path(self.task_name, self.task_root)

    @cached_property
    def hash_code(self) -> str:
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

    @cached_property
    def execution_order(self) -> List[str]:
        o = []

        def _dfs(node: str) -> None:
            if node in o:
                return
            o.append(node)
            for n in self.node_map[node].depend_on:
                _dfs(n)

        _dfs(END_NODE.NAME)
        return list(reversed(o))

    @cached_property
    def all_child_map(self) -> Dict[str, List[str]]:
        cmap = dict()

        def _dfs(node: str) -> Set[str]:
            if node in cmap:
                return cmap[node]
            nl = set()
            for n in self.child_map[node]:
                nl.add(n)
                nls = _dfs(n)
                for n in nls:
                    if n not in nl:
                        nl.add(n)
            cmap[node] = nl
            return nl

        _dfs(ROOT_NODE.NAME)
        return {
            k: [x for x in self.execution_order if x in v]
            for k, v in cmap.items()
        }


def load_RunnerArgSpec(arg: RunnerArgSpec, storage: KVStorageBase) -> Any:
    if arg.arg_type == ARG_TYPE.RAW:
        return arg.value
    elif arg.arg_type in (
        ARG_TYPE.TASK_OUTPUT,
        ARG_TYPE.TASK_ITER,
    ):
        assert storage is not None
        assert arg.storage_path is not None
        return storage.get(arg.storage_path)
    else:
        raise ValueError(f"unknown arg type {arg.arg_type}")


def load_input(
    args: List[RunnerArgSpec],
    kwargs: Dict[str, RunnerArgSpec],
    storage: KVStorageBase,
):
    _args = [load_RunnerArgSpec(node_arg, storage) for node_arg in args]
    _kwargs = {k: load_RunnerArgSpec(v, storage) for k, v in kwargs.items()}
    return _args, _kwargs


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
        # self.redirect_stdout = contextlib.redirect_stdout(self.stdout_file)
        # self.redirect_stdout.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # assert self.redirect_stdout is not None
        # self.redirect_stdout.__exit__(exc_type, exc_value, traceback)
        assert self.stdout_file is not None
        self.stdout_file.close()
        if self.cwd:
            os.chdir(self.previous_dir)
        if self.pythonpath:
            sys.path.remove(self.pythonpath)


def execute_task(spec: RunnerTaskSpec):
    """
    Execute the task by importing the task module and calling the
    'run' function.

    NOTE: this function should only be called when all dependent
    task finished. No dirty check or dependency check will be invoked
    in this function.

    Returns:
        None
    """
    task_module_path = spec.task.replace("/", ".")[1:]
    storage = storage_factory(spec.storage_path)
    with ExecEnv(spec.root, spec.work_dir):
        if task_module_path in sys.modules:
            logger.info(f"reloading module {task_module_path}")
            mod = importlib.reload(sys.modules[task_module_path])
        else:
            mod = importlib.import_module(task_module_path)
        # mod.__dict__['work_dir'] = '.'
        if not hasattr(mod, "run"):
            raise NotImplementedError()
        args, kwargs = load_input(spec.args, spec.kwargs, storage)
        output = mod.run(*args, **kwargs)
        storage.store(spec.output_path, value=output)
    return 0


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


class BaseTaskExecutor(abc.ABC):
    def __init__(self):
        self._task_spec_first: Optional[RunnerTaskSpec] = None

    @property
    def spec(self) -> RunnerTaskSpec:
        assert self._task_spec_first is not None, "spec should not be None"
        return self._task_spec_first

    @property
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

    @abc.abstractmethod
    def _call_run(self, spec: RunnerTaskSpec):
        raise NotImplementedError

    def call_run(self, spec: RunnerTaskSpec):
        """
        Execute the task by importing the task module and iter over the
        iterator.
        return:
            StopIteration: for no more data and call finish
            Exception: for error and call finish
            str: for data_output_key
        """
        self._task_spec_first = spec
        output = self._call_run(spec)
        if not isinstance(output, Exception) or isinstance(
            output, StopIteration
        ):
            self.storage.store(spec.output_path, value=output)
        if isinstance(output, Exception):
            return output
        return spec.output_path


class IteratorTaskExecutor(BaseTaskExecutor):
    def __init__(self):
        self._task_spec_first: Optional[RunnerTaskSpec] = None
        self.generator = None

    def _call_run(self, spec: RunnerTaskSpec):
        with self.env:
            try:
                if self.generator is None:
                    args, kwargs = load_input(
                        spec.args, spec.kwargs, self.storage
                    )
                    self.generator = self.mod.run(*args, **kwargs)
                    assert isinstance(self.generator, types.GeneratorType)
                output = next(self.generator)
                return output
            except StopIteration as e:
                self.generator = None
                return e
            except Exception as e:
                logger.error(e, stack_info=True)
                self.generator = None
                return e


class MapperTaskExecutor(BaseTaskExecutor):
    @property
    def mod(self):
        if self.mod_name in sys.modules:
            return importlib.reload(sys.modules[self.mod_name])
        else:
            return importlib.import_module(self.mod_name)

    def _call_run(self, spec: RunnerTaskSpec):
        with self.env:
            try:
                args, kwargs = load_input(spec.args, spec.kwargs, self.storage)
                output = self.mod.run(*args, **kwargs)
                return output
            except Exception as e:
                logger.error(e)
                return e


class CollectorTaskExecutor(BaseTaskExecutor):
    def __init__(self):
        super().__init__()
        self.thread = None
        self.queue = None

    @cached_property
    def iter_args(self) -> Tuple[Tuple[int, IteratorArg]]:
        iter_args = []
        for index, arg in enumerate(self.spec.args):
            if arg.arg_type == ARG_TYPE.TASK_ITER:
                iter_args.append((index, IteratorArg()))
        return tuple(iter_args)

    @cached_property
    def iter_kwargs(self) -> Tuple[Tuple[str, IteratorArg]]:
        iter_kwargs = []
        for k, arg in self.spec.kwargs.items():
            if arg.arg_type == ARG_TYPE.TASK_ITER:
                iter_kwargs.append((k, IteratorArg()))
        return tuple(iter_kwargs)

    @cached_property
    def args(self) -> Tuple[Any]:
        args, _ = load_input(self.spec.args, {}, self.storage)
        for i, iter_obj in self.iter_args:
            iter_obj.put_payload(args[i])
            args[i] = iter_obj
        return tuple(args)

    @cached_property
    def kwargs(self) -> Dict[str, Any]:
        _, kwargs = load_input([], self.spec.kwargs, self.storage)
        for k, iter_obj in self.iter_kwargs:
            iter_obj.put_payload(kwargs[k])
            kwargs[k] = iter_obj
        return kwargs

    def _worker_thread(self, *args, **kwargs):
        with self.env:
            try:
                self.mod.run(*args, **kwargs)
            except Exception as e:
                return e

    def _call_run(self, spec: RunnerTaskSpec):
        if self.thread is None:
            assert self.spec is spec
            self.thread = threading.Thread(
                target=self._worker_thread,
                args=self.args,
                kwargs=self.kwargs,
            )
            self.thread.start()
        else:
            for i, iter_arg in self.iter_args:
                iter_arg.put_payload(
                    load_RunnerArgSpec(spec.args[i], self.storage)
                )
            for k, iter_arg in self.iter_kwargs:
                iter_arg.put_payload(
                    load_RunnerArgSpec(spec.kwargs[k], self.storage)
                )
        return None


if __name__ == "__main__":
    q = queue.Queue()
    for i in range(10):
        q.put(i)
    q.put(StopIteration())
    task = IteratorArg(q)
    for i in task:
        print(i)
    # scheduler = Graph("test/simple_task_set", ["/task"])
    # logger.info(scheduler.node_map)
    # logger.info(scheduler.node_map["/task"].kwargs)
    # logger.info(scheduler.node_map["/task"].depend_on)
    # logger.info(scheduler.source_nodes)
