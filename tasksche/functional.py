import contextlib
import hashlib
import importlib
import io
import os.path
import sys
import types
from enum import Enum, auto
from functools import cached_property
from io import BytesIO
from itertools import chain
from typing import Any, Dict, List, Union, Tuple, Callable, Optional

import yaml.scanner
from pydantic import BaseModel, Field

from tasksche.storage.storage import storage_factory, KVStorageBase
from .logger import Logger
from .task_spec import process_path

logger = Logger()


def search_for_root(base):
    path = os.path.abspath(base)
    while path != '/':
        if os.path.exists(os.path.join(path, '.root')):
            return path
        path = os.path.dirname(path)
    return None


class TaskSpecFmt(BaseModel):
    """
    Task Specification header definition.
    """
    require: Optional[Union[List[str], Dict[Union[str, int], Any]]] = Field(
        default=None)
    inherent: Optional[str] = Field(default=None)


class ARG_TYPE(Enum):
    RAW = auto()
    TASK_OUTPUT = auto()


class RequirementArg(BaseModel):
    arg_type: ARG_TYPE
    from_task: Optional[str] = Field(default=None)
    value: Optional[Any] = Field(default=None)


class RunnerArgSpec(BaseModel):
    arg_type: ARG_TYPE
    value: Optional[Any] = Field(default=None)
    storage_path: Optional[str | int] = Field(default=None)


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
    assert task_name.startswith('/')
    return os.path.join(root, task_name[1:] + '.py')


def file_path_to_task_name(
        file_path: str, root: Optional[str] = None) -> Tuple[str, str]:
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
        raise Exception(f'Cannot find task root! {file_path}')
    file_path = os.path.abspath(file_path)
    assert file_path.startswith(root)
    return file_path[len(root):-len('.py')], file_path


def process_inherent(child: TaskSpecFmt, parent: TaskSpecFmt) -> TaskSpecFmt:
    new_fmt = child.model_copy(deep=True)
    if new_fmt.require is None:
        new_fmt.require = parent.require
    elif (isinstance(parent.require, dict)
          and isinstance(new_fmt.require, dict)):
        new_fmt.require.update(parent.require)
    else:
        logger.warning('incompatible requirement')
    return new_fmt


def parse_task_specs(task_name: str, root: str) -> TaskSpecFmt:
    payload = bytearray()
    file_path = task_name_to_file_path(task_name, root)
    with open(file_path, 'rb') as f:
        f.readline()
        while True:
            line = f.readline()
            if line == b'"""\n' or line == b'"""':
                break
            payload.extend(line)
    task_info = TaskSpecFmt.model_validate(
        yaml.safe_load(BytesIO(payload))
    )
    if task_info is None:
        raise ValueError()
    if task_info.inherent is not None:
        inh_task_name = process_path(task_name, task_info.inherent)
        inh_task = parse_task_specs(inh_task_name, root)
        if task_info.require is None:
            task_info.require = inh_task.require
        elif isinstance(inh_task.require, dict) and isinstance(
                task_info.require, dict):
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
        self._task_spec = task_spec
        self.task_name = task_name
        self.task_root = task_root

    def _parse_arg_str(self, arg) -> RequirementArg:
        if isinstance(arg, str) and arg.startswith('$'):
            return RequirementArg(
                arg_type=ARG_TYPE.TASK_OUTPUT,
                from_task=process_path(self.task_name, arg[1:]))
        return RequirementArg(arg_type=ARG_TYPE.RAW, value=arg)

    @cached_property
    def args(self) -> List[RequirementArg]:
        if isinstance(self.task_spec.require, list):
            return [self._parse_arg_str(arg) for arg in self.task_spec.require]
        elif isinstance(self.task_spec.require, dict):
            keys = [
                k for k in self.task_spec.require.keys() if isinstance(k, int)]
            if len(keys) == 0:
                return []
            max_key = max(keys)
            if len(keys) < max_key + 1:
                logger.warning(
                    'missing positional argument will be set to None')
            return [
                self._parse_arg_str(
                    self.task_spec.require[k]
                ) if k in keys else RequirementArg(
                    arg_type=ARG_TYPE.RAW,
                    value=None
                ) for k in range(max_key + 1)
            ]
        else:
            raise ValueError(
                f'unknown requirement type type {self.task_spec.require}')

    @cached_property
    def kwargs(self) -> Dict[str, RequirementArg]:
        if isinstance(self.task_spec.require, dict):
            return {
                k: self._parse_arg_str(v)
                for k, v in self.task_spec.require.items()
                if isinstance(k, str)}
        else:
            return {}

    @cached_property
    def is_source_node(self) -> bool:
        return len(self.depend_on) == 0

    @cached_property
    def task_spec(self) -> TaskSpecFmt:
        if self._task_spec is None:
            return parse_task_specs(self.task_name, self.task_root)
        return self._task_spec

    @cached_property
    def depend_on(self) -> List[str]:
        return [
            val.from_task
            for val in chain(self.args, self.kwargs.values())
            if val.arg_type == ARG_TYPE.TASK_OUTPUT and val.from_task is not None
        ]

    @cached_property
    def code_file(self) -> str:
        return task_name_to_file_path(self.task_name, self.task_root)

    @cached_property
    def hash_code(self) -> str:
        with open(self.code_file, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()


class END_NODE(FlowNode):
    def __init__(self, depend_on: List[str]) -> None:
        super().__init__(
            '_END_', '', TaskSpecFmt(require=[f'${k}' for k in depend_on]))

    def __repr__(self) -> str:
        return f'<END_NODE depend_on: {self.depend_on}>'


class Graph:
    def __init__(self, root: str, target_tasks: List[str]):
        self.root = os.path.abspath(root)
        self.target_tasks: List[str] = target_tasks

    @cached_property
    def node_map(self) -> Dict[str, FlowNode]:
        to_add = set(self.target_tasks)
        nmap: Dict[str, FlowNode] = {}
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
        nmap['_END_'] = END_NODE(sink_node)
        return nmap

    @cached_property
    def source_nodes(self) -> List[str]:
        return [
            k for k, v in self.node_map.items()
            if v.is_source_node
        ]

    def _agg(
            self,
            map_func: Callable[[FlowNode], Any],
            reduce_func: Callable[[List[Any]], Any],
            target: str,
            result: Optional[Dict[str, Any]] = None
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
            targets: Optional[List[str] | str] = None):
        result = {}
        if targets is None:
            targets = list(self.node_map.keys())
        for target in targets:
            self._agg(map_func, reduce_func, target, result)
        return result

    @cached_property
    def requirements_map(self) -> Dict[str, List[str]]:
        def map_func(node: FlowNode) -> List[str]:
            return node.depend_on

        def reduce_func(nodes: List[List[str]]) -> List[str]:
            return sorted(list(set().union(*nodes)))

        return self.aggregate(
            map_func=map_func,
            reduce_func=reduce_func,
            targets=self.target_tasks,
        )


def load_input(spec: RunnerTaskSpec, storage: Optional[KVStorageBase] = None):
    def get_input(arg: RunnerArgSpec):
        if arg.arg_type == ARG_TYPE.RAW:
            return arg.value
        elif arg.arg_type == ARG_TYPE.TASK_OUTPUT:
            assert storage is not None
            return storage.get(arg.storage_path)
        else:
            raise ValueError(f'unknown arg type {arg.arg_type}')

    args = [get_input(node_arg) for node_arg in spec.args]
    kwargs = {k: get_input(v) for k, v in spec.kwargs.items()}
    return args, kwargs


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
        self.stdout_file = open(os.path.join(self.cwd, 'stdout.txt'), 'w')
        self.redirect_stdout = contextlib.redirect_stdout(self.stdout_file)
        self.redirect_stdout.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        assert self.redirect_stdout is not None
        assert self.stdout_file is not None
        self.redirect_stdout.__exit__(exc_type, exc_value, traceback)
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
    task_module_path = spec.task.replace('/', '.')[1:]
    storage = storage_factory(spec.storage_path)
    with ExecEnv(spec.root, spec.work_dir):
        if task_module_path in sys.modules:
            logger.info(f'reloading module {task_module_path}')
            mod = importlib.reload(sys.modules[task_module_path])
        else:
            mod = importlib.import_module(task_module_path)
        mod.__dict__['work_dir'] = '.'
        if not hasattr(mod, 'run'):
            raise NotImplementedError()
        args, kwargs = load_input(spec, storage)
        output = mod.run(*args, **kwargs)
        # if output is generator, iter over it and return the last item
        if isinstance(output, types.GeneratorType):
            # with open('_progress.pipe', 'w') as f:
            while True:
                try:
                    _ = next(output)
                except StopIteration as e:
                    output = e.value
                    break
        storage.store(spec.output_path, output)
    return 0


if __name__ == '__main__':
    scheduler = Graph('test/simple_task_set', ['/task'])
    logger.info(scheduler.node_map)
    logger.info(scheduler.node_map['/task'].kwargs)
    logger.info(scheduler.node_map['/task'].depend_on)
    logger.info(scheduler.source_nodes)
