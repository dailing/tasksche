import contextlib
import io
import multiprocessing
import os.path
import sys
from enum import Enum, auto
from functools import cached_property
from io import BytesIO
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Tuple,
    Union,
    TypeVar,
)

import yaml.scanner
from pydantic import BaseModel, Field, ValidationError

from .logger import Logger

logger = Logger()


class __INVALIDATE__:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(__INVALIDATE__, cls).__new__(cls)

        return cls._instance

    def __repr__(self) -> str:
        return "<INVALID>"


class EVENT_TYPE(Enum):
    CALL = auto()
    PUSH = auto()
    POP_AND_NEXT = auto()
    POP = auto()
    # result event
    FINISH = auto()
    GENERATOR_END = auto()
    ERROR = auto()


class ARG_TYPE(Enum):
    RAW = auto()
    TASK_OUTPUT = auto()
    TASK_ITER = auto()


class RequirementArg(BaseModel):
    arg_type: ARG_TYPE
    from_task: Optional[str] = Field(default=None)
    value: Optional[Any] = Field(default=None)


class RunnerArgSpec(BaseModel):
    arg_type: ARG_TYPE
    value: Optional[Any] = Field(default=None)
    storage_path: Optional[str] = Field(default=None)


class ISSUE_TASK_TYPE(str, Enum):
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

    require: Optional[Union[List[Any], Dict[Union[str, int], Any]]] = Field(
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
    requires: Dict[str | int, str]
    storage_path: str
    output_path: Optional[str]
    work_dir: Optional[str]
    task_type: EVENT_TYPE
    task_id: str
    process_id: str = Field(default="")


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
    def __init__(
        self, iter_items: Optional[multiprocessing.Queue] = None
    ) -> None:
        if iter_items is None:
            iter_items = multiprocessing.Queue()
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
        # self.last_index = 0
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

    @cached_property
    def _last_index_name_(self):
        assert self.attr_name is not None
        return f"__{self.attr_name}__last_index__"

    def __get__(self, instance: Any, owner: Any):
        if instance is None:
            return self
        last_index = getattr(instance, self._last_index_name_, 0)
        member = getattr(instance, self.updating_member, None)
        assert member is not None
        updated_member = member[last_index:]
        last_index = len(member)
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
        setattr(instance, self._last_index_name_, last_index)
        return val


def lazy_property(
    updating_member: str,
    default_factory: Optional[Callable[..., T]] = None,
) -> Callable[[Callable[[Any, Optional[T], Any], T]], LazyProperty]:
    def wrapper(func: Callable[[Any, Optional[T], Any], T]) -> LazyProperty:
        return LazyProperty(func, updating_member, default_factory)

    return wrapper
