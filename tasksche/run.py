import asyncio
import contextlib
import inspect
import logging
import multiprocessing
import os
import os.path
import pickle
import sys
import time
from abc import abstractmethod, ABC
# from queue import Queue as _Queue
from asyncio import Queue
from dataclasses import dataclass
from enum import Enum, auto
from functools import cached_property
from hashlib import md5
from io import BytesIO, StringIO
from itertools import zip_longest
from multiprocessing import Process
from pprint import pprint
from typing import Any, Dict, List, Tuple, Union, Optional
import concurrent.futures
import yaml
from typing_extensions import Self
from watchfiles import awatch


def get_logger(name: str, print_level=logging.DEBUG):
    formatter = logging.Formatter(
        fmt="%(levelname)10s "
            "[%(filename)15s:%(lineno)-3d %(asctime)s]"
            " %(message)s",
        datefmt='%H:%M:%S',
    )
    logger_obj = logging.getLogger(name)
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(print_level)
    logger_obj.addHandler(stream_handler)
    logger_obj.setLevel(logging.DEBUG)
    return logger_obj


logger = get_logger('runner', print_level=logging.INFO)


def pprint_str(*args, **kwargs):
    sio = StringIO()
    pprint(*args, stream=sio, **kwargs)
    sio.seek(0)
    return sio.read()


class Status(Enum):
    STATUS_READY = 'ready'
    STATUS_FINISHED = 'finished'
    STATUS_ERROR = 'error'
    STATUS_PENDING = 'pending'
    STATUS_RUNNING = 'running'


@dataclass
class ExecInfo:
    time: float
    depend_hash: Dict[str, str]


class DumpedTypeOperation(Enum):
    DELETE = 'DELETE'


class DumpedType:
    def __init__(self, file_name, field_name, cache=True) -> None:
        self.file_name = file_name
        self.field_name = field_name
        self.cache = cache

    def __get__(self, instance, owner):
        """
        Get the value of the descriptor.

        This method is used to get the value of the descriptor when accessed
        through an instance or a class.

        Parameters:
        - instance: The instance of the class that the descriptor is accessed
            through.
        - owner: The class that owns the descriptor.

        Returns:
        - The value of the descriptor.
        """
        if instance is None:
            return self
        if self.cache and hasattr(instance, self.field_name):
            return getattr(instance, self.field_name)
        else:
            file_name = os.path.join(
                getattr(instance, 'output_dump_folder'), self.file_name)
            if not os.path.exists(file_name):
                return None
            with open(file_name, 'rb') as f:
                result = pickle.load(f)
                if self.cache:
                    setattr(instance, self.field_name, result)
            return result

    def __set__(self, instance, value):
        """
        Set the value of the descriptor attribute.

        Args:
            instance: The instance of the class that the descriptor is being
                set on.
            value: The new value to set for the descriptor attribute.

        Returns:
            None
        """
        if self.cache:
            setattr(instance, self.field_name, value)
        dump_folder = getattr(instance, 'output_dump_folder')
        if not os.path.exists(dump_folder):
            os.makedirs(dump_folder, exist_ok=True)
        file_name = os.path.join(
            dump_folder, self.file_name)
        if value is DumpedTypeOperation.DELETE:
            if os.path.exists(file_name):
                os.remove(file_name)
            if hasattr(instance, self.field_name):
                delattr(instance, self.field_name)
        else:
            with open(file_name, 'wb') as f:
                pickle.dump(value, f)


class ExecEnv:
    def __init__(self, pythonpath, cwd):
        self.pythonpath = pythonpath
        self.cwd = cwd
        self.previous_dir = os.getcwd()
        self.stdout_file = None
        self.redirect_stdout = None

    def __enter__(self):
        if self.pythonpath:
            sys.path.insert(0, self.pythonpath)
        if not os.path.exists(self.cwd):
            os.makedirs(self.cwd, exist_ok=True)
        if self.cwd:
            os.chdir(self.cwd)
        self.stdout_file = open(os.path.join(self.cwd, 'stdout.txt'), 'w')
        self.redirect_stdout = contextlib.redirect_stdout(self.stdout_file)
        self.redirect_stdout.__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        self.redirect_stdout.__exit__(exc_type, exc_value, traceback)
        self.stdout_file.close()
        if self.cwd:
            os.chdir(self.previous_dir)
        if self.pythonpath:
            sys.path.remove(self.pythonpath)


class TaskSpec:
    _exec_info_dump = DumpedType(file_name='exec_info.pkl',
                                 field_name='__exec_info')
    _exec_result = DumpedType(file_name='exec_result.pkl',
                              field_name='__exec_result')

    def __init__(
            self,
            root: Optional[str],
            task_name: str,
            task_dict: Union[Dict[str, Self], None] = None
    ) -> None:
        self.root = root
        self.task_name = task_name
        self.task_dict = task_dict
        self._status = None
        self._dirty = None

    @cached_property
    def _exec_env(self) -> ExecEnv:
        return ExecEnv(self.root, self.output_dump_folder)

    @staticmethod
    def _get_output_folder(root, task_name: str):
        """
        Get the path of the output directory.

        Args:
            root (str): The root directory.
            task_name (str): The name of the task.

        Returns:
            str: The path of the output directory.
        """
        task_name = task_name[1:].replace('/', '.')
        return os.path.join(os.path.dirname(root), '__output', task_name)

    @staticmethod
    def _get_dump_file(root: str, task_name: str):
        return os.path.join(
            TaskSpec._get_output_folder(root, task_name),
            'dump.pkl'
        )

    @cached_property
    def output_dump_folder(self):
        return self._get_output_folder(self.root, self.task_name)

    @cached_property
    def output_dump_file(self):
        return self._get_dump_file(self.root, self.task_name)

    def update_status(self):
        """
        Updates the status of the task.

        If the task is not dirty, the status is set to `STATUS_FINISHED`.
        If the task is dirty, the status is set to `STATUS_PENDING` by default.
        The status is determined based on the status of the parent tasks.
        """
        if not self.dirty:
            self._status = Status.STATUS_FINISHED
            return
        self.status = Status.STATUS_PENDING
        parent_status = [self.task_dict[t].status for t in self.depend_task]
        if all(status == Status.STATUS_FINISHED for status in parent_status):
            if self.status == Status.STATUS_PENDING:
                self._status = Status.STATUS_READY
        elif Status.STATUS_ERROR in parent_status:
            self._status = Status.STATUS_ERROR
        elif Status.STATUS_PENDING in parent_status:
            self._status = Status.STATUS_PENDING
        elif Status.STATUS_RUNNING in parent_status:
            self._status = Status.STATUS_PENDING

    @property
    def status(self):
        if self._status is None:
            self.update_status()
        return self._status

    def _broadcast_status(self):
        for t in self.all_task_depend_me:
            self.task_dict[t].update_status()

    @status.setter
    def status(self, value: Status):
        """
        Set the status of the current node and update the status of all its
        child nodes.

        Args:
            value (Status): The new status value.
        """
        if value is None:
            self.update_status()
        if value == self._status:
            return
        if self._status is None:
            self._status = value
            return
        self._status = value
        self._broadcast_status()

    def _hash(self):
        """
        Calculate the MD5 hash of the code file and return the hex digest
        string.

        Returns:
            str: The hex digest string of the MD5 hash.
        """
        code_file = self.task_file
        md5_hash = md5()
        with open(code_file, 'rb') as f:
            code = f.read()
        md5_hash.update(code)
        for inherent_task in self._cfg_dict['inherent_list']:
            task_spec = TaskSpec(self.root, inherent_task)
            with open(task_spec.task_file, 'rb') as f:
                md5_hash.update(f.read())
        return md5_hash.hexdigest()

    @cached_property
    def dependent_hash(self) -> Dict[str, str]:
        depend_hash = {
            task_name: self.task_dict[task_name]._hash()
            for task_name in self._all_dependent_tasks
        }
        # Add me to dependent_hash
        depend_hash[self.task_name] = self._hash()
        return depend_hash

    def _check_dirty(self) -> bool:
        """
        Check if the current task is dirty by comparing the code hash with
        the last saved dump, as well as the dependency hash.

        Returns:
            bool: True if the task is dirty, False otherwise.
        """
        parent_dirty = [self.task_dict[t].dirty for t in self.depend_task]
        if any(parent_dirty):
            self._dirty = True
            return self._dirty
        if self._exec_info_dump is None:
            self._dirty = True
            return self._dirty
        exec_info: ExecInfo = self._exec_info_dump
        self._dirty = False
        if exec_info.depend_hash != self.dependent_hash:
            self._dirty = True
            return self._dirty
        return self._dirty

    @property
    def dirty(self):
        if self._dirty is not None:
            return self._dirty
        return self._check_dirty()

    @dirty.setter
    def dirty(self, value: bool):
        if value is None:
            self._dirty = None
            return
        assert isinstance(value, bool)
        prev_value = self._dirty
        self._dirty = value
        if prev_value is None:
            return
        if prev_value != value and value is True:
            # Propagate dirty to dependent tasks
            for t in self.depend_by:
                self.task_dict[t].dirty = True

    @cached_property
    def task_file(self):
        """
        Get the path of the task file.

        Returns:
            str: The path of the task file.
        """
        assert self.task_name.startswith('/')
        return os.path.join(self.root, self.task_name[1:] + '.py')

    @staticmethod
    def update_dict_recursive(d1, d2):
        """
        Recursively updates the first dictionary `d1` with the key-value
        pairs from the second dictionary `d2`.

        Parameters:
            - d1 (dict): The dictionary to be updated.
            - d2 (dict): The dictionary containing the key-value pairs
                to update `d1` with.
        Returns:
            None
        """
        for key, value in d2.items():
            if (
                    key in d1
                    and isinstance(d1[key], dict)
                    and isinstance(value, dict)
            ):
                TaskSpec.update_dict_recursive(d1[key], value)
            else:
                d1[key] = value

    @cached_property
    def _cfg_dict(self) -> Dict[str, Any]:
        """
        Loads the raw YAML content of the task file into a dictionary.

        Returns:
            Dict[str, Any]: The dictionary containing the YAML content.
        """
        payload = bytearray()
        with open(self.task_file, 'rb') as f:
            f.readline()
            while True:
                line = f.readline()
                if line == b'"""\n' or line == b'"""':
                    break
                payload.extend(line)
        try:
            task_info: Dict[str:Any] = yaml.safe_load(BytesIO(payload))
        except yaml.scanner.ScannerError as e:
            logger.error(f"ERROR parse {self.task_file}")
            raise e
        if task_info is None:
            task_info: Dict[str, Any] = {}
        inherent_list = []
        if 'inherent' in task_info:
            inh_path = process_path(self.task_name, task_info['inherent'])
            inh_cfg = TaskSpec(self.root, inh_path)._cfg_dict
            inherent_list.append(inh_path)
            inherent_list.extend(inh_cfg['inherent_list'])
            self.update_dict_recursive(inh_cfg, task_info)
            task_info = inh_cfg
        task_info['inherent_list'] = inherent_list
        return task_info

    @cached_property
    def _inherent_task(self) -> Optional[str]:
        """
        Returns the task name of inherent task.

        :return: inherent task name
        :rtype: str
        """
        if len(self._cfg_dict['inherent_list']) == 0:
            return None
        task_path = self._cfg_dict['inherent_list'][-1]
        return task_path

    @cached_property
    def _require_map(self) -> Dict[Union[int, str], str]:
        """
        Generates a dictionary that maps integers or strings to strings
        based on the 'require' key in the '_cfg_dict' attribute.

        This is the raw requirement dictionary parsed from the task,
        the required tasks are regulated to the relative path from root.

        NOTE: task requirement are marked with $ sign
        TODO: use $$ to represent original $

        Returns:
            Dict[Union[int, str], str]: The generated dictionary.

        Raises:
            Exception: If 'require' is neither a list nor a dictionary.
        """
        require: Dict[Union[int, str], str] = \
            self._cfg_dict.get('require', {})
        if isinstance(require, dict):
            pass
        elif isinstance(require, list):
            require = {i: v for i, v in enumerate(require)}
        else:
            raise Exception('require not list or dict')
        for k, v in require.items():
            if isinstance(v, str) and v.startswith('$'):
                task_path = v[1:]
                task_path = process_path(self.task_name, task_path)
                require[k] = f'${task_path}'
        return require

    @cached_property
    def depend_task(self) -> List[str]:
        """
        A list of dependency task_name.
        Returns:
            List[str]: A list of dependency tasks.
        """
        return list(
            map(
                lambda x: x[1:],
                filter(
                    lambda x: isinstance(x, str) and x.startswith('$'),
                    self._require_map.values()
                )
            )
        )

    @cached_property
    def depend_by(self) -> List[str]:
        """
        Get the tasks that directly depend on this task.

        Returns:
            List[str]: A list of task names that directly depend on this task.
        """
        child_tasks = []
        for task_name, task_spec in self.task_dict.items():
            if self.task_name in task_spec.depend_task:
                child_tasks.append(task_name)
        return child_tasks

    @cached_property
    def _all_dependent_tasks(self) -> List[str]:
        """
        Get all child tasks of this task using BFS.
        NOTE: the output follows the hierarchical order, i.e. the child
        task is put after the parent task

        Returns:
            List[str]: A list of unique task names.
        """
        visited = set()
        queue = [self.task_name]
        queue_idx = 0
        while len(queue) > queue_idx:
            task_name = queue[queue_idx]
            queue_idx += 1
            if task_name not in visited:
                visited.add(task_name)
                task_spec = self.task_dict[task_name]
                queue.extend([
                    task for task in task_spec.depend_task
                    if task not in visited
                ])
        return queue[1:]

    @cached_property
    def all_task_depend_me(self) -> List[str]:
        """
        Get all parent tasks of this task using BFS.
        NOTE: the output are sorted by name to keep consistent on each run.

        Returns:
            List[str]: A list of unique task names.
        """
        visited = set()
        queue = [self.task_name]
        queue_idx = 0
        while len(queue) > queue_idx:
            task_name = queue[queue_idx]
            queue_idx += 1
            if task_name not in visited:
                visited.add(task_name)
                task_spec = self.task_dict[task_name]
                queue.extend([
                    task for task in task_spec.depend_by
                    if task not in visited
                ])
        queue = queue[1:]
        return sorted(queue)

    def __repr__(self) -> str:
        lines = ''
        lines = lines + f"<Task:{self.task_name}>\n"
        for parent, me, child in zip_longest(
                self.depend_task,
                [self.task_name],
                self.depend_by, fillvalue=''
        ):
            lines = lines + f'{parent:15s}-->{me:15s}-->{child:15s}\n'
        return lines

    def _prepare_args(
            self,
            arg: Any,
    ) -> Any:
        if isinstance(arg, str) and arg.startswith('$'):
            task_name = arg[1:]
            result_dump = TaskSpec(self.root, task_name)._exec_result
            return result_dump
        else:
            return arg

    def _load_input(self) -> Tuple[List[Any], Dict[str, Any]]:
        """
        Load the input arguments for the task.
        NOTE: this function should only be called by @execute

        Returns:
            Tuple[List[Any], Dict[str, Any]]: A tuple containing the list of
                positional arguments and the dictionary of keyword arguments.
        """
        arg_keys = [key for key in self._require_map.keys()
                    if isinstance(key, int)]
        args = []
        if len(arg_keys) > 0:
            args = [None] * (max(arg_keys) + 1)
            for k in arg_keys:
                args[k] = self._prepare_args(
                    self._require_map[k]
                )
        kwargs = {k: self._prepare_args(v)
                  for k, v in self._require_map.items()
                  if isinstance(k, str)}
        return args, kwargs

    @cached_property
    def task_module_path(self):
        if self._inherent_task is not None:
            task_path = self._inherent_task
        else:
            task_path = self.task_name
        task_file = task_path
        mod_path = task_file[1:].replace('/', '.')
        return mod_path

    def execute(self):
        """
        Execute the task by importing the task module and calling the
        'run' function.

        NOTE: this function should only be called when all dependent
        task finished. No dirty check or dependency check will be invoked
        in this function.

        Returns:
            None
        """
        logger.debug(f'executing {self.task_name}@{self.task_module_path}')
        with self._exec_env:
            try:
                import importlib
                mod = importlib.import_module(self.task_module_path)
                mod.__dict__['work_dir'] = self.output_dump_folder
                if not hasattr(mod, 'run'):
                    raise NotImplementedError()
                args, kwargs = self._load_input()
                output = mod.run(*args, **kwargs)
                self._exec_result = output
            except Exception as e:
                logger.error(e, stack_info=True)
                return 1
        return 0

    @property
    def _exec_info(self) -> ExecInfo:
        """
        Returns the execution information for the current execution.

        :return: An instance of the ExecInfo class.
        :rtype: ExecInfo
        """
        exec_info = ExecInfo(
            time=time.time(),
            depend_hash=self.dependent_hash
        )
        return exec_info

    def dump_exec_info(self, exec_info=None):
        if exec_info is None:
            exec_info = self._exec_info
        self._exec_info_dump = exec_info

    def clear(self):
        logger.info(f'clearing {self.task_name}')
        if self.status != Status.STATUS_FINISHED:
            return
        self._exec_info_dump = DumpedTypeOperation.DELETE
        self._exec_result = DumpedTypeOperation.DELETE
        self.dirty = None
        self.status = None
        for related_tasks in self.depend_by:
            self.task_dict[related_tasks].clear()


class EndTask(TaskSpec):
    def __init__(
            self,
            task_dict: Dict[str, TaskSpec],
            depend_task: List[str]
    ):
        super().__init__(None, "_END_", task_dict)
        self._dirty = True
        self._depend_task = depend_task

    @property
    def depend_task(self) -> List[str]:
        return self._depend_task

    @property
    def depend_by(self) -> List[str]:
        return []

    def clear(self):
        pass

    def dependent_hash(self):
        return None


def task_dict_to_pdf(task_dict: Dict[str, TaskSpec]):
    """
    save graph to a pdf file using graphviz
    """
    from graphviz import Digraph
    dot = Digraph('G', format='pdf', filename='./export',
                  graph_attr={'layout': 'dot'})
    dot.attr(rankdir='LR')
    for k in task_dict.keys():
        dot.node(
            k,
            label=k,
        )
    for node, spec in task_dict.items():
        for b in spec.depend_task:
            dot.edge(b, node)
    dot.render(cleanup=True)


def search_for_root(base):
    path = os.path.abspath(base)
    while path != '/':
        if os.path.exists(os.path.join(path, '.root')):
            return path
        path = os.path.dirname(path)
    return None


def path_to_task_spec(tasks: List[str], root=None) -> Dict[str, TaskSpec]:
    """
    Convert the paths specified by the tasks argument into TaskSpec objects.

    Args:
        tasks (List[str]): A list of task paths.
        root (str, optional): The root directory of the tasks. If not provided,
            it will be searched for automatically.

    Returns:
        Dict[str, TaskSpec]: A dictionary of TaskSpec2 objects,
            where the keys are task names.

    Raises:
        Exception: If the task root cannot be found.
    """
    if root is None:
        root = search_for_root(tasks[0])
    if root is None:
        raise Exception(f'Cannot find task root! {tasks[0]}')
    logger.debug(f'root: {root}')
    logger.debug(f'tasks: {tasks}')
    task_names = []
    for task_path in tasks:
        task_path = os.path.abspath(task_path)
        if os.path.isdir(task_path):
            continue
        if not task_path.endswith('.py'):
            continue
            # task_path = os.path.dirname(task_path)
        # if not os.path.exists(os.path.join(task_path, 'task.py')):
        #     logger.warning(f'{task_path} is not a task')
        #     continue
        assert task_path.startswith(root)
        assert os.path.exists(task_path)
        task_name = task_path[len(root):-3]
        if task_name not in task_names:
            task_names.append(task_name)
    task_dict: Dict[str, TaskSpec] = {}
    for task_name in task_names:
        task_dict[task_name] = TaskSpec(root, task_name, task_dict=task_dict)

    return task_dict


def _dfs_build_exe_graph(
        task_specs: List[TaskSpec],
        task_dict: Dict[str, TaskSpec] = None
):
    """
    Recursively builds an execution graph for the given task specifications.
    All dependent tasks are added to the execution graph.

    Args:
        task_specs (List[TaskSpec]): A list of task specifications.
        task_dict (Dict[str, TaskSpec]): A dictionary representing the task
            specifications.

    Returns:
        None
    """
    if task_dict is None:
        task_dict = {}
    for task_spec in task_specs:
        for depend_task_name in task_spec.depend_task:
            if depend_task_name in task_dict:
                continue
            depend_task_spec = TaskSpec(
                root=task_spec.root,
                task_name=depend_task_name,
                task_dict=task_dict
            )
            logger.info(f'adding task {depend_task_name}')
            task_dict[depend_task_name] = depend_task_spec
            _dfs_build_exe_graph([depend_task_spec], task_dict)


def build_exe_graph(tasks: List[str]):
    """
    Build the execution graph for the given tasks.
    NOTE: tasks are path of tasks in FS, not the tasks names

    Args:
        tasks (List[str]): A list of task names.

    Raises:
        Exception: If the task root cannot be found.
    """
    target_tasks = path_to_task_spec(tasks)
    target_task_name = list(target_tasks.keys())
    task_dict = target_tasks
    _dfs_build_exe_graph(list(target_tasks.values()), task_dict)
    task_dict['_END_'] = EndTask(task_dict, target_task_name)
    return target_task_name, task_dict


class TaskEventType(Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name

    TASK_FINISHED = auto()
    TASK_ERROR = auto()
    TASK_INTERRUPT = auto()
    FILE_CHANGE = auto()


@dataclass
class TaskEvent:
    event_type: TaskEventType
    task_name: str
    task_root: str

    def __repr__(self) -> str:
        return f'<{self.event_type}: {self.task_name}@{self.task_root}>'


class RunnerBase(ABC):

    @abstractmethod
    def add_task(self, task_root: str, task_name: str) -> None:
        """
        Add a task to the task list.

        Args:
            task_root (str): The root directory of the task.
            task_name (str): The name of the task.

        Returns:
            None
        """
        pass

    @abstractmethod
    def get_running_tasks(self) -> List[str]:
        """
        Get the list of running processes.

        :return: A list of strings representing the names of the running
            processes.
        :rtype: List[str]
        """
        pass

    @abstractmethod
    def stop_tasks_and_wait(self, tasks: List[str]):
        pass


class PRunner(RunnerBase):

    @staticmethod
    def _run(task_root, task_name):
        task_spec = TaskSpec(task_root, task_name)
        value = task_spec.execute()
        return value

    def __init__(self, event_queue: Queue, loop: asyncio.AbstractEventLoop) -> None:
        self.event_queue = event_queue
        self.loop = loop
        self.processes: Dict[str, Process] = {}
        self.executor = concurrent.futures.ProcessPoolExecutor(max_workers=6)

    async def __aenter__(self):
        pass

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        logger.info('exiting ...')
        for p in self.processes.values():
            if p.is_alive():
                p.terminate()
            p.join()
        self.processes.clear()

    async def _run_task(self, task_root: str, task_name: str):
        logger.info(f'running task {task_name} ...')

        def _start_process_return_exit_code():
            p = Process(
                target=self._run,
                args=(task_root, task_name))
            self.processes[task_name] = p
            p.start()
            p.join()
            logger.info(p.exitcode)
            return p.exitcode
        exit_code = await self.loop.run_in_executor(
            self.executor,
            self._run,
            task_root, task_name
        )
        if exit_code == 0:
            await self.event_queue.put(TaskEvent(
                TaskEventType.TASK_FINISHED, task_name, task_root))
            logger.info(f'finished task {task_name}')
        else:
            await self.event_queue.put(TaskEvent(
                TaskEventType.TASK_ERROR, task_name, task_root))
            logger.info(f'Error task {task_name}')

    def add_task(self, task_root: str, task_name: str) -> None:
        self.loop.create_task(self._run_task(task_root, task_name))

    def get_running_tasks(self) -> List[str]:
        for k, v in self.processes.items():
            if not v.is_alive():
                v.join()
                del self.processes[k]
        return list(self.processes.keys())

    def stop_tasks_and_wait(self, tasks: List[str]):
        for task in tasks:
            if task not in self.processes:
                continue
            if not self.processes[task].is_alive():
                del self.processes[task]
                continue
            logger.info(f'killing task {task}')
            self.processes[task].terminate()
            self.processes[task].join()
            self.processes.pop(task)


class FileWatcher:
    def __init__(
            self,
            root: str,
            queue: Queue,
            loop: Optional[asyncio.AbstractEventLoop] = None):
        self.root = root
        self.stop_event = asyncio.Event()
        self.event_queue = queue
        self.loop = loop
        self.async_task: Optional[asyncio.Task] = None

    async def _func(self):
        logger.info(f'watching {self.root} ...')
        async for event in awatch(self.root, stop_event=self.stop_event):
            files = set([x[1] for x in event])
            for f in files:
                await self.event_queue.put(TaskEvent(
                    TaskEventType.FILE_CHANGE, f, 'None'))

    async def __aenter__(self):
        return self.start()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    def start(self):
        if self.async_task is not None:
            logger.warning('already started')
            self.stop()
        self.stop_event.clear()
        self.async_task = self.loop.create_task(self._func())
        return self

    async def stop(self):
        self.stop_event.set()
        await self.async_task


class SchedulerCallbackBase(ABC):

    def status_change(self, event: Status, task_spec: TaskSpec):
        pass


class _CallBackCollection(SchedulerCallbackBase):
    def __init__(
            self,
            callbacks: Optional[List[SchedulerCallbackBase]] = None
    ) -> None:
        super().__init__()
        if callbacks is None:
            self.callbacks = []
        else:
            self.callbacks = callbacks

    def _handle(self, payload: Any, task_spec: TaskSpec):
        caller_frame = inspect.currentframe().f_back
        caller_function_name = caller_frame.f_code.co_name
        for cb in self.callbacks:
            func = getattr(cb, caller_function_name)
            func(payload, task_spec)

    def status_change(self, event: Status, task_spec: TaskSpec):
        return self._handle(event, task_spec)


class SchedulerEventType(Enum):
    STATUS_CHANGE = auto()


@dataclass
class SchedulerEvent:
    event: SchedulerEventType
    msg: Any


class CallbackCaller:
    def __init__(self, callback: SchedulerCallbackBase):
        self.callback = callback


class TaskScheduler:
    def __init__(
            self,
            target,
            loop: Optional[asyncio.AbstractEventLoop] = None,
    ) -> None:
        if loop is None:
            loop = asyncio.new_event_loop()
        self.target, self.task_dict = build_exe_graph(target)
        self._task_event_queue = Queue(loop=loop)
        self._call_back = _CallBackCollection()
        self._call_back_caller = None
        self.main_loop_task: Optional[asyncio.Task] = None
        self._sche_event_queue = Queue(loop=loop)
        self._callback_caller = None
        self._callback_caller_event = None
        self.loop = loop
        self.runner = PRunner(self._task_event_queue, self.loop)

    def _restart_cb_caller(self):
        # self._callback_caller
        pass

    def on_event(self, cb: SchedulerCallbackBase):
        self._call_back.callbacks.append(cb)
        return self

    def _get_ready(self) -> Union[str, None]:
        """
        Get a task that is ready to be executed.

        :return: Union of string or None.
        """
        for task in self.task_dict.values():
            if task.status == Status.STATUS_READY:
                return task.task_name
        return None

    def get_ready_set_running(self):
        ready_task = self._get_ready()
        if ready_task:
            self.set_running(ready_task)
        return ready_task

    def set_status(self, task: str, status: Status):
        assert isinstance(task, str), task
        assert task in self.task_dict, f'{task} is not a valid task'
        self.task_dict[task].status = status
        self.loop.create_task(self._sche_event_queue.put(SchedulerEvent(
            SchedulerEventType.STATUS_CHANGE,
            task,
        )))

    def set_finished(self, task: str):
        self.set_status(task, Status.STATUS_FINISHED)

    def set_error(self, task: str):
        self.set_status(task, Status.STATUS_ERROR)

    def set_running(self, task: str):
        self.set_status(task, Status.STATUS_RUNNING)

    def print_status(self):
        for t in self.task_dict.values():
            print(f'{t.task_name}: {t.status}')

    async def clear(self, tasks: Union[List[str], str, None] = None):
        if tasks is None:
            tasks = self.task_dict.keys()
        elif isinstance(tasks, str):
            tasks = [tasks]
        for task in tasks:
            logger.info(f'clearing {task}')
            self.task_dict[task].clear()
            await self._task_event_queue.put(
                TaskEvent(TaskEventType.TASK_INTERRUPT, task, self.root))

    @cached_property
    def root(self):
        return self.task_dict[self.target[0]].root

    @cached_property
    def file_watcher(self):
        return FileWatcher(self.root, self._task_event_queue, self.loop)

    async def _reload(self):
        """
        1. loop through all tasks
            2. Initiate new TaskSpec, compare dependency_hash with old TaskSpec
            3. Add new TaskSpec to task_dict if task changed
        4. calculate depend_by
        4. change status to broadcast the task status
        5. remove task event in event queue for changed task with all children
        """
        queued_event = []
        while not self._task_event_queue.empty():
            queued_event.append(await self._task_event_queue.get())
        _, new_task_dict = build_exe_graph(
            [self.task_dict[x].task_file for x in self.target])
        removed_tasks = list(set(self.task_dict.keys()) -
                             set(new_task_dict.keys()))
        for k in removed_tasks:
            self.task_dict.pop(k)
            logger.info(f'removing task {k}')
        # changed or added tasks
        changed_tasks = []
        for task_name, new_spec in new_task_dict.items():
            if task_name not in self.task_dict:
                logger.info(f'adding task {task_name}')
                self.task_dict[task_name] = new_spec
                changed_tasks.append(task_name)
            else:
                old_spec = self.task_dict[task_name]
                if old_spec.dependent_hash != new_spec.dependent_hash:
                    self.task_dict[task_name] = new_spec
                    changed_tasks.append(task_name)
        for task_name in changed_tasks:
            self.task_dict[task_name].update_status()
        all_influenced_tasks = set()
        for task in changed_tasks:
            all_influenced_tasks.add(task)
            all_influenced_tasks.update(
                set(self.task_dict[task].all_task_depend_me))
        logger.info(f'all_influenced_tasks: {all_influenced_tasks}')
        self.runner.stop_tasks_and_wait(list(all_influenced_tasks))
        for event in queued_event:
            if event.event_type == TaskEventType.FILE_CHANGE:
                continue
            if event.task_name not in self.task_dict:
                continue
            if event.task_name in changed_tasks:
                continue
            await self._task_event_queue.put(event)

    async def _run(self, once=True):
        """
        Runs the main loop of the program. This function continuously checks
        for ready tasks and executes them. If there are no ready tasks, it
        waits for an event from the event queue. When an event is received,
        it checks the event type and performs the corresponding action.

        If the ready task is "_END_", it indicates that all tasks have
        finished and the function breaks out of the loop. Otherwise, it
        retrieves the task specification for the ready task and adds it to
        the task runner for execution.

        Returns:
            None
        """
        logger.info('running...')
        async with self.runner, self.file_watcher:
            while True:
                ready_task = self.get_ready_set_running()
                # logger.info(f'got ready task:{ready_task}')
                if ready_task is None:
                    try:
                        event = await self._task_event_queue.get()
                    except ValueError:
                        logger.info('stop running')
                        self.runner.stop_tasks_and_wait(
                            self.runner.get_running_tasks())
                        break
                    except Exception as e:
                        logger.error(e, stack_info=True)
                        logger.error(f'{type(e)}')
                        break
                    logger.debug(f'got event:{event}')
                    if event is None:
                        logger.info('stop running')
                        break
                    elif event.event_type == TaskEventType.TASK_FINISHED:
                        self.set_finished(event.task_name)
                        self.task_dict[event.task_name].dump_exec_info()
                    elif event.event_type == TaskEventType.TASK_ERROR:
                        self.set_error(event.task_name)
                        logger.info(f'task:{event.task_name} is errored')
                    elif event.event_type == TaskEventType.TASK_INTERRUPT:
                        logger.info(f'task:{event.task_name} is interrupted')
                        pass
                    elif event.event_type == TaskEventType.FILE_CHANGE:
                        logger.info(f'file changed:{event.task_name}')
                        await self._reload()
                    else:
                        raise NotImplementedError()
                else:
                    # logger.info(f'got ready task:{ready_task}')
                    if ready_task == '_END_':
                        logger.info('all tasks are finished')
                        if once:
                            logger.info("in single step mode, exiting...")
                            break
                        continue
                    task_spec = self.task_dict[ready_task]
                    self.runner.add_task(task_spec.root, task_spec.task_name)
        # self.runner = None
        logger.info('exiting _run')

    async def stop(self):
        logger.info('stop running...')
        if self.main_loop_task is None:
            logger.error('not running, exit')
            return
        logger.info('sending stop signal')
        await self._task_event_queue.put(None)
        logger.info('waiting for main_loop_task')
        await self.main_loop_task
        while not self._task_event_queue.empty():
            await self._task_event_queue.get()
        self.main_loop_task = None
        # self._task_event_queue = None
        logger.info('stopped')

    def run(self, once=False, daemon=False):
        """
        add _run to loop if daemon,
        else run _run and await
        """
        if self.main_loop_task is not None and not self.main_loop_task.done():
            logger.error('already running !!')
            return
        if daemon:
            self.main_loop_task = self.loop.create_task(self._run(once))
        else:
            self.loop.run_until_complete(self._run(once))

    @property
    def sche_event_queue(self):
        return self._sche_event_queue


def serve_target(tasks: List[dir]):
    logger.info(f'serve2 {tasks}')
    scheduler = TaskScheduler(tasks, asyncio.new_event_loop())
    # task_dict_to_pdf(scheduler.task_dict)
    logger.debug(scheduler.task_dict)
    scheduler.run(once=False, daemon=False)


def exec_task(task_root, task_name):
    task_spec = TaskSpec(task_root, task_name)
    task_spec.execute()


def process_path(task_name: str, path: str):
    task_name = os.path.dirname(task_name)
    if not path.startswith('/'):
        path = os.path.join(task_name, path)
    path_filtered = []
    for sub_path in path.split('/'):
        if sub_path == '.':
            continue
        if sub_path == '..':
            path_filtered.pop()
            continue
        path_filtered.append(sub_path)
    if path_filtered[0] != '':
        path_filtered = task_name.split('/') + path_filtered
    path_new = '/'.join(path_filtered)
    return path_new
