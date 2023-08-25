from enum import Enum, auto
import asyncio
import logging
import os
import os.path
import pickle
import signal
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from functools import cached_property
from hashlib import md5
from io import BytesIO, StringIO
from pprint import pprint
from queue import Queue
from typing import Any, Callable, Dict, List, Set, Tuple
from typing_extensions import Self
from typing import Union
from itertools import zip_longest

import yaml

try:
    import networkx as nx
    import socketio
    from watchfiles import awatch
except ImportError:
    pass


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
    code_hash: str
    depend_hash: Dict[str, str]


@dataclass
class _TaskSpec:
    task_root: str
    task_name: str
    require: Union[Dict[str, str], List[str], None] = None
    inherent: str = None
    virtual: bool = False
    rerun: bool = False
    export: bool = False
    # expire to run the task if following tasks ends
    expire: int = -1
    end_time: int = -1
    remove: bool = False
    depend: Union[List[str], None] = None


class DumpedType():
    def __init__(self, file_name=None, field_name=None, cache=True) -> None:
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
        with open(file_name, 'wb') as f:
            pickle.dump(value, f)


class TaskSpec2:

    _exec_info: ExecInfo = DumpedType(file_name='exec_info.pkl',
                                      field_name='__exec_info')
    _exec_result = DumpedType(file_name='exec_result.pkl',
                              field_name='__exec_result')

    def __init__(
            self,
            root: str,
            task_name: str,
            task_dict: Dict[str, Self] = None
    ) -> None:
        self.root = root
        self.task_name = task_name
        self.task_dict = task_dict
        self._status = None
        self._dirty = None

    @ staticmethod
    def _get_output_folder(root, taskname: str):
        """
        Get the path of the output directory.

        Args:
            root (str): The root directory.
            taskname (str): The name of the task.

        Returns:
            str: The path of the output directory.
        """
        taskname = taskname[1:].replace('/', '.')
        return os.path.join(os.path.dirname(root), '_output', taskname)

    @ staticmethod
    def _get_dump_file(root: str, taskname: str):
        return os.path.join(
            TaskSpec2._get_output_folder(root, taskname),
            'dump.pkl'
        )

    @cached_property
    def output_dump_folder(self):
        return self._get_output_folder(self.root, self.task_name)

    @cached_property
    def output_dump_file(self):
        return self._get_dump_file(self.root, self.task_name)

    def _update_status(self):
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
            self._update_status()
        return self._status

    @status.setter
    def status(self, value: Status):
        """
        Set the status of the current node and update the status of all its
        child nodes.

        Args:
            value (Status): The new status value.
        """
        if value == self._status:
            return
        if self._status is None:
            self._status = value
            return
        self._status = value
        for t in self._all_task_depend_me:
            self.task_dict[t]._update_status()

    def _hash(self):
        """
        Calculate the MD5 hash of the code file and return the hex digest
        string.

        TODO: add other dependency too·

        Returns:
            str: The hex digest string of the MD5 hash.
        """
        code_file = self._task_file
        with open(code_file, 'rb') as f:
            code = f.read()
        md5_hash = md5(code)
        return md5_hash.hexdigest()

    @property
    def _dependent_hash(self):
        depend_hash = {
            task_name: self.task_dict[task_name]._hash()
            for task_name in self._all_dependent_tasks
        }
        return depend_hash

    def _check_dirty(self) -> bool:
        """
        Check if the current task is dirty by comparing the code hash with
        the last saved dump, as well as the dependency hash.

        TODO: finish this

        Returns:
            bool: True if the task is dirty, False otherwise.
        """
        parent_dirty = [self.task_dict[t].dirty for t in self.depend_task]
        if any(parent_dirty):
            self._dirty = True
            return self._dirty
        if self._exec_info is None:
            self._dirty = True
            return self._dirty
        exec_info: ExecInfo = self._exec_info
        self._dirty = False
        if exec_info.code_hash != self._hash():
            self._dirty = True
        for t in self.depend_task:
            if (
                    not self._dirty
                    and self.task_dict[t]._hash() != exec_info.depend_hash[t]
            ):
                self._dirty = True
        for t in self.depend_task:
            if self.task_dict[t]._hash() != self._exec_info.code_hash[t]:
                logger.info(f'{t} updated hash mismatch')
                self._dirty = True
        return self._dirty

    @property
    def dirty(self):
        if self._dirty is not None:
            return self._dirty
        return self._check_dirty()

    @dirty.setter
    def dirty(self, value: bool):
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
    def _task_file(self):
        """
        Get the path of the task file.

        Returns:
            str: The path of the task file.
        """
        assert self.task_name.startswith('/')
        return os.path.join(self.root, self.task_name[1:], 'task.py')

    @cached_property
    def _cfg_dict(self) -> Dict[str, Any]:
        """
        Loads the raw YAML content of the task file into a dictionary.

        Returns:
            Dict[str, Any]: The dictionary containing the YAML content.
        """
        payload = bytearray()
        with open(self._task_file, 'rb') as f:
            f.readline()
            while True:
                line = f.readline()
                if line == b'"""\n' or line == b'"""':
                    break
                payload.extend(line)
        try:
            task_info = yaml.safe_load(BytesIO(payload))
        except yaml.scanner.ScannerError as e:
            logger.error(f"ERROR parse {self._task_file}")
            raise e
        if task_info is None:
            task_info = {}
        return task_info

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
        A list of dependency taskname.
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
    def _all_task_depend_me(self) -> List[str]:
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

    def _dump_result(self, output: Any):
        """
        Dump the result of the task to the output dump file.
        NOTE: this function should only be called by @execute

        Args:
            output (Any): The output of the task.

        Returns:
            None
        """
        payload = output
        if not os.path.exists(os.path.dirname(self.output_dump_file)):
            os.makedirs(os.path.dirname(self.output_dump_file))
        pickle.dump(payload, open(self.output_dump_file, 'wb'))

    def _prepare_args(
            self,
            arg: Any,
    ) -> Any:
        if isinstance(arg, str) and arg.startswith('$'):
            task_name = arg[1:]
            result_dump = TaskSpec2(self.root, task_name)._exec_result
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
            args = [None] * max(arg_keys)
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
        task_path = os.path.join(self.task_name, 'task')
        return task_path[1:].replace('/', '.')

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
        logger.info(f'executing {self.task_name}@{self.root}')
        sys.path.append(self.root)
        import importlib
        mod = importlib.import_module(self.task_module_path)
        mod.__dict__['work_dir'] = self.output_dump_folder
        if not hasattr(mod, 'run'):
            raise NotImplementedError()
        args, kwargs = self._load_input()
        output = mod.run(*args, **kwargs)
        self._exec_result = output
        return True

    @property
    def exec_info(self) -> ExecInfo:
        pass


class EndTask(TaskSpec2):
    def __init__(
            self,
            task_dict: Dict[str, Self],
            depend_task: List[str]
    ):
        super().__init__(None, "_END_", task_dict)
        self._status = Status.STATUS_PENDING
        self._dirty = True
        self._depend_task = depend_task

    @property
    def depend_task(self) -> List[str]:
        return self._depend_task

    @property
    def depend_by(self) -> List[str]:
        return []


def task_dict_to_pdf(task_dict: Dict[str, TaskSpec2]):
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
        path = os.path.split(path)[0]
    return None


def _dfs_build_exe_graph(
        root: str,
        task_names: List[str],
        task_dict: Dict[str, TaskSpec2] = None
):
    """
    This function builds the execution graph for the given root and task names.

    Args:
        root (str): The root directory.
        task_names (List[str]): The list of task names.
        task_dict (Dict[str, TaskSpec2], optional): The dictionary of task
            specifications. Defaults to None.
    """
    if task_dict is None:
        task_dict = {}
    t_to_add = []
    for task_name in task_names:
        if task_name in task_dict:
            continue
        t_to_add.append(task_name)
        task_spec = TaskSpec2(root, task_name, task_dict)
        task_dict[task_name] = task_spec
        _dfs_build_exe_graph(root, task_spec.depend_task, task_dict)


def build_exe_graph(tasks: List[str], root=None):
    """
    Build the execution graph for the given tasks.
    NOTE: tasks are path of tasks in FS, not the tasks names

    Args:
        tasks (List[str]): A list of task names.
        root (str, optional): The root directory of the tasks. If not provided,
            it will be searched for automatically.

    Raises:
        Exception: If the task root cannot be found.
    """
    if root is None:
        root = search_for_root(tasks[0])
    if root is None:
        raise Exception('Cannot find task root!')
    # do a bfs to build TaskSpec graph
    target_task_name = []
    for t in tasks:
        t = os.path.abspath(t)
        if os.path.isfile(t):
            t = os.path.dirname(t)
        if not os.path.exists(os.path.join(t, 'task.py')):
            logger.warning(f'{t} is not a task')
            continue
        assert t.startswith(root)
        assert os.path.exists(t)
        task_name = t[len(root):]
        if task_name == '':
            task_name = '/'
        if task_name not in target_task_name:
            target_task_name.append(task_name)
    # logger.info(target_task_name)
    task_dict: Dict[str, TaskSpec2] = {}
    _dfs_build_exe_graph(root, target_task_name, task_dict)
    task_dict['_END_'] = EndTask(task_dict, target_task_name)
    # logger.info(task_dict)
    # for spec in task_dict.values():
    #     logger.info(spec)
    return target_task_name, task_dict


class EventType(Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name

    TASK_FINISHED = auto()
    TASK_ERROR = auto()
    TASK_INTERRUPT = auto()


@dataclass
class SchedulerEvent:
    event_type: EventType
    task_name: str
    task_root: str

    def __repr__(self) -> str:
        return f'<{self.event_type}: {self.task_name}@{self.task_root}>'


class Runner():
    def __init__(self, event_queue: Queue) -> None:
        self.event_queue = event_queue

    def add_task(self, task_root: str, task_name: str) -> None:
        """
        Add a task to the task list.

        Args:
            task_root (str): The root directory of the task.
            task_name (str): The name of the task.

        Returns:
            None
        """
        task_spec = TaskSpec2(task_root, task_name)
        task_spec.execute()
        self.event_queue.put(SchedulerEvent(
            EventType.TASK_FINISHED, task_name, task_root))


class TaskSche2():
    def __init__(self, target, get_runner: Callable[[Queue], Runner]) -> None:
        self.target, self.task_dict = build_exe_graph(target)
        self.event_queue = Queue()
        if get_runner is not None:
            self.runner = get_runner(self.event_queue)

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

    def set_status(self, task: str, status: str):
        assert isinstance(task, str), task
        assert task in self.task_dict, f'{task} is not a valid task'
        self.task_dict[task].status = status

    def set_finished(self, task: str):
        self.set_status(task, Status.STATUS_FINISHED)

    def set_error(self, task: str):
        self.set_status(task, Status.STATUS_ERROR)

    def set_running(self, task: str):
        self.set_status(task, Status.STATUS_RUNNING)

    def print_status(self):
        for t in self.task_dict.values():
            print(f'{t.task_name}: {t.status}')

    def run(self):
        logger.info('running...')
        while True:
            ready_task = self.get_ready_set_running()
            if ready_task is None:
                event = self.event_queue.get()
                logger.info(f'got event:{event}')
                if event.event_type == EventType.TASK_FINISHED:
                    self.set_finished(event.task_name)
                elif event.event_type == EventType.TASK_ERROR:
                    self.set_error(event.task_name)
                elif event.event_type == EventType.TASK_INTERRUPT:
                    self.set_error(event.task_name)
                else:
                    raise NotImplementedError()
            else:
                logger.info(f'got ready task:{ready_task}')
                if ready_task == '_END_':
                    logger.info('all tasks are finished')
                    break
                task_spec = self.task_dict[ready_task]
                self.runner.add_task(task_spec.root, task_spec.task_name)
        logger.info('done...')


def serve_target2(tasks: List[dir]):
    logger.info(tasks)
    build_exe_graph(tasks)
    pass


def exec_task(task_root, task_name):
    task_spec = TaskSpec2(task_root, task_name)
    task_spec.execute()


class TaskSpec(_TaskSpec):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.code_hash = self._get_hash()

        require = self.get('require', {})
        if require is None:
            require = {}
        if isinstance(require, list):
            it = enumerate(require)
        else:
            it = require.items()
        for k, v in it:
            require[k] = process_reference(v, self)
            self.require = require

    def _get_hash(self) -> str:
        m = md5()
        m.update(open(self.code_file, 'rb').read())
        for f in self.depend_files:
            m.update(open(f, 'rb').read())
        return m.hexdigest()

    def __getitem__(self, item):
        return getattr(self, item)

    def to_json(self):
        return dict(
            task_name=self.task_name,
            dependent_tasks=self.dependent_tasks,
            inherent=self.inherent,
        )

    def get(self, name, default=None):
        if hasattr(self, name):
            return getattr(self, name)
        return default

    def __repr__(self) -> str:
        tt = f'TaskSpec({self.task_root}@{self.task_name})'
        if self.inherent_task is not None:
            tt += f'->{self.inherent_task}'
        return tt

    def __hash__(self) -> int:
        return hash((self.task_root, self.task_name))

    def __eq__(self, __o: object) -> bool:
        if isinstance(__o, TaskSpec):
            return self.task_name == __o.task_name
        return False

    def __gt__(self, __o: object) -> bool:
        if isinstance(__o, TaskSpec):
            return self.task_name > __o.task_name
        return False

    @cached_property
    def depend_files(self):
        result = []
        if self.inherent is not None:
            result.append(os.path.join(self.task_root,
                                       self.inherent_task[1:], 'task.py'))
        if self.depend is not None:
            for dep in self.depend:
                if not os.path.exists(dep):
                    # regard it as a task file
                    dep = process_path(self.task_name, dep)
                    dep = os.path.join(self.task_root, dep[1:], 'task.py')
                    if not os.path.exists(dep):
                        logger.error(f"Dependency {dep} does not exist.")
                        raise Exception(f"Dependency {dep} does not exist.")
                result.append(dep)
        return result

    def clean(self):
        if not (os.path.exists(self.result_file) or
                os.path.exists(self.result_info)):
            return True
        try:
            os.remove(self.result_info)
            os.remove(self.result_file)
            return True
        except Exception as e:
            logger.info(e)
        return False

    @property
    def inherent_task(self):
        if self.inherent is None:
            return None
        return process_path(self.task_name, self.inherent)

    @property
    def task_dir(self) -> str:
        return os.path.join(self.task_root, self.task_name[1:])

    @property
    def code_file(self) -> str:
        return os.path.join(self.task_dir, 'task.py')

    @property
    def code_update_time(self) -> float:
        code_update = os.stat(self.code_file).st_mtime
        for dep in self.depend_files:
            code_update = max(
                code_update,
                os.stat(dep).st_mtime)
        return code_update

    @property
    def result_file(self) -> str:
        return os.path.join('_output', self.task_dir, 'result.pkl')

    @property
    def result_info(self) -> str:
        return os.path.join('_output', self.task_dir, 'result_info.pkl')

    @property
    def dirty(self) -> bool:
        """
        Return true if the task has been modified since the last time it was
        run. Note that this does not count the status of dependent tasks.
        """
        if self.rerun:
            return True
        if self.expire > 0 and (self.end_time + self.expire < time.time()):
            return True
        code_update = self.code_update_time
        try:
            result_update = os.stat(self.result_file).st_mtime
            if code_update < result_update:
                return False
            else:
                code_hash, t = pickle.load(open(self.result_info, 'rb'))
                if code_hash == self.code_hash:
                    os.utime(self.result_file)
                    return False
        except FileNotFoundError:
            return True
        return True

    @cached_property
    def require_dict(self) -> Dict[Union[int, str], str]:
        if isinstance(self.require, dict):
            return self.require
        elif isinstance(self.require, list):
            return {i: v for i, v in enumerate(self.require)}
        elif self.require is None:
            return {}
        raise Exception('require not list or dict or NONE')

    @property
    def dependent_tasks(self):
        dep = []
        for _, v in self.require_dict.items():
            if isinstance(v, str) and v.startswith('$'):
                dep.append(v[1:])
        return dep

    @property
    def module_path(self):
        if self.inherent_task is not None:
            mod = os.path.join(self.inherent_task, 'task')
        else:
            mod = os.path.join(self.task_name, 'task')
        return mod.replace('/', '.')[1:]

    @property
    def call_arguments(self):
        kwargs = {}
        # logger.debug(self.require_dict)
        for k, v in self.require_dict.items():
            if not (isinstance(v, str) and v.startswith('$')):
                kwargs[k] = v
                continue
            result_file = os.path.join(
                '_output', self.task_root, v[2:], 'result.pkl')
            result = pickle.load(open(result_file, 'rb'))
            kwargs[k] = result
        # logger.info(kwargs)
        if isinstance(self.require, list):
            args = [None] * len(kwargs)
            for k, v in kwargs.items():
                args[k] = v
            return args
        return kwargs

    def dump_result(self, result):
        pickle.dump(result, open(self.result_file, 'wb'))
        pickle.dump((self.code_hash, time.time()),
                    open(self.result_info, 'wb'))


class TaskSpecEndTask(TaskSpec):
    task_name = '_end'

    def __init__(self, dependent_tasks):
        super().__init__(task_name=self.task_name, task_root=None)
        self._dependent_tasks = dependent_tasks

    @property
    def dirty(self) -> bool:
        return True

    def _get_hash(self) -> str:
        return '0' * 32

    @property
    def dependent_tasks(self):
        return self._dependent_tasks


def process_path(task_name, path):
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


def process_reference(ref, task_spec: TaskSpec):
    if not isinstance(ref, str):
        return ref
    if ref.startswith('$'):
        path = ref[1:]
        path_new = '$' + process_path(task_spec.task_name, path)
        return path_new
    return ref


class QUERY_EXIST:
    def __init__(self):
        raise Exception


class Graph:
    """a graph class to calculate task dependent relationships"""

    def __init__(self, nodes=None, edges=None):
        self.nodes: Set[str] = set()
        self.dag: Dict[str, Set[str]] = defaultdict(set)
        self.in_degree: Dict[str, Set[str]] = defaultdict(set)
        self.property: Dict[str, Dict[str, Any]] = defaultdict(dict)

        nodes: List[str] = nodes or []
        edges: List[Tuple[str, str]] = edges or []
        for n in nodes:
            self.add_node(n)
        for a, b in edges:
            self.add_edge(a, b)

    def clear(self):
        self.nodes.clear()
        self.dag.clear()
        self.in_degree.clear()
        self.property.clear()

    def add_node(self, node: str, **kwargs):
        self.nodes.add(node)
        self.property[node] = kwargs

    def add_edge(self, a, b):
        """
        add an edge from a to b
        """
        for node in (a, b):
            if node not in self.nodes:
                logger.warning(f'node {node} not in nodes')
        self.dag[a].add(b)
        self.in_degree[b].add(a)

    def remove_edge(self, a, b):
        """
        remove an edge from a to b
        """
        assert a in self.nodes
        assert b in self.nodes
        self.dag[a].remove(b)
        self.in_degree[b].remove(a)

    def remove_node(self, n):
        """
        remove node n from the graph
        """
        assert n in self.nodes
        for t in self.dag[n]:
            self.in_degree[t].remove(n)
        if n in self.dag:
            del self.dag[n]
        del self.property[n]
        self.nodes.remove(n)

    def _agg(self, map_func, reduce_func, node, result):
        if node not in self.nodes:
            logger.error(f'node {node} not in nodes')
        if node in result:
            return result[node]
        self_map = map_func(self.property[node])
        args = [
            self._agg(map_func, reduce_func, x, result)
            for x in self.in_degree[node]
        ]
        args.append(self_map)
        r = reduce_func(args)
        result[node] = r
        return r

    def aggregate(self, map_func, reduce_func, nodes=None, update=None):
        """
        aggregate the property of a node
        """
        result = {}
        if nodes is None:
            nodes = self.nodes
        elif isinstance(nodes, str):
            nodes = [nodes]
        for node in nodes:
            self._agg(map_func, reduce_func, node, result)
        # update property
        if isinstance(update, str):
            for k, v in result.items():
                self.property[k][update] = v
        # elif isinstance(update, callable):
        #     for k, v in result.items():
        #         update(self.property[k], v)
        elif update is None:
            pass
        else:
            logger.error(f'error update value {update}')
            raise Exception()
        return result

    def any(self, map_func):
        """
        return list of nodes if any func(n) is True for n in its child nodes
        """
        return self.aggregate(map_func, any)

    def all(self, map_func):
        """
        return list of nodes if all func(n) is True for n in its child nodes
        """
        return self.aggregate(map_func, all)

    @staticmethod
    def match_query(prop: Dict[str, Any], query: Dict[str, Any]) -> bool:
        for k, v in query.items():
            if k not in prop:
                return False
            if v is QUERY_EXIST:
                return True
            p = prop[k]
            if isinstance(v, list):
                if p not in v:
                    return False
            else:
                if p != v:
                    return False
        return True

    def match_one(self, query: Dict[str, Any]):
        for task_name, prop in self.property.items():
            if self.match_query(prop, query):
                return task_name
        return None

    def check_all(self, query: Dict[str, Any]):
        for task_name, prop in self.property.items():
            if not self.match_query(prop, query):
                return False
        return True

    def match_all(self, query: Dict[str, Any]):
        matched = []
        for task_name, prop in self.property.items():
            if self.match_query(prop, query):
                matched.append(task_name)
        return matched

    def node_label(self, node: str):
        return '\n'.join([f'{k}:{v}' for k, v in self.property[node].items()])

    def to_pdf(self, path):
        """
        save graph to a pdf file using graphviz
        """
        from graphviz import Digraph
        dot = Digraph('G', format='pdf', filename=path,
                      graph_attr={'layout': 'dot'})
        dot.attr(rankdir='LR')
        color_map = dict(
            ready='orange',
            finished='lightgreen',
            pending='red',
            running='lightblue',
        )
        for node in self.nodes:
            color = color_map.get(self.property[node].get(
                'status', 'None exist'), 'yellow')
            dot.node(
                node,
                label=f'{node}\n' + self.node_label(node),
                fillcolor=color,
                style="filled",
                color=color,
            )
        for a, bs in self.dag.items():
            for b in bs:
                dot.edge(a, b)
        dot.render(cleanup=False)


def match_targets(strs: List[str], patterns: List[str]):
    """
    return list of tasks that can match any of the patterns
    each pattern is a str of wildcards with * to match any number of characters
    """
    matches = set()
    for pattern in patterns:
        for s in strs:
            if s in matches:
                continue
            if len(s) < len(pattern):
                continue
            i, j = 0, 0
            while i < len(s) and j < len(pattern):
                if pattern[j] == '*':
                    while j < len(pattern) and pattern[j] == '*':
                        j += 1
                    if j == len(pattern):
                        matches.add(s)
                        break
                    while i < len(s) and s[i] != pattern[j]:
                        i += 1
                elif pattern[j] == '?' or pattern[j] == s[i]:
                    i += 1
                    j += 1
                else:
                    break
                if j == len(pattern) and i == len(s):
                    matches.add(s)
    return list(matches)


class Scheduler:
    def __init__(
            self,
            root: str = 'None',
            targets=Union[str, List[str], None],
            sock_addr=None,
    ):
        self._g = Graph()
        if isinstance(targets, str):
            targets = [targets]
        self._targets = targets
        self.targets = None
        self.tasks: Dict[str, TaskSpec] = {}
        self.root = root
        self.processes: Dict[
            str,
            Tuple[asyncio.subprocess.Process, asyncio.Task]] = {}
        self.sock_addr = sock_addr
        self.sio: socketio.AsyncClient = socketio.AsyncClient()
        self.stop_new_job = False

        self.finished = False
        self.task_queue = Queue()

        self.processes_lock: Union[asyncio.Lock, None] = None
        self.new_task_lock: Union[asyncio.Lock, None] = None
        tasks = parse_target(self.root)
        self.add_tasks(list(tasks.values()))

    def add_tasks(self, tasks: List[TaskSpec]):
        if '_end' in self._g.nodes:
            self._g.remove_node('_end')
        out_degree = defaultdict(lambda: 0)
        for t in tasks:
            assert t.task_name not in self.tasks
            self.tasks[t.task_name] = t
        targets = self._targets
        if targets is None:
            targets = self.tasks.keys()
        elif isinstance(targets, list):
            targets = match_targets(list(self.tasks.keys()), targets)
        else:
            raise TypeError(
                f'targets must be a list or None, not type {type(targets)}')
        bfs_queue = Queue()
        running_tasks = set()
        for t in targets:
            bfs_queue.put(t)
        while not bfs_queue.empty():
            cur_task = bfs_queue.get()
            if cur_task in running_tasks:
                continue
            running_tasks.add(cur_task)
            for dep_task in self.tasks[cur_task].dependent_tasks:
                out_degree[dep_task] += 1
                bfs_queue.put(dep_task)
        running_tasks.add('_end')
        sink_nodes = []
        for t in targets:
            if t not in out_degree and not self.tasks[t].virtual:
                sink_nodes.append(t)
        self.targets = sink_nodes
        logger.info(f'adding target {sink_nodes}')
        task_end = TaskSpecEndTask(sink_nodes)
        self.tasks[task_end.task_name] = task_end
        all_task = tasks + [task_end]
        for t in all_task:
            self._g.add_node(t.task_name)
            self._g.property[t.task_name]['dirty'] = t.dirty
            self._g.property[t.task_name]['virtual'] = t.virtual
            self._g.property[t.task_name]['name'] = t.task_name
            self._g.property[t.task_name]['ignore'] = \
                t.task_name not in running_tasks
            if t.expire >= 0:
                self._g.property[t.task_name]['expire'] = t.expire
        for t in self.tasks.values():
            for t2 in t.dependent_tasks:
                self._g.add_edge(t2, t.task_name)
        self._g.aggregate(
            lambda x: x.get('dirty', True),
            any,
            nodes=self.targets,
            update='dirty'
        )

        self._update_status()

    def to_pdf(self, path):
        self._g.to_pdf(path)

    def _update_status(self):
        def map_func(prop):
            if prop.get('virtual', False):
                return 'virtual'
            if 'status' not in prop:
                if prop['dirty']:
                    return 'pending'
                else:
                    return 'finished'
            else:
                return prop['status']

        def reduce_func(props: List[str]):
            ready = True
            # if props[-1] != 'pending':
            #     return props[-1]
            if 'virtual' in props:
                return 'virtual'

            for s in props[:-1]:
                if s != 'finished':
                    ready = False
            if ready and (props[-1] == 'pending'):
                return 'ready'
            if 'error' in props[:-1]:
                return 'error'
            if 'pending' in props[:-1]:
                return 'pending'
            if 'running' in props[:-1]:
                return 'pending'
            if 'ready' in props[:-1]:
                return 'pending'
            return props[-1]

        self._g.aggregate(
            map_func,
            reduce_func,
            nodes=[TaskSpecEndTask.task_name],
            update='status'
        )
        # self.to_pdf('_output/scheduler')
        logger.debug(pprint_str(self._g.property))

    def get_ready(self):
        t = self._g.match_one(dict(status='ready'))
        return t

    def set_status(self, task: str, status: Union[str, None]):
        assert isinstance(task, str)
        if status is None and 'status' in self._g.property[task]:
            del self._g.property[task]['status']
        if isinstance(status, str):
            self._g.property[task]['status'] = status
        self._update_status()

    def set_running(self, task):
        self.set_status(task, 'running')

    def set_finished(self, task):
        self.set_status(task, 'finished')

    def set_error(self, task):
        self.set_status(task, 'error')

    async def create_process(self, ready_task: str):
        logger.debug('running task ' + ready_task)
        env = dict(os.environ)
        task = self.tasks.get(ready_task, None)
        if task is None:
            return None
        env['PYTHONPATH'] = os.path.abspath(task.task_root)
        process = await asyncio.create_subprocess_exec(
            'python',
            os.path.split(__file__)[0] + '/worker.py',
            task.task_root,
            task.code_file,
            stdout=sys.stdout,
            stderr=sys.stderr,
            env=env,
            cwd=os.path.split(os.path.abspath(task.task_root))[0]
        )
        return process

    def _init_new_job(self):
        """
        create a coroutine and make it propagate
        """
        logger.debug('propagate tasks')
        self.task_queue.put(asyncio.create_task(self.try_available_job()))

    async def try_available_job(self):
        if self.new_task_lock.locked():
            logger.info('stopped')
            return
        async with self.new_task_lock, self.processes_lock:
            ready_job = self.get_ready()
            if ready_job == TaskSpecEndTask.task_name:
                # end of tasks
                logger.info('all finished')
                # asyncio.create_task(self.on_finished())
                return
            if ready_job in self.processes:
                logger.error(f'job already running {ready_job}')
                raise Exception()
            logger.debug(f'trying job {ready_job}')
            if ready_job is None:
                return
            logger.info(f'running {ready_job}')
            self.set_running(ready_job)
            p = await self.create_process(ready_job)
            # call other jobs if avail
            self.processes[ready_job] = (p, asyncio.current_task())
            self._init_new_job()
        if self.sio.connected:
            await self.sio.emit('graph_change')
        ret_code = await p.wait()
        async with self.processes_lock:
            if ret_code == 0:
                self.tasks[ready_job].end_time = time.time()
                self.set_finished(ready_job)
                self._init_new_job()
                logger.info(f'task finished {ready_job}')
            else:
                # del self.processes[ready_job]
                self.set_error(ready_job)
                logger.info(f'error task {ready_job} {ret_code}')
            del self.processes[ready_job]
            # check if all jobs finished
            # self.check_finish()
        if self.sio.connected:
            await self.sio.emit('graph_change')

    async def async_on_finished(self):
        async with self.processes_lock, self.new_task_lock:
            logger.info('finished all')
            expire_task_names = self._g.match_all(dict(expire=QUERY_EXIST))
            if len(expire_task_names) == 0:
                return
            expire_tasks = [self.tasks[i] for i in expire_task_names]
            expire_tasks = list(filter(lambda x: not x.virtual, expire_tasks))
            if len(expire_tasks) == 0:
                return
            now = time.time()
            expire_times = [t.expire - (now - t.end_time)
                            for t in expire_tasks]
            task = expire_tasks[expire_times.index(min(expire_times))]
            logger.info(f'counting down for task {task} {task.expire}s')
            while not task.dirty:
                await asyncio.sleep(
                    task.expire - (time.time() - task.end_time))
            logger.info('trigger expire task')
            self.set_status(task.task_name, 'ready')
            self.finished = False
            self._update_status()
        self._init_new_job()

    async def async_file_watcher_task(self):
        logger.info('file watcher started ...')
        logger.info(self.root)
        async for changes in awatch(
                self.root,
                recursive=True,
                step=500,
        ):
            logger.debug(changes)
            async with self.new_task_lock:
                tasks = parse_target(self.root)

                removed_keys = set(self.tasks.keys()) - set(tasks.keys())
                added_keys = set(tasks.keys()) - set(self.tasks.keys())
                changed_hash_keys = set()

                for key in set(tasks.keys()) & set(self.tasks.keys()):
                    if tasks[key].code_hash != self.tasks[key].code_hash:
                        changed_hash_keys.add(key)

                # terminate all tasks if needed
                async with self.processes_lock:
                    agg_res = self._g.aggregate(
                        lambda x: x['name'] in (
                            changed_hash_keys | removed_keys),
                        any
                    )
                    process_to_end = []
                    for task_name, changed in agg_res.items():
                        if changed:
                            process_to_end.append(task_name)
                logger.info(process_to_end)
                for task_name in process_to_end:
                    if task_name not in self.processes:
                        continue
                    p, cr = self.processes[task_name]
                    p.send_signal(signal.SIGINT)
                    # p.terminate()
                    await cr

                # update status, and other things here
                async with self.processes_lock:
                    # remove nodes
                    for r in (removed_keys | changed_hash_keys):
                        del self.tasks[r]
                        self._g.remove_node(r)
                    # add new Nodes
                    task_to_add = [tasks[i]
                                   for i in (changed_hash_keys | added_keys)]
                    self.add_tasks(task_to_add)
                    for p in process_to_end:
                        if (p in self.tasks
                                and 'status' in self._g.property.get(p, {})):
                            self.set_status(p, 'pending')
                    self._update_status()
            self._init_new_job()

    async def async_socket_on_get_tasks(self):
        elements = []
        color_map = dict(
            ready='yellow',
            finished='lightgreen',
            pending='orange',
            running='lightblue',
            error='red',
        )
        from networkx.drawing.nx_agraph import graphviz_layout
        logger.debug('on_get_task')
        g = nx.DiGraph()
        for task_name, task in self.tasks.items():
            if self._g.property[task_name]['ignore']:
                continue
            for dep in task.dependent_tasks:
                g.add_edge(dep, task_name)

        pos = graphviz_layout(g, prog='dot')

        for task_name, task in self.tasks.items():
            if self._g.property[task_name]['ignore']:
                continue
            position_xy = pos.get(task_name, (0, 0))
            prop = self._g.property.get(task_name, {})
            label = (task_name + '<br>' +
                     '<br>'.join(f'{a}:{b}' for a, b in prop.items()))
            bg_color = color_map.get(prop.get('status', None), None)
            # if task_name == '_end':
            #     bg_color = 'gray'
            elements.append(dict(
                id=task_name,
                label=label,
                position=dict(y=position_xy[0] * 0.7, x=-position_xy[1] * 3),
                style={'backgroundColor': bg_color, },
                sourcePosition='right',
                targetPosition='left',
            ))
            for dep_task in task.dependent_tasks:
                elements.append(dict(
                    id=f'{dep_task}-{task_name}',
                    source=dep_task,
                    target=task_name,
                    markerEnd='arrowclosed',
                ))
            # payload = task.to_json()
            # payload['prop'] = self._g.property[task_name]
            # payload['pos'] = pos[task_name]
            # result[task_name] = payload
        return {e['id']: e for e in elements}

    async def async_socket_on_connect(self):
        logger.info('connect')
        result = await self.sio.call('join', data=dict(room=self.root))
        logger.info(f'connected, {result}')

    async def async_socket_task(self):
        logger.info('socket task started ...')
        # connect to sockerIO server for event dispatch
        if self.sock_addr is not None:
            try:
                # self.sio = socketio.AsyncClient()
                self.sio.on('connect', self.async_socket_on_connect)
                await self.sio.connect(self.sock_addr, wait_timeout=3)

                self.sio.on('get_task', self.async_socket_on_get_tasks)
            except ConnectionError:
                # self.sio = None
                logger.info('connection error')

    async def async_serve_main(self, exit=False):
        # file watcher task
        self.processes_lock = asyncio.Lock()
        self.new_task_lock = asyncio.Lock()
        if not exit:
            asyncio.create_task(self.async_file_watcher_task())
        asyncio.create_task(self.async_socket_task())
        # self.check_finish()
        # start running
        self._init_new_job()
        while True:
            new_task_exist = not self.task_queue.empty()
            while not self.task_queue.empty():
                t = self.task_queue.get()
                await asyncio.wait_for(t, None)
            # logger.info('end')
            # call end task
            if exit:
                break
            if new_task_exist:
                asyncio.create_task(self.async_on_finished())
            await asyncio.sleep(10)

    def serve(self, exit=False):
        try:
            asyncio.run(self.async_serve_main(exit=exit))
        except KeyboardInterrupt:
            logger.info("END")
            raise Exception()


def extract_anno(root, file) -> TaskSpec:
    payload = bytearray()
    with open(file, 'rb') as f:
        f.readline()
        while True:
            line = f.readline()
            if line == b'"""\n':
                break
            payload.extend(line)
    task_name = os.path.split(file)[0]
    assert task_name.startswith(root)
    task_name = task_name[len(root):]
    if not task_name.startswith('/'):
        task_name = '/' + task_name
    try:
        task_info = yaml.safe_load(BytesIO(payload))
    except yaml.scanner.ScannerError as e:
        logger.info(f"ERROR parse {root} {file}")
        raise e
    if task_info is None:
        task_info = {}
    task_info['task_root'] = root
    task_info['task_name'] = task_name
    tki = TaskSpec(**task_info)
    return tki


def parse_target(target: str) -> Dict[str, TaskSpec]:
    tasks: Dict[str, TaskSpec] = {}
    for dir_path, _, file_names in os.walk(target):
        for file in file_names:
            if file == 'task.py':
                file_path = os.path.join(dir_path, file)
                try:
                    task_info = extract_anno(target, file_path)
                    tasks[task_info.task_name] = task_info
                except Exception as e:
                    logger.error(
                        f'Error parsing node {target}, {file_path}, {e}',
                        exc_info=e, stack_info=True)
                    logger.error(str(os.stat(file_path)))
                    logger.error(os.path.abspath('.'))
                    # TODO send message about this error node
                    raise e
    return tasks


def serve_target(
        target: str,
        task: Union[str, List[str]] = None,
        addr=None,
        exit=False,
):
    if task is not None and isinstance(task, str):
        task = [task]
    logger.info(f'serve on: {target}, tasks: {task} ...')
    scheduler = Scheduler(target, task, addr)
    scheduler.serve(exit)


def clean_target(target: str):
    logger.info(f'cleaning : {target}')
    tasks = parse_target(target)
    for _, v in tasks.items():
        v.clean()


def new_task(target: str, task: str):
    logger.info(f'creating new task : {target} {task}')
    if not os.path.exists(target):
        logger.info("ERROR no such root")
        return
    if task.startswith('/'):
        task = task[1:]
    full_path = os.path.join(target, task)
    if not os.path.exists(full_path):
        os.makedirs(full_path)
    file_path = os.path.join(full_path, 'task.py')
    if not os.path.exists(file_path):
        with open(file_path, 'w') as f:
            f.write(
                '''"""
require: {}

"""

def run():
    pass

'''
            )
