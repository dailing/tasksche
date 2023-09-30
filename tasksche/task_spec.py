import contextlib
import importlib
import io
import os
import os.path
import shutil
import sys
import time
from functools import cached_property
from hashlib import md5
from io import BytesIO
from typing import Any, Dict, List, Tuple, Union, Optional

import yaml
from typing_extensions import Self

from .common import __INVALIDATE__, Status, ExecInfo, DumpedType, CachedPropertyWithInvalidator
from .logger import Logger

_INVALIDATE = __INVALIDATE__()
logger = Logger()


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
        self.stdout_file = open(os.path.join(self.cwd, 'stdout.txt'), 'w')
        self.redirect_stdout = contextlib.redirect_stdout(self.stdout_file)
        self.redirect_stdout.__enter__()

    def __exit__(self, exc_type, exc_value, traceback):
        assert self.redirect_stdout is not None
        assert self.stdout_file is not None
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
            task_dict: Optional[Dict[str, Self]] = None
    ) -> None:
        self.root = root
        self.task_name = task_name
        self.task_dict = task_dict

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
        assert self.root is not None
        return self._get_dump_file(self.root, self.task_name)

    @CachedPropertyWithInvalidator
    def status(self):
        def _f():
            assert self.task_dict is not None
            parent_status = [
                self.task_dict[t].status for t in self.depend_task]
            if all(status == Status.STATUS_FINISHED
                   for status in parent_status):
                if self.dirty:
                    return Status.STATUS_READY
                else:
                    return Status.STATUS_FINISHED
            elif Status.STATUS_ERROR in parent_status:
                return Status.STATUS_ERROR
            elif Status.STATUS_PENDING in parent_status:
                return Status.STATUS_PENDING
            elif Status.STATUS_RUNNING in parent_status:
                return Status.STATUS_PENDING
            elif Status.STATUS_READY in parent_status:
                return Status.STATUS_PENDING
            else:
                logger.error(f'{self}, {parent_status}')
                raise Exception("ERR")

        st = _f()
        logger.debug(f'get status {self}, {st}')
        return st

    @CachedPropertyWithInvalidator
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
        out_dump = os.path.join(self.output_dump_folder, 'exec_result.pkl')
        if os.path.exists(out_dump):
            with open(out_dump, 'rb') as f:
                md5_hash.update(f.read())
        return md5_hash.hexdigest()

    @CachedPropertyWithInvalidator
    def dependent_hash(self) -> Dict[str, str]:
        """
        Calculates the hash values of all dependent tasks and returns a
        dictionary mapping task names to their respective hash values.

        Returns:
            dict: A dictionary mapping task names (str) to their corresponding
            hash values (str).
        """
        assert self.task_dict is not None
        depend_hash = {
            task_name: self.task_dict[task_name]._hash
            for task_name in self._all_dependent_tasks
        }
        # Add me to dependent_hash
        depend_hash[self.task_name] = self._hash
        return depend_hash

    @CachedPropertyWithInvalidator
    def dirty(self):
        parent_dirty = [self.task_dict[t].dirty for t in self.depend_task]
        if any(parent_dirty):
            return True
        exec_info: ExecInfo = self._exec_info_dump
        if (exec_info is _INVALIDATE
                or self._exec_result is _INVALIDATE):
            return True
        if exec_info.depend_hash != self.dependent_hash:
            logger.debug(
                f'{self}, {exec_info.depend_hash}, {self.dependent_hash}')
            return True
        return False

    @dirty.register_broadcaster
    @status.register_broadcaster
    @_hash.register_broadcaster
    @dependent_hash.register_broadcaster
    def _depend_by_task_specs(self) -> List[Self]:
        return [self.task_dict[k] for k in self.depend_by]

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
        lines = lines + f"<Task:{self.task_name}>"
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
            mod = importlib.import_module(self.task_module_path)
            mod.__dict__['work_dir'] = self.output_dump_folder
            if not hasattr(mod, 'run'):
                raise NotImplementedError()
            args, kwargs = self._load_input()
            output = mod.run(*args, **kwargs)
            self._exec_result = output
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
            self.dirty = False
            self._hash = _INVALIDATE
            self.dependent_hash = _INVALIDATE
            exec_info = self._exec_info
        self._exec_info_dump = exec_info

    def clear_output_dump(self, rm_tree=False):
        self._exec_info_dump = _INVALIDATE
        self._exec_result = _INVALIDATE
        if rm_tree:
            shutil.rmtree(self.output_dump_folder, ignore_errors=True)
        self._hash = _INVALIDATE

    def clear(self, rm_tree=False):
        logger.debug(f'clearing {self.task_name}')
        # if self.status != Status.STATUS_FINISHED:
        #     return
        self.clear_output_dump(rm_tree)
        self.dirty = _INVALIDATE
        self.status = _INVALIDATE


class EndTask(TaskSpec):
    def __init__(
            self,
            task_dict: Dict[str, TaskSpec],
            depend_task: List[str]
    ):
        super().__init__('/tmp/none_exist', "_END_", task_dict)
        self._depend_task = depend_task

    @property
    def dirty(self):
        return True

    @dirty.setter
    def dirty(self, value):
        pass

    @property
    def depend_task(self) -> List[str]:
        return self._depend_task

    @property
    def depend_by(self) -> List[str]:
        return []

    def clear(self, rm_tree=False):
        pass

    def dependent_hash(self):
        return None

    @cached_property
    def task_module_path(self):
        return '_END_'
