import asyncio
import os
import os.path
import signal
import sys
from abc import ABC, abstractmethod
from asyncio import Queue
from asyncio.subprocess import Process
from dataclasses import dataclass
from enum import Enum, auto
from functools import cached_property
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

from watchfiles import awatch

from .callback import CALLBACK_TYPE, CallbackRunner
from .cbs import ProgressCB
from .common import __INVALIDATE__, Status
from .logger import Logger
from .task_spec import EndTask, TaskSpec

_INVALIDATE = __INVALIDATE__()
logger = Logger()


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


def path_to_task_spec(
        tasks: List[Union[str, Path]], root=None) -> Dict[str, TaskSpec]:
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
        task_dict: Optional[Dict[str, TaskSpec]] = None
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
            logger.debug(f'adding task {depend_task_name}')
            task_dict[depend_task_name] = depend_task_spec
            _dfs_build_exe_graph([depend_task_spec], task_dict)


def build_exe_graph(tasks: List[Union[str, Path]]) \
        -> Tuple[List[str], Dict[str, TaskSpec]]:
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
    task_root: Optional[str]

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

    def __init__(self, event_queue: Queue) -> None:
        self.event_queue = event_queue
        self.processes: Dict[str, Process] = {}

    async def __aenter__(self):
        pass

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        logger.info('exiting ...')
        for p in list(self.processes.values()):
            # sent int signal
            try:
                p.send_signal(signal.SIGINT)
                await p.wait()
            except ProcessLookupError:
                logger.debug(f'process {p.pid} already exited')
                pass
        self.processes.clear()

    async def _run_task(self, task_root: str, task_name: str):
        logger.debug(f'running task {task_name} ...')
        process = await asyncio.create_subprocess_exec(
            'python',
            '-m',
            'tasksche',
            'exec_task',
            task_root,
            task_name,
        )
        self.processes[task_name] = process
        exit_code = await process.wait()
        del self.processes[task_name]
        logger.debug(f'exiting task {task_name} ...')
        if exit_code == 0:
            await self.event_queue.put(TaskEvent(
                TaskEventType.TASK_FINISHED, task_name, task_root))
            logger.debug(f'finished task {task_name}')
        elif exit_code == 3:
            logger.debug(f'killing task {task_name}')
            await self.event_queue.put(TaskEvent(
                TaskEventType.TASK_INTERRUPT, task_name, task_root
            ))
        elif exit_code == 2:
            await self.event_queue.put(TaskEvent(
                TaskEventType.TASK_ERROR, task_name, task_root))
            logger.info(f'Error task {task_name}')
        else:
            logger.error(f'UNKNOWN EXIT CODE {exit_code}')

    def add_task(self, task_root: str, task_name: str) -> None:
        asyncio.create_task(self._run_task(task_root, task_name))

    def get_running_tasks(self) -> List[str]:
        return list(self.processes.keys())

    async def stop_tasks_and_wait(self, tasks: List[str]):
        for task in tasks:
            if task not in self.processes:
                continue
            logger.info(f'killing task {task}')
            try:
                self.processes[task].send_signal(signal.SIGINT)
            except Exception as e:
                logger.error(e, stack_info=True)
            await self.processes[task].wait()
            self.processes.pop(task)


class FileWatcher:
    def __init__(
            self,
            root: str,
            queue: Queue):
        self.root = root
        self.stop_event = asyncio.Event()
        self.event_queue = queue
        self.async_task: Optional[asyncio.Task] = None

    async def _func(self):
        logger.debug(f'watching {self.root} ...')
        async for event in awatch(self.root, stop_event=self.stop_event):
            files = set([x[1] for x in event])
            for f in files:
                await self.event_queue.put(TaskEvent(
                    TaskEventType.FILE_CHANGE, f, 'None'))

    async def __aenter__(self):
        return await self.start()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()
        self.async_task = None

    async def start(self):
        if self.async_task is not None:
            logger.warning('already started')
            await self.stop()
        self.stop_event.clear()
        self.async_task = asyncio.create_task(self._func())
        return self

    async def stop(self):
        self.stop_event.set()
        if self.async_task is not None:
            logger.debug('stopping ...')
            await self.async_task


class SchedulerEventType(Enum):
    STATUS_CHANGE = auto()


@dataclass
class SchedulerEvent:
    event: SchedulerEventType
    msg: Any


class TaskScheduler:

    def __init__(
            self,
            target: List[Union[str, Path]],
            call_backs: Optional[List[CALLBACK_TYPE]] = None,
    ) -> None:
        self.target, self.task_dict = build_exe_graph(target)
        self.main_loop_task: Optional[asyncio.Task] = None
        if call_backs is None:
            call_backs = []
        self._cbs = CallbackRunner(call_backs)
        self.runner = PRunner(self._task_event_queue)

    @cached_property
    def _task_event_queue(self):
        return Queue()

    @cached_property
    def sche_event_queue(self):
        return Queue()

    def _get_ready(self) -> Union[str, None]:
        """
        Get a task that is ready to be executed.

        :return: Union of string or None.
        """
        for task in self.task_dict.values():
            logger.debug(f'checking task {task.task_name} {task.status}')
            if task.status == Status.STATUS_READY:
                return task.task_name
        return None

    async def get_ready_set_running(self):
        ready_task = self._get_ready()
        if ready_task:
            await self.set_running(ready_task)
        return ready_task

    async def set_status(self, task: str, status: Status):
        logger.debug(f'setting status of {task} to {status}')
        assert isinstance(task, str), task
        assert task in self.task_dict, f'{task} is not a valid task'
        self.task_dict[task].status = status
        await self.sche_event_queue.put(SchedulerEvent(
            SchedulerEventType.STATUS_CHANGE,
            (task, status)))

    async def set_finished(self, task: str):
        await self.set_status(task, Status.STATUS_FINISHED)

    async def set_error(self, task: str):
        await self.set_status(task, Status.STATUS_ERROR)

    async def clear_status(self, task: str):
        self.task_dict[task].status = _INVALIDATE

    async def set_running(self, task: str):
        await self.set_status(task, Status.STATUS_RUNNING)

    async def clear(
            self,
            tasks: Union[Iterable[str], str, None] = None,
            deep: bool = False,
            _event: bool = True,
    ) -> None:
        """
        Clear specific tasks or all tasks in the task dictionary.

        Args:
            tasks (Union[Iterable[str], str, None], optional): The tasks to be
                cleared. Defaults to None.
            deep (bool, optional): Whether to perform a deep clear, which
                clears all subtasks of the specified tasks. Defaults to False.
            _event (bool, optional): Whether to emit the event. If True,
                the event will be emitted. If False, no event will be emitted.

        Returns:
            None
        """
        if tasks is None:
            tasks = self.task_dict.keys()
        elif isinstance(tasks, str):
            tasks = [tasks]
        sub_tasks = set()
        for task in tasks:
            logger.info(f'clearing {task}')
            self.task_dict[task].clear()
            if deep:
                sub_tasks.update(self.task_dict[task].all_task_depend_me)
            if _event:
                await self._task_event_queue.put(
                    TaskEvent(TaskEventType.TASK_INTERRUPT, task, self.root))
        if deep:
            sub_tasks = sub_tasks - set(tasks)
            # clear all sub-tasks, without emitting event
            await self.clear(list(sub_tasks), deep=False, _event=False)

    @cached_property
    def root(self):
        return self.task_dict[self.target[0]].root

    @cached_property
    def file_watcher(self):
        assert self.root is not None
        return FileWatcher(self.root, self._task_event_queue)

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
                elif new_spec.dirty:
                    changed_tasks.append(task_name)
                    self.task_dict[task_name].dirty = None
        for task_name in changed_tasks:
            self.task_dict[task_name].status = _INVALIDATE
        all_influenced_tasks = set()
        for task in changed_tasks:
            all_influenced_tasks.add(task)
            all_influenced_tasks.update(
                set(self.task_dict[task].all_task_depend_me))
        logger.info(f'all_influenced_tasks: {all_influenced_tasks}')
        await self.runner.stop_tasks_and_wait(list(all_influenced_tasks))
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

        async def loop() -> bool:
            """
            the main event loop body
            return True for stoping the event loop
            """
            ready_list = []
            while True:
                ready_task = await self.get_ready_set_running()
                if ready_task is None:
                    break
                ready_list.append(ready_task)
            if len(ready_list) == 1 and ready_list[0] == '_END_':
                logger.info('all tasks are finished')
                await self.set_finished(ready_list[0])
                if once:
                    logger.info("in single step mode, exiting...")
                    return True
                else:
                    return False
            for ready_task in ready_list:
                task_spec = self.task_dict[ready_task]
                assert task_spec.root is not None
                self.runner.add_task(task_spec.root, task_spec.task_name)
                self._cbs.task_start(task=task_spec)
            # set all task to running, waiting for event now:
            try:
                event = await self._task_event_queue.get()
            except ValueError:
                logger.info('stop running')
                await self.runner.stop_tasks_and_wait(
                    self.runner.get_running_tasks())
                return True
            except Exception as e:
                logger.error(e, stack_info=True)
                logger.error(f'{type(e)}')
                return True
            logger.debug(f'got event:{event}')
            if event is None:
                logger.info('stop running')
                return True
            elif event.event_type == TaskEventType.TASK_FINISHED:
                await self.set_finished(event.task_name)
                self.task_dict[event.task_name].dump_exec_info()
                self._cbs.task_finish(task=self.task_dict[event.task_name])
            elif event.event_type == TaskEventType.TASK_ERROR:
                await self.set_error(event.task_name)
                logger.info(f'task:{event.task_name} is errored')
                self._cbs.task_error(task=self.task_dict[event.task_name])
            elif event.event_type == TaskEventType.FILE_CHANGE:
                logger.info(f'file changed:{event.task_name}')
                await self._reload()
            else:
                raise NotImplementedError()
            return False

        async with self.runner, self.file_watcher:
            while not await loop():
                pass
        # clearing running tasks
        for task in self.task_dict.values():
            if task.status == Status.STATUS_RUNNING:
                task.status = _INVALIDATE
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

    def run(self, once=False, daemon=False, timeout=None):
        """
        add _run to loop if daemon,
        else run _run and await
        # TODO: add task level timeout
        """
        if self.main_loop_task is not None and not self.main_loop_task.done():
            logger.error('already running !!')
            return
        if daemon:
            self.main_loop_task = asyncio.create_task(self._run(once))
        else:
            # asyncio.run()
            asyncio.run(self._run(once))


default_cbs = [
    ProgressCB(),
]


def serve_target(tasks: List[Union[str, Path]]):
    logger.info(f'serve2 {tasks}')
    scheduler = TaskScheduler(tasks, call_backs=default_cbs)
    # task_dict_to_pdf(scheduler.task_dict)
    # logger.debug(scheduler.task_dict)
    scheduler.run(once=False, daemon=False)


def run_target(tasks: List[Union[str, Path]]):
    logger.info(f'exec {tasks}')
    scheduler = TaskScheduler(tasks, call_backs=default_cbs)
    # task_dict_to_pdf(scheduler.task_dict)
    # logger.debug(scheduler.task_dict)
    scheduler.run(once=True, daemon=False)


def _exec_task(root: str, task: str):
    try:
        task_spec = TaskSpec(root, task)
        task_spec.execute()
    except KeyboardInterrupt:
        logger.info(f'Cancelling task {task}')
        sys.exit(3)
    except Exception as e:
        logger.error(e, stack_info=True)
        sys.exit(2)
