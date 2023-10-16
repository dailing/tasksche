import abc
import contextlib
import importlib
import io
import os
import sys
import types
import uuid
from functools import partial
from typing import Any, Dict, List, Optional, Callable

from .callback import CALLBACK_TYPE, CallbackRunner, CallbackBase, CallBackEvent
from .cbs.FinishChecker import FinishChecker
from .common import Status
from .functional import ARG_TYPE, Graph, RunnerTaskSpec, RunnerArgSpec, RequirementArg, search_for_root
from .logger import Logger
from .storage.storage import ResultStorage, StatusStorage, storage_factory, KVStorageBase

logger = Logger()


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


def load_input(spec: RunnerTaskSpec, storage: Optional[KVStorageBase] = None):
    def get_input(arg: RunnerArgSpec):
        if arg.arg_type == ARG_TYPE.RAW:
            return arg.value
        elif arg.arg_type == ARG_TYPE.TASK_OUTPUT:
            return storage.get(arg.storage_path)
        else:
            raise ValueError(f'unknown arg type {arg.arg_type}')

    args = [get_input(node_arg) for node_arg in spec.args]
    kwargs = {k: get_input(v) for k, v in spec.kwargs.items()}
    return args, kwargs


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


class RunnerBase(abc.ABC):
    @abc.abstractmethod
    def add_task(self, spec: RunnerTaskSpec, cb: Callable):
        """
        Add a task to the runner.

        Args:
            spec (RunnerTaskSpec): The specification of the task.
            cb (Callable): The callback function to be executed when the task is completed.

        Raises:
            NotImplementedError: This method needs to be implemented by a subclass.
        """
        raise NotImplementedError


class LocalRunner(RunnerBase):

    def add_task(self, spec: RunnerTaskSpec, cb: Callable):
        execute_task(spec)
        cb()


class Scheduler(CallbackBase):
    def __init__(
            self,
            graph: Graph,
            result_storage: ResultStorage,
            status_storage: StatusStorage,
            cbs: List[CALLBACK_TYPE]) -> None:
        self.graph = graph
        self.cb = CallbackRunner(cbs + [self])
        self.runner = LocalRunner()
        self.task_status = {}
        self.result_storage = result_storage
        self.status_storage = status_storage

    def feed(self, run_id=None, **kwargs):
        if run_id is None:
            run_id = uuid.uuid4().hex
        event = CallBackEvent(
            graph=self.graph, run_id=run_id,
            result_storage=self.result_storage,
            status_storage=self.status_storage)
        self.cb.on_feed(event, kwargs)

    def on_feed(
            self,
            event: CallBackEvent,
            kwargs: Dict[str, Any]):
        for n in event.graph.node_map.keys():
            if n in event.graph.source_nodes:
                self.status_storage.store(n, event.run_id, value=Status.STATUS_READY)
            else:
                self.status_storage.store(n, event.run_id, value=Status.STATUS_PENDING)
        for n in event.graph.source_nodes:
            self.cb.on_task_ready(event.new_inst(task_name=n))

    def on_task_ready(self, event: CallBackEvent):
        task_name, run_id, graph = event.task_name, event.run_id, event.graph
        if task_name == '_END_':
            self.cb.on_task_finish(event)
            return

        def arg_transfer(arg: RequirementArg) -> RunnerArgSpec:
            if arg.arg_type == ARG_TYPE.RAW:
                return RunnerArgSpec.parse_obj(arg.dict())
            elif arg.arg_type == ARG_TYPE.TASK_OUTPUT:
                return RunnerArgSpec(
                    arg_type=ARG_TYPE.TASK_OUTPUT,
                    storage_path=ResultStorage.key_for(arg.from_task, run_id),
                )
            else:
                raise TypeError(f'unknown arg type {arg.arg_type}')

        self.status_storage.store(task_name, run_id, value=Status.STATUS_RUNNING)
        self.runner.add_task(
            RunnerTaskSpec(
                task=task_name,
                root=graph.root,
                args=[arg_transfer(arg) for arg in graph.node_map[task_name].args],
                kwargs={
                    k: arg_transfer(arg)
                    for k, arg in graph.node_map[task_name].kwargs.items()
                },
                storage_path=self.result_storage.storage_path,
                output_path=ResultStorage.key_for(task_name, run_id),
                work_dir=os.path.join('/tmp/storage/', run_id, task_name.replace('/', '_')),
            ),
            partial(self.cb.on_task_finish, event),
        )

    def on_task_finish(self, event: CallBackEvent):
        task_name, run_id, graph = event.task_name, event.run_id, event.graph
        self.status_storage.store(task_name, run_id, value=Status.STATUS_FINISHED)
        for k, v in self.graph.node_map.items():
            if task_name not in v.depend_on:
                continue
            ready = True
            for n in v.depend_on:
                if self.status_storage.get(n, run_id) != Status.STATUS_FINISHED:
                    ready = False
                    break
            if ready:
                self.cb.on_task_ready(event.new_inst(task_name=k))


def run(
        task: str,
        run_id: Optional[str] = None,
        storage_result: str = 'file:default',
        storage_status: str = 'mem:default',
) -> None:
    task = os.path.abspath(task)
    assert task.endswith('.py')
    root = search_for_root(task)
    task_name = task[len(root):-3]
    scheduler = Scheduler(
        Graph(root, [task_name]),
        ResultStorage(storage_result),
        StatusStorage(storage_status),
        [
            FinishChecker(storage_result)
        ]
    )
    scheduler.feed(run_id=str(run_id))


if __name__ == '__main__':
    # sche = Scheduler(
    #     Graph('test/simple_task_set', ['/task']),
    #     ResultStorage('file:default'),
    #     StatusStorage('mem:default'), [
    #         FinishChecker()
    #     ])
    # from pprint import pprint
    # pprint(sche.graph.requirements_map)
    # sche.feed()
    run('test/simple_task_set/task.py', run_id='test')
    # import fire
    #
    # fire.Fire(run)
