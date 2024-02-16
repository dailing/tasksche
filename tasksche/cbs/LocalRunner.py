import asyncio
import concurrent.futures
import importlib
import multiprocessing
import typing
from collections import defaultdict
from functools import cached_property
from itertools import chain
from typing import Dict, Optional, Any

from ..callback import (
    CallbackBase,
    CallBackEvent,
    InvokeSignal,
)
from ..functional import (
    ARG_TYPE,
    EVENT_TYPE,
    ExecEnv,
    IteratorArg,
    RunnerTaskSpec,
)
from ..logger import Logger
from ..new_sch import TaskSpec
from ..storage.storage import storage_factory

logger = Logger()


class RunnerHandle:
    def __init__(
        self,
    ) -> None:
        super().__init__()
        self.spec: Optional[RunnerTaskSpec] = None
        self.process: Optional[multiprocessing.Process] = None
        self.output_queue: multiprocessing.Queue = multiprocessing.Queue()
        self.exception = None

    @cached_property
    def storage(self):
        return storage_factory(self.spec.storage_path)

    @cached_property
    def node(self) -> TaskSpec:
        return TaskSpec(self.spec.task, self.spec.root)

    @cached_property
    def iter_args(self) -> Dict[int | str, IteratorArg]:
        return {
            k: IteratorArg()
            for k, v in self.node.requires.items()
            if v.arg_type == ARG_TYPE.TASK_ITER
        }

    @cached_property
    def value_args(self):
        return {
            k: v.value
            for k, v in self.node.requires.items()
            if v.arg_type == ARG_TYPE.RAW
        }

    def load_args_from_storage(
        self, args: Dict[int | str, str]
    ) -> Dict[int | str, Any]:
        retval = {}
        for k, v in args.items():
            retval[k] = self.storage.get(v)
            if isinstance(v, StopIteration):
                self.exception = v
                raise v
        return {k: self.storage.get(v) for k, v in args.items()}

    @cached_property
    def stored_args(self):
        return self.load_args_from_storage(self.spec.requires)

    @cached_property
    def all_requires_loaded(self) -> Dict[int | str, Any]:
        ret_val = {}
        for k, v in chain(
            self.iter_args.items(),
            self.value_args.items(),
            self.stored_args.items(),
        ):
            ret_val[k] = v
        return ret_val

    @cached_property
    def args(self) -> typing.List[Any]:
        args = []
        for k, v in self.all_requires_loaded.items():
            if isinstance(k, int):
                if k >= len(args):
                    args.extend([None] * (k - len(args) + 1))
                args[k] = v
        return args

    @cached_property
    def kwargs(self) -> Dict[str, Any]:
        kwargs = {}
        for k, v in self.all_requires_loaded.items():
            if isinstance(k, str):
                kwargs[k] = v
        return kwargs

    def call(self, spec: RunnerTaskSpec):
        assert spec.task_type == EVENT_TYPE.CALL
        if self.spec is None:
            self.spec = spec
        else:
            assert len(spec.requires) == 1
            for k, v in self.load_args_from_storage(spec.requires).items():
                if isinstance(v, StopIteration):
                    self.exception = v
                    self.output_queue.put(v)
                    return
                if isinstance(k, int):
                    self.args[k] = v
                else:
                    self.kwargs[k] = v
        for v in chain(self.args, self.kwargs.values()):
            if isinstance(v, StopIteration):
                self.exception = v
                self.output_queue.put(v)
                return
            elif isinstance(v, Exception):
                self.output_queue.put(v)
                return
        self.process = multiprocessing.Process(
            target=self.worker,
            args=(self.spec, self.output_queue, self.args, self.kwargs),
        )
        self.process.start()

    def term(self):
        if self.process is not None:
            if self.process.is_alive():
                self.process.terminate()
                self.process.join()
            self.process = None

    def pop(self, spec: RunnerTaskSpec, join=True):
        logger.debug(f"pop {spec.task}")
        assert spec.task_type in (EVENT_TYPE.POP, EVENT_TYPE.POP_AND_NEXT)
        # assert self.process is not None
        output = self.output_queue.get()
        self.storage.set(spec.output_path, output)
        logger.debug(f"pop {spec.task} done")
        if isinstance(output, Exception):
            return output
        if join and self.process is not None:
            self.process.join()
            self.process = None

    def push(self, spec: RunnerTaskSpec):
        logger.debug(f"push {spec.task}")
        if self.exception is not None:
            return self.exception
        assert spec.task_type == EVENT_TYPE.PUSH
        # assert self.process is not None
        assert len(spec.requires) == 1
        for k, v in self.load_args_from_storage(spec.requires).items():
            if isinstance(k, int):
                iter_obj = self.args[k]
            else:
                iter_obj = self.kwargs[k]
            assert isinstance(iter_obj, IteratorArg)
            iter_obj.put_payload(v)

    @classmethod
    def worker(
        cls,
        spec: RunnerTaskSpec,
        output_queue: multiprocessing.Queue,
        args,
        kwargs,
    ):
        logger.debug(f"worker start: {spec.task}")
        with ExecEnv(spec.root, spec.work_dir):
            mod = importlib.import_module(spec.task.replace("/", ".")[1:])
            if not hasattr(mod, "run"):
                output_queue.put(
                    NotImplementedError(
                        f"task {spec.task} must have 'run' function"
                    )
                )
                return
            try:
                output = mod.run(*args, **kwargs)
                if isinstance(output, typing.Generator):
                    while True:
                        try:
                            output_queue.put(next(output))
                            logger.debug(f"put queue: {output}")
                        except StopIteration as e:
                            output_queue.put(e)
                            logger.debug(f"stop iteration: {output}")
                            break
                else:
                    logger.debug(f"output: {output}")
                    output_queue.put(output)
                    logger.debug(f"put queue: {spec.task} {output}")
            except Exception as e:
                logger.error(f"ERROR {e}", exc_info=e)
                output_queue.put(e)
        logger.debug(f"worker end: {spec.task}")


class LocalRunner(CallbackBase):
    def __init__(self) -> None:
        super().__init__()
        self.processes: Dict[str, RunnerHandle] = defaultdict(RunnerHandle)
        self.thread_pool = concurrent.futures.ThreadPoolExecutor()

    async def on_task_start(self, event: CallBackEvent):
        assert event.task_spec is not None

        spec = event.task_spec
        if event.task_spec.task_type == EVENT_TYPE.CALL:
            handler = self.processes[event.task_spec.process_id]
            handler.call(spec)
            return InvokeSignal("on_task_finish", event)
        elif event.task_spec.task_type in (
            EVENT_TYPE.POP,
            EVENT_TYPE.POP_AND_NEXT,
        ):
            assert event.task_spec.process_id in self.processes
            handler = self.processes[event.task_spec.process_id]
            exception = await asyncio.get_event_loop().run_in_executor(
                self.thread_pool,
                handler.pop,
                spec,
                event.task_spec.task_type == EVENT_TYPE.POP,
            )
            if isinstance(exception, StopIteration):
                return InvokeSignal("on_iter_stop", event)
            elif exception is not None:
                p = self.processes[event.task_spec.process_id]
                p.term()
                return InvokeSignal("on_task_error", event)
            return InvokeSignal("on_task_finish", event)
        elif event.task_spec.task_type == EVENT_TYPE.PUSH:
            assert event.task_spec.process_id in self.processes
            handler = self.processes[event.task_spec.process_id]
            handler.push(spec)
            return InvokeSignal("on_task_finish", event)
        else:
            raise NotImplementedError

    def on_run_finish(self, event: CallBackEvent):
        for k, v in self.executor.items():
            v.task_queue.put(None)
        for k, v in self.executor.items():
            if v.process is not None:
                v.process.join()
