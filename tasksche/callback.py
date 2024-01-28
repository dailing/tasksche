import asyncio
from collections import defaultdict
from functools import cached_property
from typing import (
    AsyncGenerator,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    TypeVar,
)

from pydantic import BaseModel

from .functional import RunnerTaskSpec
from .logger import Logger

logger = Logger()


class CallBackEvent(BaseModel):
    # graph: Graph
    # run_id: str
    task_id: str
    task_name: str
    task_spec: Optional[RunnerTaskSpec]

    class Config:
        arbitrary_types_allowed = True

    def new_inst(self, **kwargs):
        assert "previous_event" not in kwargs
        return self.model_copy(update=kwargs | dict(previous_event=self))

    # @property
    # def is_generator(self) -> bool:
    #     assert self.task_name is not None
    #     return self.graph.node_map[self.task_name].is_generator


class CallBackSignal:
    def __init__(self, signal: str, event: CallBackEvent):
        super().__init__()
        self.signal = signal
        self.event = event


class InterruptSignal(CallBackSignal, Exception):
    """
    Cancel all following callbacks in the task stack and invoke the target
    callback
    """

    pass


class InvokeSignal(CallBackSignal):
    """
    Invoke target callback after the current task stack
    """

    pass


class CallbackBase:
    """
    Abstract Base Class for callback system.
    """

    def on_task_start(self, event: CallBackEvent):
        """
        Called when a task is started. Mainly used by RUNNER to initiate the
        task.
        """
        raise NotImplementedError

    def on_task_run(self, event: CallBackEvent):
        """
        Called when a task is started. Mainly used by RUNNER to initiate the
        task.
        """
        raise NotImplementedError

    def on_gen_finish(self, event: CallBackEvent):
        """
        Called when a task is finished.
        """
        raise NotImplementedError

    def on_task_error(self, event: CallBackEvent):
        """
        Called when a task is interrupted. should kill all running tasks and
        return
        """
        raise NotImplementedError

    def on_task_finish(self, event: CallBackEvent):
        """
        Called when a task is finished.
        """
        raise NotImplementedError

    def on_run_finish(self, event: CallBackEvent):
        """
        Called when a run is finished, all tasks in the run_id are finished.
        """
        raise NotImplementedError

    def on_interrupt(self, event: CallBackEvent):
        """
        Called when a task is interrupted. should kill all running tasks and
        return
        """
        raise NotImplementedError

    def on_iter_stop(self, event: CallBackEvent):
        """
        Called when a task is interrupted. should kill all running tasks and
        return
        """
        raise NotImplementedError


CALLBACK_TYPE = TypeVar("CALLBACK_TYPE", bound=CallbackBase)
CALL_BACK_DICT: Dict[str, Callable] = {
    k: v for k, v in CallbackBase.__dict__.items() if not k.startswith("_")
}


async def _handle_call_back_output(
    retval: Generator | InvokeSignal | AsyncGenerator,
) -> List[InvokeSignal]:
    if retval is None:
        return []
    elif isinstance(retval, InvokeSignal):
        return [retval]
    elif isinstance(retval, Generator):
        val = []
        for ret in retval:
            _val = await _handle_call_back_output(ret)
            val.extend(_val)
        return val
    elif isinstance(retval, AsyncGenerator):
        val = []
        async for ret in retval:
            _val = await _handle_call_back_output(ret)
            val.extend(_val)
        return val
    # elif isinstance(retval, CallBackEvent):
    #     return [], retval
    logger.error(f"unknown type {type(retval)}")
    raise NotImplementedError()


class _CallbackRunnerMeta(type):
    """
    Meta Class for CallbackRunner
    this class is used to wrap the callback function in runner.
    when a callback function is called, it will be wrapped by this class.
    1. Lock
    2. Iterate over all callbacks registered under the corresponding name
    3. Call/Async-call the callback,
    4. Check the return value
    5. Change the event passed to the next callback in the stack
    6. Collect events
    7. Run events if any
    8. Return

    When using the callback runner, each callback entry that overrides the
    BaseClass will be called in the order of registration. The callback
    function can perform the following actions:
    1. Return/yield a CallbackEvent instance: this changes the event passed to
       the next callback in the stack.
    2. Return/yield InvokeSignal: this invokes the target callback after the
       current callback stack is finished.
    3. Raise InterruptSignal: this cancels all following callbacks in the task
       stack and invokes the target callback immediately.
    Note: For the yielded InvokeSignal, the target callback will be invoked
    concurrently.
    """

    @staticmethod
    def wrapper(func: Callable, cb_name):
        async def f(_instance, event: CallBackEvent, *args, **kwargs):
            assert isinstance(
                event, CallBackEvent
            ), f"{type(event)} is not CallBackEvent"
            cb_dict = getattr(_instance, "_cbs")
            call_later = []
            cb_instance = kwargs.pop("cb_instance", None)
            cg: List[Tuple[str, str]] = _instance.call_graph
            # in_node =
            if cb_instance is not None:
                logger.info(f"Calling for {cb_instance.__class__.__name__} ")
            try:
                for cb in cb_dict[cb_name]:
                    if (
                        cb_instance is not None
                        and cb_instance is not cb.__self__
                    ):
                        continue
                    logger.debug(
                        f"[{cb_name:15s}]"
                        f"[{str(cb.__self__.__class__.__name__):14s}] "
                        f"{str(event.task_name):15s} "
                        f"{str(event.task_id):15s} "
                        f"{event.task_spec.process_id if event.task_spec is not None else '':15s} "
                    )
                    if asyncio.iscoroutinefunction(cb):
                        retval = await cb(event, *args, **kwargs)
                    else:
                        retval = cb(event, *args, **kwargs)
                    _cbs = await _handle_call_back_output(retval)
                    call_later.extend(_cbs)
            except InterruptSignal as e:
                call_later.append(e)
            coroutines_to_call = []
            for sig in call_later:
                func_to_call = getattr(_instance, sig.signal)
                se = sig.event
                cg.append(
                    (
                        f"{event.task_name}-{event.task_id}-{func.__name__}",
                        f"{se.task_name}-{se.task_id}-{sig.signal}",
                    )
                )
                assert asyncio.iscoroutinefunction(func_to_call)
                _event = sig.event
                # _event.previous_task = event.task_name
                coroutines_to_call.append(func_to_call(_event))
            await asyncio.gather(*coroutines_to_call)
            return kwargs

        f.__doc__ = func.__doc__
        return f

    def __new__(cls, name, bases, attrs):
        for k, v in CALL_BACK_DICT.items():
            if not k.startswith("on_"):
                continue
            attrs[k] = _CallbackRunnerMeta.wrapper(v, k)
        return super().__new__(cls, name, bases, attrs)


class CallbackRunner(CallbackBase, metaclass=_CallbackRunnerMeta):
    def __init__(self, callbacks: List[CALLBACK_TYPE]) -> None:
        """
        Initializes CallbackRunner with a list of callbacks.
        :param callbacks: List of callback objects
        """
        self.cbs = callbacks
        self.single_issue = False
        self.call_graph = []

    @cached_property
    def _cbs(self) -> Dict[str, List[Callable]]:
        cbs = defaultdict(list)
        for cb in self.cbs:
            for (
                call_back_name,
                invalid_func,
            ) in CALL_BACK_DICT.items():
                if invalid_func is getattr(cb.__class__, call_back_name):
                    continue
                cbs[call_back_name].append(getattr(cb, call_back_name))
        return cbs


def parse_callbacks(callback_name: List[str]) -> List[Callable]:
    """
    Parse a list of callback names and return a list of callback functions.

    Args:
        callback_name (List[str]): A list of callback names.

    Returns:
        List[Callable]: A list of callback functions.
    """
    import importlib

    callbacks = []
    for name in callback_name:
        try:
            # Check if the callback function is in the .cbs package
            callback_module = importlib.import_module(".cbs", __package__)
            if hasattr(callback_module, name):
                callbacks.append(getattr(callback_module, name)())
        except ImportError:
            # If not in the .cbs package, import the package and initialize the
            # callback function
            callback_module = importlib.import_module(f".{name}", __package__)

        callback_function = getattr(callback_module, name)
        callbacks.append(callback_function)

    return callbacks
