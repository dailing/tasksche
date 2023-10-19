import asyncio
import dataclasses
import inspect
from collections import defaultdict
from functools import cached_property
from typing import Callable, Dict, List, Any, TypeVar, Optional, Generator, Tuple

from pydantic import BaseModel, field_validator

from .functional import Graph, ROOT_NODE
from .logger import Logger
from .storage.storage import ResultStorage, StatusStorage

logger = Logger()


@dataclasses.dataclass
class _CallBackPipe:
    kwargs: Dict[str, Any]


def call_back_pipe(**kwargs) -> _CallBackPipe:
    return _CallBackPipe(kwargs)


class CallBackEvent(BaseModel):
    graph: Graph
    run_id: str
    task_name: Optional[str] = None
    value: Optional[Any] = None
    result_storage: Optional[ResultStorage] = None
    status_storage: Optional[StatusStorage] = None

    class Config:
        arbitrary_types_allowed = True

    def new_inst(self, **kwargs):
        return self.model_copy(update=kwargs)
        # return CallBackEvent(**self.model_dump(exclude=kwargs.keys()), **kwargs)

    @field_validator("graph")
    def validate_graph(cls, v):
        if not isinstance(v, Graph):
            raise ValueError(f'{type(v)} is not Graph')
        return v

    @field_validator("result_storage")
    def validate_result_storage(cls, v):
        if not isinstance(v, ResultStorage) and v is not None:
            raise ValueError(f'{type(v)} is not ResultStorage')
        return v

    @field_validator("status_storage")
    def validate_status_storage(cls, v):
        if not isinstance(v, StatusStorage) and v is not None:
            raise ValueError(f'{type(v)} is not StatusStorage')
        return v


class CallBackSignal:
    def __init__(self, signal: str, event: CallBackEvent, *args, **kwargs):
        super().__init__()
        self.signal = signal
        self.event = event
        self.args = args
        self.kwargs = kwargs


class InterruptSignal(CallBackSignal, Exception):
    """
    Cancel all following callbacks in the task stack and invoke the target callback
    """
    pass


class InvokeSignal(CallBackSignal):
    """
    Invoke target callback after the current task stack
    """
    pass


class CallbackBase:
    IGNORE_ROOT_NODE = False
    """
    Abstract Base Class for callback system.
    """

    def on_feed(self, event: CallBackEvent, payload: Any = None):
        raise NotImplementedError()

    def on_task_ready(self, event: CallBackEvent):
        """
        Called when a task is ready.
        """
        raise NotImplementedError()

    def on_task_start(self, event: CallBackEvent):
        """
        Called when a task is started. Mainly used by RUNNER to initiate the task.
        """
        raise NotImplementedError

    def on_task_finish(self, event: CallBackEvent):
        """
        Called when a task is finished.
        """
        raise NotImplementedError

    def on_run_finish(self, event: CallBackEvent):
        """
        Called when a run is finished.
        """
        raise NotImplementedError

    def on_interrupt(self, event: CallBackEvent):
        """
        Called when a task is interrupted. should kill all running tasks and return
        """
        raise NotImplementedError


CALLBACK_TYPE = TypeVar('CALLBACK_TYPE', bound=CallbackBase)
CALL_BACK_DICT: Dict[str, Callable] = {
    k: v for k, v in CallbackBase.__dict__.items() if not k.startswith('_')
}


def _handle_call_back_output(
        retval=None | Generator | InvokeSignal
) -> Tuple[List[InvokeSignal], Optional[CallBackEvent]]:
    # logger.info('here')
    if retval is None:
        return [], None
    elif isinstance(retval, InvokeSignal):
        return [retval], None
    elif isinstance(retval, Generator):
        # logger.info('generator')
        val = []
        event = None
        for ret in retval:
            _val, _event = _handle_call_back_output(ret)
            val.extend(_val)
            event = _event
        return val, event
    elif isinstance(retval, CallBackEvent):
        return [], retval
    logger.error(f'unknown type {type(retval)}')
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

    When using the callback runner, each callback entry overrides the BaseClass will
    be called according to the order of registration.
    The callback function can do something like this:
    1. Return/yield a CallbackEvent instance: change the event passed to the next
        callback in the stack
    2. Return/yield InvokeSignal: invoke the target callback after the current callback
        stack is finished
    3. Raise InterruptSignal: cancel all following callbacks in the task stack and
        invoke the target callback immediately
    Note: for the yielded InvokeSignal, the target callback will be invoked concurrently
    """

    @staticmethod
    def wrapper(func: Callable, cb_name):
        # get list of args using inspect package, and transfer to kwargs
        list_of_args_name = inspect.getfullargspec(func).args
        if len(list_of_args_name) > 0 and list_of_args_name[0] == 'self':
            list_of_args_name = list_of_args_name[1:]
        assert list_of_args_name[0] == 'event', f'invalid args {list_of_args_name}'
        list_of_args_name = list_of_args_name[1:]

        async def f(_instance, event: CallBackEvent, *args, **kwargs):
            assert isinstance(event, CallBackEvent), \
                f'{type(event)} is not CallBackEvent'
            for arg_value, arg_name in zip(args, list_of_args_name[:len(args)]):
                kwargs[arg_name] = arg_value
            cb_dict = getattr(_instance, '_cbs')
            call_later = []
            try:
                for cb in cb_dict[cb_name]:
                    if (
                            event.task_name == ROOT_NODE.NAME
                            and getattr(cb.__self__, 'IGNORE_ROOT_NODE')):
                        continue
                    logger.debug(
                        f'[{cb_name:15s}][{str(cb.__self__.__class__.__name__):15s}] '
                        f'{str(event.task_name):15s} '
                        f'{event.run_id}'
                    )
                    if asyncio.iscoroutinefunction(cb):
                        retval = await cb(event, **kwargs)
                    else:
                        retval = cb(event, **kwargs)
                    _cbs, _event = _handle_call_back_output(retval)
                    call_later.extend(_cbs)
                    event = _event if _event is not None else event
            except InterruptSignal as e:
                call_later.append(e)
            fs = []
            for sig in call_later:
                func_to_call = getattr(_instance, sig.signal)
                assert asyncio.iscoroutinefunction(func_to_call)
                fs.append(asyncio.create_task(
                    func_to_call(sig.event, *sig.args, **sig.kwargs)))
            if len(fs) > 0:
                await asyncio.wait(fs)
            return kwargs

        f.__doc__ = func.__doc__
        return f

    def __new__(cls, name, bases, attrs):
        for k, v in CALL_BACK_DICT.items():
            if not k.startswith('on_'):
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

    @cached_property
    def _cbs(self) -> Dict[str, List[Callable]]:
        cbs = defaultdict(list)
        for cb in self.cbs:
            for call_back_name, invalid_func in CALL_BACK_DICT.items():
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
            callback_module = importlib.import_module(
                '.cbs', __package__)
            if hasattr(callback_module, name):
                callbacks.append(getattr(callback_module, name)())
        except ImportError:
            # If not in the .cbs package, import the package and initialize the callback function
            callback_module = importlib.import_module(f'.{name}', __package__)

        callback_function = getattr(callback_module, name)
        callbacks.append(callback_function)

    return callbacks
