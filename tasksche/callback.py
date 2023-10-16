import dataclasses
import inspect
from collections import defaultdict
from functools import cached_property
from typing import Callable, Dict, List, Any, TypeVar, Optional

from pydantic import BaseModel, validator

from .functional import Graph
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
        return CallBackEvent(**self.dict(exclude=kwargs.keys()), **kwargs)

    @validator("graph")
    def validate_graph(cls, v):
        if not isinstance(v, Graph):
            raise ValueError(f'{type(v)} is not Graph')
        return v

    @validator("result_storage")
    def validate_result_storage(cls, v):
        if not isinstance(v, ResultStorage) and v is not None:
            raise ValueError(f'{type(v)} is not ResultStorage')
        return v

    @validator("status_storage")
    def validate_status_storage(cls, v):
        if not isinstance(v, StatusStorage) and v is not None:
            raise ValueError(f'{type(v)} is not StatusStorage')
        return v


class EmitEvent(Exception):
    def __init__(self, signal: str, event: CallBackEvent, *args, **kwargs):
        super().__init__()
        self.signal = signal
        self.event = event
        self.args = args
        self.kwargs = kwargs


class CallbackBase:
    """
    Abstract Base Class for callback system.
    """

    def on_feed(self, event: CallBackEvent, kwargs: Dict[str, Any]):
        raise NotImplementedError()

    def on_task_ready(self, event: CallBackEvent):
        """
        Called when a task is ready.
        """
        raise NotImplementedError()

    def on_task_finish(self, event: CallBackEvent):
        raise NotImplementedError


CALLBACK_TYPE = TypeVar('CALLBACK_TYPE', bound=CallbackBase)
CALL_BACK_DICT: Dict[str, Callable] = {
    k: v for k, v in CallbackBase.__dict__.items() if not k.startswith('_')
}


class _CallbackRunnerMeta(type):
    @staticmethod
    def wrapper(func: Callable, cb_name):
        # get list of args using inspect package, and transfer to kwargs
        list_of_args_name = inspect.getfullargspec(func).args
        if len(list_of_args_name) > 0 and list_of_args_name[0] == 'self':
            list_of_args_name = list_of_args_name[1:]
        assert list_of_args_name[0] == 'event', f'invalid args {list_of_args_name}'
        list_of_args_name = list_of_args_name[1:]

        def f(_instance, event: CallBackEvent, *args, **kwargs):
            assert isinstance(event, CallBackEvent), \
                f'{type(event)} is not CallBackEvent'
            for arg_value, arg_name in zip(args, list_of_args_name[:len(args)]):
                kwargs[arg_name] = arg_value
            cb_dict = getattr(_instance, '_cbs')
            for cb in cb_dict[cb_name]:
                logger.debug(
                    f'[{cb_name:15s}][{str(cb.__self__.__class__.__name__):15s}] '
                    f'{str(event.task_name):15s} '
                    f'{event.run_id}'
                )
                try:
                    retval = cb(event, **kwargs)
                    if isinstance(retval, CallBackEvent):
                        event = retval
                except EmitEvent as e:
                    func_to_call = getattr(_instance, e.signal)
                    func_to_call(e.event, *e.args, **e.kwargs)
                    break
            return kwargs

        f.__doc__ = func.__doc__
        return f

    def __new__(cls, name, bases, attrs):
        for k, v in CALL_BACK_DICT.items():
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
