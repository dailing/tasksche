import dataclasses
from collections import defaultdict
from typing import Callable, Dict, List, Any, TypeVar

from .common import CachedPropertyWithInvalidator, Status
from .logger import Logger
from .task_spec import TaskSpec

logger = Logger()


@dataclasses.dataclass
class _CallBackPipe:
    kwargs: Dict[str, Any]


def call_back_pipe(**kwargs) -> _CallBackPipe:
    return _CallBackPipe(kwargs)


class CallbackBase:
    """
    Abstract Base Class for callback system.
    """
    __callbacks__: Dict[str, Callable] = None

    def before_status_change(
            self, task: TaskSpec, old_status: Status, new_status: Status):
        """
        Called before status change.
        """
        raise NotImplementedError()

    def after_status_change(
            self, task: TaskSpec, new_status: Status):
        """
        Called after status change.
        """
        raise NotImplementedError()

    def task_start(self, task: TaskSpec):
        raise NotImplementedError()

    def task_finish(self, task: TaskSpec):
        raise NotImplementedError()

    def task_error(self, task: TaskSpec):
        raise NotImplementedError()

    def task_interrupt(self, task: TaskSpec):
        raise NotImplementedError()


CALLBACK_TYPE = TypeVar('CALLBACK_TYPE', bound=CallbackBase)
CALL_BACK_DICT: Dict[str, Callable] = {
    k: v for k, v in CallbackBase.__dict__.items() if not k.startswith('_')
}


class _CallbackRunnerMeta(type):
    @staticmethod
    def wrapper(func, cb_name):
        def f(_instance, **kwargs):
            cb_dict = getattr(_instance, '_cbs')
            for cb in cb_dict[cb_name]:
                retval = cb(**kwargs)
                if retval is None:
                    pass
                elif isinstance(retval, _CallBackPipe):
                    kwargs.update(retval.kwargs)
                else:
                    logger.error(
                        f'invalid return for {func} {retval}',
                        exc_info=True,
                        stack_info=True,
                    )
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

    @CachedPropertyWithInvalidator
    def _cbs(self) -> Dict[str, List[Callable]]:
        cbs = defaultdict(list)
        for cb in self.cbs:
            for call_back_name, invalid_func in CALL_BACK_DICT.items():
                if invalid_func is getattr(cb.__class__, call_back_name):
                    continue
                cbs[call_back_name].append(getattr(cb, call_back_name))
        return cbs
