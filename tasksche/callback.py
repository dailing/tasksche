import asyncio
import functools
from collections import defaultdict
from functools import cached_property
from typing import (
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    Generator,
    List,
    Optional,
    Tuple,
    TypeVar,
)

from pydantic import BaseModel, ConfigDict

from .functional import RunnerTaskSpec
from .logger import Logger
from .asynctools import iter_over_async_gens

logger = Logger()


class CallBackEvent(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    # graph: Graph
    # run_id: str
    task_id: str
    task_name: str
    task_spec: Optional[RunnerTaskSpec]
    call_on: Optional[Any] = None

    def new_inst(self, **kwargs):
        assert "previous_event" not in kwargs
        return self.model_copy(update=kwargs | dict(previous_event=self))


class CallBackSignal:
    def __init__(self, signal: str, event: CallBackEvent):
        super().__init__()
        self.signal = signal
        self.event = event


class InvokeSignal(CallBackSignal):
    """
    Invoke target callback after the current task stack
    """

    pass


class CallbackBase:
    """
    Abstract Base Class for callback system.
    """

    def on_init(self, event: CallBackEvent):
        """
        Called when a task is started. Mainly used by RUNNER to initiate the
        task.
        """
        raise NotImplementedError

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

    def on_file_change(self, event: CallBackEvent):
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


class CallbackRunner:
    def __init__(self, callbacks: List[Optional[CALLBACK_TYPE]]) -> None:
        # self.event_queue: asyncio.Queue[Tuple[str, CallBackEvent]] = (
        #     asyncio.Queue()
        # )
        self._cbs = list(filter(lambda x: x is not None, callbacks))

    @cached_property
    def cbs(self) -> Dict[str, List[Callable]]:
        cbs = defaultdict(list)
        for cb in self._cbs:
            for (
                call_back_name,
                invalid_func,
            ) in CALL_BACK_DICT.items():
                if invalid_func is getattr(cb.__class__, call_back_name):
                    continue
                cbs[call_back_name].append(getattr(cb, call_back_name))
        return cbs

    async def handle_call(self, sig: InvokeSignal):
        funcs = []
        assert isinstance(sig, InvokeSignal)
        assert sig.signal in self.cbs
        assert sig.signal is not None
        for cb in self.cbs[sig.signal]:
            logger.debug(
                f"=== [{sig.signal}] [{cb.__self__.__class__.__name__}][{cb.__name__}] {sig.event}"
            )
            funcs.append(functools.partial(cb, sig.event))
        gg = iter_over_async_gens(funcs)
        new_tasks = []
        async for signal in gg:
            assert isinstance(signal, InvokeSignal), (type(signal), signal)
            new_tasks.append(asyncio.create_task(self.handle_call(signal)))
        await asyncio.gather(*new_tasks)

    async def run(self, init_call_back=None):
        if init_call_back is None:
            init_call_back = CallBackEvent(
                task_id="",
                task_name="ROOT",
                task_spec=None,
            )
        await self.handle_call(InvokeSignal("on_init", init_call_back))


def parse_callbacks(callback_name: List[str]) -> List[Callable]:
    """TODO
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
