from collections import defaultdict

from ..callback import CallbackBase, CallBackEvent, InvokeSignal, InterruptSignal
from ..functional import execute_task, IteratorTaskExecutor
from ..logger import Logger
from ..util import call_back_event_to_runner_task

logger = Logger()


class LocalRunner(CallbackBase):
    def __init__(self):
        super().__init__()
        self.iterators = defaultdict(IteratorTaskExecutor)

    def on_task_start(self, event: CallBackEvent):
        if event.graph.node_map[event.task_name].is_generator:
            n_iter = event.n_iter
            if event.n_iter is None:
                n_iter = [0]
            else:
                assert isinstance(n_iter, list)
                n_iter = n_iter.copy()
                n_iter.append(0)
            logger.info(f'on start {n_iter}')
            assert len(n_iter) == event.graph.layer_map[event.task_name]
            return InvokeSignal('on_iterate', event.new_inst(n_iter=n_iter))
        spec = call_back_event_to_runner_task(event)
        execute_task(spec)
        return InvokeSignal('on_task_finish', event)

    def on_iterate(self, event: CallBackEvent):
        spec = call_back_event_to_runner_task(event)
        iterator_executor = self.iterators[spec.task]
        output = iterator_executor.call_run(spec)
        logger.debug(f'{event.n_iter} {output}')
        assert len(event.n_iter) == event.graph.layer_map[event.task_name]
        if isinstance(output, StopIteration):
            if len(event.n_iter) == 1:
                yield InvokeSignal('on_iter_finish', event)
        elif isinstance(output, Exception):
            raise InterruptSignal('on_task_error', event)
        elif isinstance(output, str):
            yield InvokeSignal('on_task_finish', event.new_inst())
            event = event.new_inst(n_iter=event.n_iter.copy())
            event.n_iter[-1] += 1
            logger.info(output)
            yield InvokeSignal('on_iterate', event)
        else:
            raise TypeError(f'output type {type(output)} {output} is not supported')

    def on_iter_finish(self, event: CallBackEvent):
        pass
