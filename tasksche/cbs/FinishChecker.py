from itertools import chain

from ..callback import CallbackBase, CallBackEvent, InterruptSignal
from ..logger import Logger
from ..storage.storage import ResultStorage

logger = Logger()

NOT_DUMP_HASH = object()


class FinishChecker(CallbackBase):
    def __init__(
            self,
            result_storage: str = 'file:default',
            hash_storage: str = 'file:hash'):
        self.result_storage = ResultStorage(result_storage)
        self.hash_storage = ResultStorage(hash_storage)

    def on_task_finish(self, event: CallBackEvent):
        if event.task_name == '_END_':
            return
        if event.value is NOT_DUMP_HASH:
            return event.new_inst(value=None)
        task_name, run_id, graph = event.task_name, event.run_id, event.graph
        code_hash_map = {
            k: graph.node_map[k].hash_code for k in chain(
                graph.requirements_map[task_name], [task_name])}

        result_map = {
            k: self.result_storage.get_hash(k, run_id)
            for k in graph.requirements_map[task_name]
        }
        self.hash_storage.store(task_name, run_id, value=(code_hash_map, result_map))

    def on_task_ready(self, event: CallBackEvent):
        task_name, run_id, graph = event.task_name, event.run_id, event.graph
        if not self.result_storage.has(task_name, run_id):
            return
        # logger.info('check hash')
        if not self.hash_storage.has(task_name, run_id):
            return
        # logger.info('compare hash')
        code_hash_map, result_map = self.hash_storage.get(task_name, run_id)
        if code_hash_map is None or result_map is None:
            return
        for k in graph.requirements_map[task_name]:
            if code_hash_map.get(k, None) != graph.node_map[k].hash_code:
                # logger.info(f'{k} code hash not match')
                return
            if result_map.get(k, None) != self.result_storage.get_hash(k, run_id):
                # logger.info(f'{k} result not match')
                return
        if code_hash_map.get(task_name, None) != graph.node_map[task_name].hash_code:
            # logger.info(f'{task_name} code hash not match')
            return
        # logger.info('hash match')
        raise InterruptSignal('on_task_finish', event.new_inst(value=NOT_DUMP_HASH))
