import os

from ..callback import CallbackBase, CallBackEvent, InvokeSignal
from ..functional import RunnerTaskSpec, RequirementArg, RunnerArgSpec, ARG_TYPE
from ..functional import execute_task
from ..storage.storage import ResultStorage


class LocalRunner(CallbackBase):
    def __init__(self):
        super().__init__()

    def on_task_ready(self, event: CallBackEvent):
        task_name = event.task_name
        graph = event.graph
        run_id = event.run_id

        def arg_transfer(arg: RequirementArg) -> RunnerArgSpec:
            if arg.arg_type == ARG_TYPE.RAW:
                return RunnerArgSpec.model_validate(arg.model_dump())
            elif arg.arg_type == ARG_TYPE.TASK_OUTPUT:
                return RunnerArgSpec(
                    arg_type=ARG_TYPE.TASK_OUTPUT,
                    storage_path=ResultStorage.key_for(arg.from_task, run_id),
                )
            else:
                raise TypeError(f'unknown arg type {arg.arg_type}')

        spec = RunnerTaskSpec(
            task=task_name,
            root=graph.root,
            args=[arg_transfer(arg) for arg in graph.node_map[task_name].args],
            kwargs={
                k: arg_transfer(arg)
                for k, arg in graph.node_map[task_name].kwargs.items()
            },
            storage_path=event.result_storage.storage_path,
            output_path=ResultStorage.key_for(task_name, run_id),
            work_dir=os.path.join(
                '/tmp/storage/', run_id, task_name.replace('/', '_')),
        )
        execute_task(spec)
        return InvokeSignal('on_task_finish', event)
