import inspect
import os.path

from .callback import CallBackEvent
from .functional import RequirementArg, RunnerArgSpec, ARG_TYPE, RunnerTaskSpec
from .storage.storage import ResultStorage


def path_for(p) -> str:
    directory = os.path.split(inspect.stack()[1].filename)[0]
    return os.path.join(directory, p)


def call_back_event_to_runner_task(event: CallBackEvent) -> RunnerTaskSpec:
    task_name = event.task_name
    graph = event.graph
    run_id = event.run_id
    n_iter = event.n_iter

    def arg_transfer(arg: RequirementArg) -> RunnerArgSpec:
        if arg.arg_type == ARG_TYPE.RAW:
            return RunnerArgSpec.model_validate(arg.model_dump())
        elif arg.arg_type == ARG_TYPE.TASK_OUTPUT:
            return RunnerArgSpec(
                arg_type=ARG_TYPE.TASK_OUTPUT,
                storage_path=ResultStorage.key_for(
                    arg.from_task, run_id, n_iter[:event.graph.layer_map[arg.from_task]]),
            )
        else:
            raise TypeError(f'unknown arg type {arg.arg_type}')

    spec = RunnerTaskSpec(
        task=task_name,
        root=graph.root,
        args=[
            arg_transfer(arg) for arg in graph.node_map[task_name].args
            if arg.arg_type != ARG_TYPE.VIRTUAL
        ],
        kwargs={
            k: arg_transfer(arg)
            for k, arg in graph.node_map[task_name].kwargs.items()
        },
        storage_path=event.result_storage.storage_path,
        output_path=ResultStorage.key_for(task_name, run_id, i_iter=event.n_iter),
        work_dir=os.path.join(
            '/tmp/storage/', run_id, task_name.replace('/', '_')),
    )
    return spec
