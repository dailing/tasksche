from typing import Dict


# from ..task_spec import TaskSpec
from ..callback import CallbackBase
from ..functional import Status


class ProgressCB(CallbackBase):
    def __init__(self):
        super().__init__()
        self.bars: Dict[str, tqdm] = {}
        self.total_tasks = -1

    def on_task_start(self, task):
        if self.total_tasks < 0:
            # Initiate bars
            assert task.task_dict is not None
            self.total_tasks = len(task.task_dict) - 1
            total_finished = sum(
                [
                    v.status == Status.STATUS_FINISHED
                    for v in task.task_dict.values()
                ]
            )
            self.bars["ALL"] = tqdm(
                position=0,
                total=self.total_tasks,
                desc="ALL",
                initial=total_finished,
            )
        # add a new bar
        self.bars[task.task_name] = tqdm(
            position=len(self.bars), total=100, desc=f"{task.task_name}"
        )

    def on_task_finish(self, task):
        task_name = task.task_name
        # pos = self.bars[task_name].pos
        self.bars[task_name].update(100)
        # self.bars[task_name].close()
        # del self.bars[task_name]
        # for bar in self.bars.values():
        #     if bar.pos > pos:
        #         bar.pos -= 1
        self.bars["ALL"].update(1)
