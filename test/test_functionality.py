from functools import cached_property
import functools
import multiprocessing
import threading
import sys
from threading import Thread
import time

from regex import F
from tasksche.scheduler import run
import shutil
import os

result_storage = "/tmp/__work_dir"


class TestFunctionality:
    @cached_property
    def testroot(self):
        return os.path.split(__file__)[0]

    def test_simple_taskset(self):
        root = f"{self.testroot}/simple_task_set"
        storage = f"/tmp/__work_dir"
        if os.path.exists(storage):
            shutil.rmtree(storage, ignore_errors=True)
        assert not os.path.exists(storage)
        os.makedirs(storage, exist_ok=False)
        assert os.path.exists(storage), storage
        assert os.path.exists(root), root
        run(
            [f"{root}/task4.py"], storage_path=result_storage, work_dir=storage
        )
        assert os.path.exists(storage)
        assert os.path.exists(f"{storage}/task4/output.txt")
        assert open(f"{storage}/task4/output.txt").read() == "2141\n"
        shutil.rmtree(storage)

    def test_loop_task(self):
        root = f"{self.testroot}/loop_task"
        storage = f"/tmp/__work_dir"
        assert os.path.exists(root), root
        shutil.rmtree(storage, ignore_errors=True)
        assert not os.path.exists(storage)
        os.makedirs(storage, exist_ok=False)
        assert os.path.exists(storage), storage
        run(
            [f"{root}/task4.py"], storage_path=result_storage, work_dir=storage
        )
        assert os.path.exists(storage)
        assert os.path.exists(f"{storage}/task4/result.txt")
        assert open(f"{storage}/task4/result.txt").read() == "88\n"
        shutil.rmtree(storage)

    def test_error_reload(self):
        # todo check task 1 and 2 are not rerun
        root = f"{self.testroot}/simple_task_set_with_exception"
        root2 = f"{self.testroot}/simple_task_set"
        storage = f"/tmp/__work_dir"
        storage2 = f"/tmp/__work_dir"
        assert os.path.exists(root), root
        assert os.path.exists(root2), root2
        shutil.rmtree(storage, ignore_errors=True)
        assert not os.path.exists(storage)
        os.makedirs(storage, exist_ok=False)
        assert os.path.exists(storage), storage
        run([f"{root}/task4.py"], storage_path=storage, work_dir=storage)
        assert not os.path.exists(f"{storage}/task4")
        # shutil.copytree(
        #     storage,
        #     storage2,
        # )
        run(
            [f"{root2}/task4.py"],
            storage_path=result_storage,
            work_dir=storage,
        )
        assert os.path.exists(f"{storage2}/task4/output.txt")
        assert open(f"{storage2}/task4/output.txt").read() == "2141\n"
        # shutil.rmtree(storage2)
        shutil.rmtree(storage)

    def test_online_reload(self):
        root = f"{self.testroot}/simple_test_online_reload"
        storage = f"/tmp/__work_dir"
        if os.path.exists(storage):
            shutil.rmtree(storage, ignore_errors=True)
        assert not os.path.exists(storage)
        os.makedirs(storage, exist_ok=False)
        assert os.path.exists(storage), storage
        file_cnt = open(f"{root}/task2.py", "r").read()
        with open(f"{root}/task2.py", "w") as f:
            f.write(file_cnt.replace("    infinite: 0", "    infinite: 1"))

        assert os.path.exists(root), root
        target = functools.partial(
            run,
            [f"{root}/task3.py"],
            watch_root=True,
            storage_path=storage,
            work_dir=storage,
        )
        process = multiprocessing.Process(target=target)
        process.start()
        time.sleep(2)
        assert os.path.exists(storage)
        assert os.path.exists(f"{storage}/task2")
        assert os.path.exists(
            f"{storage}/task2/output.txt"
        ), f"{storage}/task2/output.txt"
        assert not os.path.exists(f"{storage}/task3")

        print(
            "================================================", file=sys.stderr
        )
        with open(f"{root}/task2.py", "w") as f:
            f.write(file_cnt.replace("    infinite: 1", "    infinite: 0"))
        time.sleep(2)
        assert os.path.exists(f"{storage}/task3/output.txt")
        assert process.is_alive()
        if process.is_alive():
            process.kill()
            process.terminate()
        process.join()
        with open(f"{root}/task2.py", "w") as f:
            f.write(file_cnt.replace("    infinite: 0", "    infinite: 1"))
        shutil.rmtree(storage)
