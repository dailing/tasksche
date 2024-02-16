from functools import cached_property
import functools
import multiprocessing
from threading import Thread
import time
from tasksche.scheduler import run
import shutil
import os


class TestFunctionality:
    @cached_property
    def testroot(self):
        return os.path.split(__file__)[0]

    def test_simple_taskset(self):
        root = f"{self.testroot}/simple_task_set"
        storage = f"{root}/__default"
        assert os.path.exists(root), root
        shutil.rmtree(storage, ignore_errors=True)
        assert not os.path.exists(storage)
        run([f"{root}/task4.py"])
        assert os.path.exists(storage)
        assert os.path.exists(f"{storage}/task4/output.txt")
        assert os.path.exists(f"{storage}/task4/stdout.txt")
        assert open(f"{storage}/task4/output.txt").read() == "2141\n"
        assert open(f"{storage}/task4/stdout.txt").read() == "test\n"
        shutil.rmtree(storage)

    def test_loop_task(self):
        root = f"{self.testroot}/loop_task"
        storage = f"{root}/__default"
        assert os.path.exists(root), root
        shutil.rmtree(storage, ignore_errors=True)
        assert not os.path.exists(storage)
        run([f"{root}/task4.py"])
        assert os.path.exists(storage)
        assert os.path.exists(f"{storage}/task4/result.txt")
        assert os.path.exists(f"{storage}/task4/stdout.txt")
        assert open(f"{storage}/task4/result.txt").read() == "88\n"
        shutil.rmtree(storage)

    def test_error_reload(self):
        # todo check task 1 and 2 are not rerun
        root = f"{self.testroot}/simple_task_set_with_exception"
        root2 = f"{self.testroot}/simple_task_set"
        storage = f"{root}/__default"
        storage2 = f"{root2}/__default"
        assert os.path.exists(root), root
        assert os.path.exists(root2), root2
        shutil.rmtree(storage, ignore_errors=True)
        shutil.rmtree(storage2, ignore_errors=True)
        assert not os.path.exists(storage)
        assert not os.path.exists(storage2)
        run([f"{root}/task4.py"])
        assert not os.path.exists(f"{storage}/task4")
        shutil.copytree(
            storage,
            storage2,
        )
        run([f"{root2}/task4.py"])
        assert os.path.exists(f"{storage2}/task4/output.txt")
        assert os.path.exists(f"{storage2}/task4/stdout.txt")
        assert open(f"{storage2}/task4/output.txt").read() == "2141\n"
        shutil.rmtree(storage2)
        shutil.rmtree(storage)

    def test_online_reload(self):
        root = f"{self.testroot}/simple_test_online_reload"
        storage = f"{root}/__default"
        assert os.path.exists(root), root
        shutil.rmtree(storage, ignore_errors=True)
        assert not os.path.exists(storage)
        target = functools.partial(run, [f"{root}/task2.py"])
        process = multiprocessing.Process(target=target)
        process.start()
        time.sleep(1)
        assert os.path.exists(storage)
        assert os.path.exists(f"{storage}/task2/output.txt")
        process.kill()
        process.join()
        shutil.rmtree(storage)
