# %%
import argparse
import shutil
from functools import cached_property
from io import BytesIO, StringIO
import pickle
from pprint import pprint
import time
from typing import Dict, List
import os
import sys
import inspect
import yaml
import subprocess
from dataclasses import dataclass
from survivalreg.util.logger import get_logger
from hashlib import md5
from typing import Union

logger = get_logger('runrun')


def pprint_str(*args, **kwargs):
    sio = StringIO()
    pprint(*args,  stream=sio, **kwargs)
    sio.seek(0)
    return sio.read()


@dataclass
class TaskSpec:
    task_root: str
    task_name: str
    require: Union[Dict[str, str], List[str], None] = None
    exec: str = ''
    inherent: str = None
    virtual: bool = False
    depend: List[str] = None
    rerun: bool = False

    def __getitem__(self, item):
        return getattr(self, item)

    def get(self, name, default=None):
        if hasattr(self, name):
            return getattr(self, name)
        return default

    def __repr__(self) -> str:
        tt = f'TaskSpec({self.task_root}@{self.task_name})'
        if self.inherent_task is not None:
            tt += f'->{self.inherent_task}'
        return tt

    def __hash__(self) -> int:
        return hash((self.task_root, self.task_name))

    def __eq__(self, __o: object) -> bool:
        return self.task_name == self.task_name

    def __gt__(self, _o: object) -> bool:
        return self.task_name > _o.task_name

    @property
    def depend_files(self):
        if self.depend is None:
            return []
        result = []
        for dep in self.depend:
            result.append(os.path.join(self.task_root,
                          process_path(self.task_root, dep)[1:]))
        return result

    def clean(self):
        if not (os.path.exists(self.result_file) or os.path.exists(self.result_info)):
            return True
        try:
            os.remove(self.result_info)
            os.remove(self.result_file)
            return True
        except Exception as e:
            logger.info(e)
        try:
            output_dir = os.path.join(self.task_dir, '_output')
            logger.info(f'removing {output_dir}')
            shutil.rmtree(output_dir, ignore_errors=True)
            # os.removedirs(os.path.join(self.task_dir, '_output'))
        except Exception as e:
            logger.info(e)
        return False

    @property
    def inherent_task(self):
        if self.inherent is None:
            return None
        return process_path(self.task_name, self.inherent)

    @property
    def task_dir(self) -> str:
        return os.path.join(self.task_root, self.task_name[1:])

    @property
    def code_file(self) -> str:
        return os.path.join(self.task_dir, 'task.py')

    @property
    def code_hash(self) -> str:
        m = md5()
        m.update(open(self.code_file, 'rb').read())
        # if self.inherent_task is not None:
        #     m.update(open(self.inherent_task_file, 'rb').read())
        for f in self.depend_files:
            m.update(open(f, 'rb').read())
        return m.hexdigest()

    @property
    def code_update_time(self) -> float:
        code_update = os.stat(self.code_file).st_mtime
        # if self.inherent_task is not None:
        #     code_update = max(code_update, os.stat(
        #         self.inherent_task_file).st_mtime)
        for dep in self.depend_files:
            code_update = max(
                code_update,
                os.stat(dep).st_mtime)
        return code_update

    @property
    def result_file(self) -> str:
        return os.path.join(self.task_dir, 'result.pkl')

    @property
    def result_info(self) -> str:
        return os.path.join(self.task_dir, 'result_info.pkl')

    @property
    def dirty(self) -> bool:
        if self.rerun:
            return True
        code_update = self.code_update_time
        try:
            result_update = os.stat(self.result_file).st_mtime
            # logger.debug(dict(code_update=code_update, result_update=result_update))
            if code_update < result_update:
                return False
            else:
                code_hash, t = pickle.load(open(self.result_info, 'rb'))
                # logger.debug(f'check_hash {code_hash} {self.code_hash}')
                if code_hash == self.code_hash:
                    return False

        except FileNotFoundError:
            # logger.debug(f'result file Not Fund for {self.task_name}')
            return True
        return True

    @cached_property
    def require_dict(self) -> Dict[Union[int, str], str]:
        if isinstance(self.require, dict):
            return self.require
        elif isinstance(self.require, list):
            return {i: v for i, v in enumerate(self.require)}
        elif self.require is None:
            return {}
        raise Exception('require not list or dict or NONE')

    @property
    def dependent_tasks(self):
        dep = []
        for _, v in self.require_dict.items():
            if isinstance(v, str) and v.startswith('$'):
                dep.append(v[1:])
        return dep

    @property
    def module_path(self):
        if self.inherent_task is not None:
            mod = os.path.join(self.inherent_task, 'task')
        else:
            mod = os.path.join(self.task_name, 'task')
        return mod.replace('/', '.')[1:]

    @property
    def call_arguments(self):
        kwargs = {}
        logger.info(self.require_dict)
        for k, v in self.require_dict.items():
            if not (isinstance(v, str) and v.startswith('$')):
                kwargs[k] = v
                continue
            result_file = os.path.join(self.task_root, v[2:], 'result.pkl')
            result = pickle.load(open(result_file, 'rb'))
            kwargs[k] = result
        # logger.info(kwargs)
        if isinstance(self.require, list):
            args = [None] * len(kwargs)
            for k, v in kwargs.items():
                # logger.info(f'setting key {k} ')
                args[k] = v
            return args
        return kwargs

    def dump_result(self, result):
        # file_hash = self.hash
        pickle.dump(result, open(self.result_file, 'wb'))
        pickle.dump((self.code_hash, time.time()),
                    open(self.result_info, 'wb'))


def process_path(task_name, path):
    if not path.startswith('/'):
        path = os.path.join(task_name, path)
    path_filtered = []
    for subpath in path.split('/'):
        if subpath == '.':
            continue
        if subpath == '..':
            path_filtered.pop()
            continue
        path_filtered.append(subpath)
    if path_filtered[0] != '':
        path_filtered = task_name.split('/') + path_filtered
    path_new = '/'.join(path_filtered)
    return path_new


def process_reference(ref, task_spec: TaskSpec):
    if not isinstance(ref, str):
        return ref
    if ref.startswith('$'):
        path = ref[1:]
        path_new = '$' + process_path(task_spec.task_name, path)
        return path_new
    return ref


def post_process_anno(anno) -> TaskSpec:
    require = anno.get('require', {})
    if require is None:
        require = {}
    it = None
    if isinstance(require, list):
        it = enumerate(require)
    else:
        it = require.items()
    for k, v in it:
        require[k] = process_reference(v, anno)
        anno.require = require
    return anno


def schedule_task(tasks: Dict[str, TaskSpec]) -> List[List[TaskSpec]]:
    schedule = []
    un_scheduled = set(tasks.values())
    scheduled = set()
    dirty_set = set()

    for t in sorted(list(un_scheduled)):
        if t.virtual:
            continue
        if not t.dirty:
            continue
        logger.debug(f'Dirty Task {t}')
    # pdb.set_trace()
    while len(un_scheduled) > 0:
        ready_list = []
        dirty_list = []
        for task in list(un_scheduled):
            ready = True
            dirty = task.dirty
            for dep in task.dependent_tasks:
                dep = tasks[dep]
                if dep not in scheduled:
                    ready = False
                    break
                if dep in dirty_set:
                    # logger.debug(f'dep {dep} dirty, {task}')
                    dirty = True
            if ready:
                ready_list.append(task)
                if dirty:
                    dirty_list.append(task)
        for task in ready_list:
            un_scheduled.remove(task)
            scheduled.add(task)
        for task in dirty_list:
            dirty_set.add(task)
        running_list = list(filter(lambda x: x in dirty_set, ready_list))
        running_list = list(filter(lambda x: x.virtual == False, running_list))
        if len(running_list) > 0:
            running_list.sort()
            schedule.append(running_list)
        # logger.info(schedule)
        # logger.info(scheduled)
        # logger.info(dirty_set)
        # logger.info(un_scheduled)
        # sys.exit(0)
    return schedule


def run_task(tasks: Dict[str, TaskSpec], target=None):
    tg = schedule_task(tasks)
    for task_group in tg:
        logger.debug('\n' + pprint_str(task_group))
    for task_group in tg:
        task_group.sort()
        tg_names = [t.task_name for t in task_group]
        try:
            idx = tg_names.index(target)
            task_group = [task_group[idx]]
        except ValueError as e:
            pass
        processes = []
        for task in task_group:
            env = dict(os.environ)
            env['PYTHONPATH'] = os.path.abspath(task.task_root)
            process = subprocess.Popen(
                [
                    'python',
                    os.path.split(__file__)[0] + '/worker.py',
                    task.task_root,
                    task.code_file,
                ],
                stdout=sys.stdout,
                stderr=sys.stderr,
                env=env,
                cwd=os.path.split(os.path.abspath(task.task_root))[0]
            )
            processes.append(process)
        for process in processes:
            ret_status = process.wait()
            if ret_status != 0:
                logger.error(f"ERROR exit {ret_status}")
                return


def extract_anno(root, file) -> TaskSpec:
    payload = bytearray()
    with open(file, 'rb') as f:
        line = f.readline()
        while True:
            line = f.readline()
            if line == b'"""\n':
                break
            payload.extend(line)
    task_name = os.path.split(file)[0]
    assert task_name.startswith(root)
    task_name = task_name[len(root):]
    if not task_name.startswith('/'):
        task_name = '/' + task_name
    task_info = yaml.safe_load(BytesIO(payload))
    task_info['task_root'] = root
    task_info['task_name'] = task_name
    tki = TaskSpec(**task_info)
    return post_process_anno(tki)


def parse_target(target: str) -> Dict[str, TaskSpec]:
    tasks: Dict[str, TaskSpec] = {}
    for dir_path, _, file_names in os.walk(target):
        for file in file_names:
            if file == 'task.py':
                # logger.debug(f'parsing {dir_path}')
                file_path = os.path.join(dir_path, file)
                task_info = extract_anno(target, file_path)
                tasks[task_info.task_name] = task_info
    stop = False
    while not stop:
        stop = True
        for k, v in tasks.items():
            if v.depend is None:
                v.depend = []
            if v.inherent_task is not None:
                if v.inherent_task not in tasks:
                    logger.error(f'{v.inherent_task} not exist, {v}')
                    raise FileNotFoundError
                inh_task = tasks[v.inherent_task]
                if inh_task.task_name + '/task.py' not in v.depend:
                    v.depend.append(inh_task.task_name + '/task.py')
                    # logger.debug(f'adding {inh_task.task_name + "/task.py"}')
                    dep = False
                if inh_task.depend is not None:
                    for dep in inh_task.depend:
                        if dep not in v.depend:
                            # logger.debug(f'adding {dep}')
                            v.depend.append(dep)
                            stop = False
    return tasks


def run_target(target: str, task=None):
    logger.info(f'executing on: {target} ...')
    tasks = parse_target(target)
    # logger.debug(tasks)
    run_task(tasks, task)


def clean_target(target: str):
    logger.info(f'cleaning : {target}')
    tasks = parse_target(target)
    for _, v in tasks.items():
        v.clean()


def new_task(target: str, task: str):
    logger.info(f'creating new task : {target} {task}')
    if not os.path.exists(target):
        print("ERROR no such target")
        return
    if task.startswith('/'):
        task = task[1:]
    full_path = os.path.join(target, task)
    if not os.path.exists(full_path):
        os.makedirs(full_path)
    file_path = os.path.join(full_path, 'task.py')
    if not os.path.exists(file_path):
        with open(file_path, 'w') as f:
            f.write(
                '''"""
require: {}

"""

def run():
    pass

'''
            )


if __name__ == '__main__':
    _parsers = _parser()

    def _command(*args, **kwargs):
        _subparser = _parsers['_subparser']

        def wrap(func):
            func_name = func.__name__
            parser: argparse.ArgumentParser = _subparser.add_parser(
                func_name, *args, **kwargs)
            for k_name, par in inspect.signature(func).parameters.items():
                required = par.default is inspect.Parameter.empty
                default = par.default if par.default is not inspect.Parameter.empty else None
                logger.info(
                    f'{func_name} {k_name}, {default} req: {required} {par.annotation}')
                if required:
                    parser.add_argument(k_name, default=default,
                                        type=par.annotation)
                else:
                    k_name = '-' + k_name
                    parser.add_argument(k_name, default=default, required=required,
                                        type=par.annotation)
            parser.set_defaults(__func=func)

        return wrap

    @_command()
    def run(target: str, task: str = None):
        run_target(target, task)

    @_command()
    def clean(target: str):
        clean_target(target)

    @_command()
    def new(target: str, task: str):
        new_task(target, task)

    args = _parsers['parser'].parse_args()

    call_args = args.__dict__.copy()
    del call_args['__func']

    args.__func(**call_args)
