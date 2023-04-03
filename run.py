# %%
from collections import defaultdict, namedtuple
import shutil
from functools import cached_property
from io import BytesIO, StringIO
import pickle
from pprint import pprint
import time
from typing import Any, Dict, List, Set, Tuple
import os
import sys
import yaml
import subprocess
from dataclasses import dataclass
from hashlib import md5
from typing import Union
import signal


import logging
import datetime
import os.path


def get_logger(name: str, print_level=logging.DEBUG):
    formatter = logging.Formatter(
        fmt="%(levelname)6s [%(filename)15s:%(lineno)-3d %(asctime)s] %(message)s",
        datefmt='%H:%M:%S',
    )
    # time_now = datetime.datetime.now().strftime("%Y_%m_%d.%H_%M_%S")
    logger = logging.getLogger(name)
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(print_level)
    # if not os.path.exists(f'/tmp//tmp/log/{time_now}/'):
    #     os.makedirs(f'/tmp/log/{time_now}/', exist_ok=True)
    # file_handler = logging.FileHandler(f'/tmp/log/{time_now}/{name}.log')
    # file_handler.setFormatter(formatter)
    # file_handler.setLevel(logging.DEBUG)
    logger.addHandler(stream_handler)
    # logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)
    return logger


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
        return os.path.join('_output', self.task_dir, 'result.pkl')

    @property
    def result_info(self) -> str:
        return os.path.join('_output', self.task_dir, 'result_info.pkl')

    @property
    def dirty(self) -> bool:
        """
        Return true if the task has been modified since the last time it was
        run. Note that this does not count the status of dependent tasks.
        """
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
        # logger.debug(self.require_dict)
        for k, v in self.require_dict.items():
            if not (isinstance(v, str) and v.startswith('$')):
                kwargs[k] = v
                continue
            result_file = os.path.join(
                '_output', self.task_root, v[2:], 'result.pkl')
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
    return schedule


GraphNode = namedtuple('GraphNode', ['node', 'property'])


class Graph():
    """a graph class to calculate task dependent relationships"""

    def __init__(self, nodes=None, edges=None):
        self.nodes: Set[str] = set()
        self.dag: Dict[str, List(str)] = defaultdict(list)
        self.in_degree: Dict[str, List(str)] = defaultdict(list)
        self.property: Dict[str:Dict[str, Any]] = defaultdict(dict)

        nodes: List[str] = nodes or []
        edges: List[Tuple[str, str]] = edges or []
        for n in nodes:
            self.add_node(n)
        for a, b in edges:
            self.add_edge(a, b)

    def add_node(self, node: str, p=None):
        p = dict()
        self.nodes.add(node)
        self.property[node] = p

    def add_edge(self, a, b):
        """
        add an edge from a to b
        """
        assert a in self.nodes
        assert b in self.nodes
        self.dag[a].append(b)
        self.in_degree[b].append(a)

    def remove_edge(self, a, b):
        """
        remove an edge from a to b
        """
        assert a in self.nodes
        assert b in self.nodes
        self.dag[a].remove(b)
        self.in_degree[b].remove(a)

    def _agg(self, map_func, reduce_func, node, result):
        assert node in self.nodes
        if node in result:
            return result[node]
        self_map = map_func(self.property[node])
        args = [
            self._agg(map_func, reduce_func, x, result)
            for x in self.in_degree[node]
        ]
        args.append(self_map)
        r = reduce_func(args)
        result[node] = r
        return r

    def aggragate(self, map_func, reduce_func, nodes=None, update=None):
        """
        aggregate the property of a node
        """
        result = {}
        if nodes is None:
            nodes = self.nodes
        elif isinstance(nodes, str):
            nodes = [nodes]
        for node in nodes:
            self._agg(map_func, reduce_func, node, result)
        # update property
        if isinstance(update, str):
            for k, v in result.items():
                self.property[k][update] = v
        # elif isinstance(update, callable):
        #     for k, v in result.items():
        #         update(self.property[k], v)
        else:
            raise Exception()
        return result

    def any(self, map_func):
        """
        return list of nodes if any func(n) is True for n in its child nodes
        """
        return self.aggragate(map_func, any)

    def all(self, map_func):
        """
        return list of nodes if all func(n) is True for n in its child nodes
        """
        return self.aggragate(map_func, all)

    @staticmethod
    def match_query(prop: Dict[str, Any], query: Dict[str, Any]) -> bool:
        for k, v in query.items():
            if k not in prop:
                return False
            p = prop[k]
            if isinstance(v, list):
                if p not in v:
                    return False
            else:
                if p != v:
                    return False
        return True

    def match_one(self, query: Dict[str, Any]):
        for task_name, prop in self.property.items():
            if self.match_query(prop, query):
                return task_name
        return None

    def match_all(self, query: Dict[str, Any]):
        matched = []
        for task_name, prop in self.property.items():
            if self.match_query(prop, query):
                matched.append(task_name)
        return matched

    def node_label(self, node: str):
        return '\n'.join([f'{k}:{v}' for k, v in self.property[node].items()])

    def to_pdf(self, path):
        """
        save graph to a pdf file using graphivz
        """
        from graphviz import Digraph
        dot = Digraph('G', format='pdf', filename=path,
                      graph_attr={'layout': 'dot'})
        dot.attr(rankdir='LR')
        color_map=dict(
            ready='orange',
            finished='lightgreen',
            pending='red',
            running='lightblue',
        )
        for node in self.nodes:
            color = color_map.get(self.property[node].get('status', 'None exist'), 'yellow')
            dot.node(
                node,
                label=f'{node}\n' + self.node_label(node),
                fillcolor=color,
                style="filled",
                color=color,
                )
        for a, bs in self.dag.items():
            for b in bs:
                dot.edge(a, b)
        dot.render(cleanup=False)

    def __item__(self, node):
        return GraphNode(self.dag[node], self.property[node])


class Scheduler():
    def __init__(self, tasks: Dict[str, TaskSpec], targets=None):
        self.g = Graph()
        for t in tasks.values():
            self.g.add_node(t.task_name)
            self.g.property[t.task_name]['dirty'] = t.dirty
            self.g.property[t.task_name]['virtual'] = t.virtual
        for t in tasks.values():
            for t2 in t.dependent_tasks:
                self.g.add_edge(t2, t.task_name)
        self.targets = targets
        self.tasks = tasks

        # update dirty map
        self.g.aggragate(
            lambda x: x['dirty'],
            any,
            nodes=self.targets,
            update='dirty'
        )
        self._update_status()

    def to_pdf(self, path):
        self.g.to_pdf(path)

    def _update_status(self):
        def map_func(prop):
            if prop['virtual']:
                return 'finished'
            if 'status' not in prop:
                if prop['dirty']:
                    return 'pending'
                else:
                    return 'finished'
            else:
                return prop['status']

        def reduce_func(props: List[Dict[str, Any]]):
            ready = True
            if props[-1] != 'pending':
                return props[-1]

            for s in props[:-1]:
                if s != 'finished':
                    ready = False
            if ready:
                return 'ready'
            return 'pending'

        self.g.aggragate(
            map_func,
            reduce_func,
            nodes=self.targets,
            update='status'
        )
        self.to_pdf('scheduler')
        # logger.info(pprint_str(self.g.property))

    def get_ready(self):
        t = self.g.match_one(dict(status='ready'))
        return t

    def set_status(self, task, status):
        self.g.property[task]['status'] = status
        self._update_status()

    def set_running(self, task):
        self.set_status(task, 'running')

    def set_finished(self, task):
        self.set_status(task, 'finished')

    def set_error(self, task):
        self.set_status(task, 'error')

    def run_once(self):
        processes: Dict[str, subprocess.Popen] = {}
        try:
            while True:
                finished = True
                while True:
                    ready_task = self.get_ready()
                    if ready_task is None:
                        # logger.info('no ready task aval')
                        break
                    logger.info('running task ' + ready_task)
                    self.set_running(ready_task)
                    env = dict(os.environ)
                    task = self.tasks[ready_task]
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
                    processes[ready_task] = process

                while len(processes) > 0:
                    finished = False
                    stop = False
                    for task, process in processes.items():
                        if process.poll() is not None:
                            logger.info('task ' + task + ' finished')
                            # processes.remove((process, task))
                            del processes[task]
                            self.set_finished(task)
                            # logger.info(self.get_ready())
                            if process.returncode != 0:
                                logger.error('task' + task + 'failed')
                                raise KeyboardInterrupt
                            stop = True
                            break
                    if stop:
                        break
                    time.sleep(0.1)
                if finished:
                    break
        except KeyboardInterrupt:
            logger.info('Keyboard interrupt')
            for p in processes.values():
                p.terminate()
                p.wait()

    def serve(self):
        pass


def run_task(tasks: Dict[str, TaskSpec], target=None, debug=False):
    scheduler = Scheduler(tasks, targets=target)
    scheduler.run_once()
    return

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
        try:
            for task in task_group:
                env = dict(os.environ)
                env['PYTHONPATH'] = os.path.abspath(task.task_root)
                if debug:
                    env['DEBUG'] = 'true'
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
                processes.append((process, task))
                if debug:
                    process.wait()
            for process, task in processes:
                ret_status = process.wait()
                if ret_status != 0:
                    logger.error(f"ERROR exit {ret_status} {task}")
                    raise KeyboardInterrupt
        except KeyboardInterrupt:
            logger.error('terminating processes')
            for p, _ in processes:
                p.terminate()
                p.wait()
            break


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
    try:
        task_info = yaml.safe_load(BytesIO(payload))
    except yaml.scanner.ScannerError as e:
        print(f"ERROR parse {root} {file}")
        raise e
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


def run_target(target: str, task=None, debug=False):
    logger.info(f'executing on: {target} ...')
    if debug:
        logger.info('IN DEBUG MODE')
    tasks = parse_target(target)
    # logger.debug(tasks)
    run_task(tasks, task, debug=debug)


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
