# %%
import asyncio
import logging
import os
import os.path
import pickle
import subprocess
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from functools import cached_property
from hashlib import md5
from io import BytesIO, StringIO
from pprint import pprint
from typing import Any, Dict, List, Set, Tuple
from typing import Union

import networkx as nx
import socketio
import yaml
from watchfiles import awatch
from yaml.scanner import ScannerError


def get_logger(name: str, print_level=logging.DEBUG):
    formatter = logging.Formatter(
        fmt="%(levelname)6s "
            "[%(filename)15s:%(lineno)-3d %(asctime)s]"
            " %(message)s",
        datefmt='%H:%M:%S',
    )
    # time_now = datetime.datetime.now().strftime("%Y_%m_%d.%H_%M_%S")
    logger_obj = logging.getLogger(name)
    stream_handler = logging.StreamHandler(sys.stderr)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(print_level)
    # if not os.path.exists(f'/tmp//tmp/log/{time_now}/'):
    #     os.makedirs(f'/tmp/log/{time_now}/', exist_ok=True)
    # file_handler = logging.FileHandler(f'/tmp/log/{time_now}/{name}.log')
    # file_handler.setFormatter(formatter)
    # file_handler.setLevel(logging.DEBUG)
    logger_obj.addHandler(stream_handler)
    # logger.addHandler(file_handler)
    logger_obj.setLevel(logging.DEBUG)
    return logger_obj


logger = get_logger('runrun', print_level=logging.INFO)


def pprint_str(*args, **kwargs):
    sio = StringIO()
    pprint(*args, stream=sio, **kwargs)
    sio.seek(0)
    return sio.read()


@dataclass
class TaskSpec:
    task_root: str
    task_name: str
    require: Union[Dict[str, str], List[str], None] = None
    inherent: str = None
    virtual: bool = False
    depend: List[str] = None
    rerun: bool = False

    def __getitem__(self, item):
        return getattr(self, item)

    def to_json(self):
        return dict(
            task_name=self.task_name,
            dependent_tasks=self.dependent_tasks,
            inherent=self.inherent,
        )

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
        if isinstance(__o, TaskSpec):
            return self.task_name == __o.task_name
        return False

    def __gt__(self, __o: object) -> bool:
        if isinstance(__o, TaskSpec):
            return self.task_name > __o.task_name
        return False

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
        if not (os.path.exists(self.result_file) or
                os.path.exists(self.result_info)):
            return True
        try:
            os.remove(self.result_info)
            os.remove(self.result_file)
            return True
        except Exception as e:
            logger.info(e)
        # try:
        #     output_dir = os.path.join(self.task_dir, '_output')
        #     logger.info(f'removing {output_dir}')
        #     shutil.rmtree(output_dir, ignore_errors=True)
        #     # os.removedirs(os.path.join(self.task_dir, '_output'))
        # except Exception as e:
        #     logger.info(e)
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
            # logger.debug(dict(
            #   code_update=code_update, result_update=result_update))
            if code_update < result_update:
                return False
            else:
                code_hash, t = pickle.load(open(self.result_info, 'rb'))
                # logger.debug(f'check_hash {code_hash} {self.code_hash}')
                if code_hash == self.code_hash:
                    return False
                os.utime(self.result_file)
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
    if isinstance(require, list):
        it = enumerate(require)
    else:
        it = require.items()
    for k, v in it:
        require[k] = process_reference(v, anno)
        anno.require = require
    return anno


class Graph:
    """a graph class to calculate task dependent relationships"""

    def __init__(self, nodes=None, edges=None):
        self.nodes: Set[str] = set()
        self.dag: Dict[str, List[str]] = defaultdict(list)
        self.in_degree: Dict[str, List[str]] = defaultdict(list)
        self.property: Dict[str:Dict[str, Any]] = defaultdict(dict)

        nodes: List[str] = nodes or []
        edges: List[Tuple[str, str]] = edges or []
        for n in nodes:
            self.add_node(n)
        for a, b in edges:
            self.add_edge(a, b)

    def clear(self):
        self.nodes.clear()
        self.dag.clear()
        self.in_degree.clear()
        self.property.clear()

    def add_node(self, node: str, **kwargs):
        self.nodes.add(node)
        self.property[node] = kwargs

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
        save graph to a pdf file using graphviz
        """
        from graphviz import Digraph
        dot = Digraph('G', format='pdf', filename=path,
                      graph_attr={'layout': 'dot'})
        dot.attr(rankdir='LR')
        color_map = dict(
            ready='orange',
            finished='lightgreen',
            pending='red',
            running='lightblue',
        )
        for node in self.nodes:
            color = color_map.get(self.property[node].get(
                'status', 'None exist'), 'yellow')
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


class Scheduler:
    def __init__(self, root: str = None, targets=None, sock_addr=None):
        self._g = Graph()
        self.targets = targets
        self.tasks: Dict[str, TaskSpec] = {}
        self.root = root
        self.processes: Dict[str:asyncio.subprocess.Process] = {}
        self.coroutines_tasks:asyncio.Queue = None
        self.sock_addr = sock_addr
        self.sio: socketio.AsyncClient = socketio.AsyncClient()

        self.refresh()

    def refresh(self):
        """
            rescan files on disk and refresh everything
        """
        logger.debug('refresh')
        self._g.clear()
        assert len(self.processes) == 0
        # self.processes.clear()
        tasks = parse_target(self.root)
        self.tasks = tasks
        for t in self.tasks.values():
            self._g.add_node(t.task_name)
            self._g.property[t.task_name]['dirty'] = t.dirty
            self._g.property[t.task_name]['virtual'] = t.virtual
        for t in tasks.values():
            for t2 in t.dependent_tasks:
                self._g.add_edge(t2, t.task_name)
        self._g.aggragate(
            lambda x: x['dirty'],
            any,
            nodes=self.targets,
            update='dirty'
        )
        for task_name, prop in self._g.property.items():
            if prop['dirty']:
                self.tasks[task_name].clean()
        self._update_status()

    async def async_terminate_all_process(self):
        """
        terminate processed running
        terminate coroutines
        """
        logger.info('terminating processes')
        for v in self.processes.values():
            try:
                v.terminate()
            except ProcessLookupError:
                # process already ended
                pass
            await v.wait()
        self.processes.clear()
        # logger.info('terminating end')

    def to_pdf(self, path):
        self._g.to_pdf(path)

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

        self._g.aggragate(
            map_func,
            reduce_func,
            nodes=self.targets,
            update='status'
        )
        self.to_pdf('_output/scheduler')
        logger.debug(pprint_str(self._g.property))

    def get_ready(self):
        t = self._g.match_one(dict(status='ready'))
        return t

    def set_status(self, task, status):
        self._g.property[task]['status'] = status
        self._update_status()

    def set_running(self, task):
        self.set_status(task, 'running')

    def set_finished(self, task):
        self.set_status(task, 'finished')

    def set_error(self, task):
        self.set_status(task, 'error')

    # def run_once(self):
    #     processes: Dict[str, subprocess.Popen] = {}
    #     try:
    #         while True:
    #             finished = True
    #             while True:
    #                 ready_task = self.get_ready()
    #                 if ready_task is None:
    #                     break
    #                 logger.info('running task ' + ready_task)
    #                 self.set_running(ready_task)
    #                 env = dict(os.environ)
    #                 task = self.tasks[ready_task]
    #                 env['PYTHONPATH'] = os.path.abspath(task.task_root)
    #                 process = subprocess.Popen(
    #                     [
    #                         'python',
    #                         os.path.split(__file__)[0] + '/worker.py',
    #                         task.task_root,
    #                         task.code_file,
    #                     ],
    #                     stdout=sys.stdout,
    #                     stderr=sys.stderr,
    #                     env=env,
    #                     cwd=os.path.split(os.path.abspath(task.task_root))[0]
    #                 )
    #                 processes[ready_task] = process
    #
    #             while len(processes) > 0:
    #                 finished = False
    #                 stop = False
    #                 for task, process in processes.items():
    #                     if process.poll() is not None:
    #                         logger.info('task ' + task + ' finished')
    #                         # processes.remove((process, task))
    #                         del processes[task]
    #                         self.set_finished(task)
    #                         # logger.info(self.get_ready())
    #                         if process.returncode != 0:
    #                             logger.error('task' + task + 'failed')
    #                             raise KeyboardInterrupt
    #                         stop = True
    #                         break
    #                 if stop:
    #                     break
    #                 time.sleep(0.1)
    #             if finished:
    #                 break
    #     except KeyboardInterrupt:
    #         logger.info('Keyboard interrupt')
    #         for p in processes.values():
    #             p.terminate()
    #             p.wait()

    async def create_process(self, ready_task):
        logger.debug('running task ' + ready_task)
        env = dict(os.environ)
        task = self.tasks[ready_task]
        env['PYTHONPATH'] = os.path.abspath(task.task_root)
        process = await asyncio.create_subprocess_exec(
            'python',
            os.path.split(__file__)[0] + '/worker.py',
            task.task_root,
            task.code_file,
            stdout=sys.stdout,
            stderr=sys.stderr,
            env=env,
            cwd=os.path.split(os.path.abspath(task.task_root))[0]
        )
        return process

    def _init_new_job(self):
        """
        create a coroutine and make it propagate
        """
        logger.info('propagate tasks')
        asyncio.create_task(self.coroutines_tasks.put(
            asyncio.create_task(self.try_available_job())
        ))

    async def try_available_job(self):
        ready_job = self.get_ready()
        logger.debug(f'trying job {ready_job}')
        if ready_job is None:
            return
        logger.info(f'running {ready_job}')
        self.set_running(ready_job)
        if self.sio.connected:
            await self.sio.emit('graph_change')
        assert ready_job not in self.processes
        p = await self.create_process(ready_job)
        self.processes[ready_job] = p
        # call other jobs if avail
        self._init_new_job()
        ret_code = await p.wait()
        if ret_code == 0:
            self.set_finished(ready_job)
            self._init_new_job()
            logger.debug(f'task finished {ready_job}')
        else:
            self.set_error(ready_job)
            logger.info(f'error task {ready_job} {ret_code}')
        if self.sio.connected:
            await self.sio.emit('graph_change')
        # del self.processes[ready_job]
        logger.debug('exiting job')

    async def async_file_watcher_task(self):
        logger.info('file watcher started ...')
        async for changes in awatch(
                self.root,
                recursive=True,
                step=500,
        ):
            logger.debug(changes)
            await self.async_terminate_all_process()
            self.refresh()
            self._init_new_job()

    async def async_socket_on_get_tasks(self):
        elements = []
        color_map = dict(
            ready='yellow',
            finished='lightgreen',
            pending='orange',
            running='lightblue',
            error='red',
        )
        from networkx.drawing.nx_agraph import graphviz_layout
        logger.info('on_get_task')
        G = nx.DiGraph()
        for task_name, task in self.tasks.items():
            for dep in task.dependent_tasks:
                G.add_edge(dep, task_name)

        pos = graphviz_layout(G, prog='dot')

        result = {}
        for task_name, task in self.tasks.items():
            position_xy = pos.get(task_name, (0,0))
            prop = self._g.property.get(task_name, {})
            label = task_name+'<br>' + '<br>'.join(f'{a}:{b}' for a,b in prop.items())
            bg_color = color_map.get(prop.get('status', None), None)
            elements.append(dict(
                id=task_name,
                label=label,
                position=dict(y=position_xy[0] * 0.7, x=-position_xy[1] * 3),
                style={'backgroundColor': bg_color, },
                sourcePosition='right',
                targetPosition='left',
            ))
            for dep_task in task.dependent_tasks:
                elements.append(dict(
                    id=f'{dep_task}-{task_name}',
                    source=dep_task,
                    target=task_name,
                    markerEnd='arrowclosed',
                ))
            # payload = task.to_json()
            # payload['prop'] = self._g.property[task_name]
            # payload['pos'] = pos[task_name]
            # result[task_name] = payload
        return {e['id']:e for e in elements}

    async def async_socket_on_connect(self):
        logger.info('connect')
        result = await self.sio.call('join', data=dict(room=self.root))
        logger.info(result)

    async def async_socket_task(self):
        logger.info('socker task started ...')
        # connect to sockerIO server for event dispatch
        if self.sock_addr is not None:
            try:
                # self.sio = socketio.AsyncClient()
                self.sio.on('connect', self.async_socket_on_connect)
                await self.sio.connect(self.sock_addr, wait_timeout=3)

                self.sio.on('get_task', self.async_socket_on_get_tasks)
            except ConnectionError:
                # self.sio = None
                logger.info('connection error')

    async def async_serve_main(self):
        # file watcher task
        self.coroutines_tasks = asyncio.Queue()
        asyncio.create_task(self.async_file_watcher_task())
        asyncio.create_task(self.async_socket_task())

        # start running
        self._init_new_job()
        while True:
            t = await self.coroutines_tasks.get()
            if t is None:
                break
            await t

    def serve(self):
        asyncio.run(self.async_serve_main())


def extract_anno(root, file) -> TaskSpec:
    payload = bytearray()
    with open(file, 'rb') as f:
        f.readline()
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
    scheduler = Scheduler(target, task)
    scheduler.run_once()


def serve_target(target: str, task=None, addr=None):
    logger.info(f'serve on: {target} ...')
    scheduler = Scheduler(target, task, addr)
    scheduler.serve()


def clean_target(target: str):
    logger.info(f'cleaning : {target}')
    tasks = parse_target(target)
    for _, v in tasks.items():
        v.clean()


def new_task(target: str, task: str):
    logger.info(f'creating new task : {target} {task}')
    if not os.path.exists(target):
        print("ERROR no such root")
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
