# %%
import asyncio
import logging
import os
import os.path
import pickle
import signal
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


logger = get_logger('runner', print_level=logging.INFO)


def pprint_str(*args, **kwargs):
    sio = StringIO()
    pprint(*args, stream=sio, **kwargs)
    sio.seek(0)
    return sio.read()


@dataclass
class _TaskSpec:
    task_root: str
    task_name: str
    require: Union[Dict[str, str], List[str], None] = None
    inherent: str = None
    virtual: bool = False
    rerun: bool = False
    export: bool = False
    # expire to run the task if following tasks ends
    expire: int = -1
    end_time = -1
    remove = False


class TaskSpec(_TaskSpec):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.code_hash = self._get_hash()

        require = self.get('require', {})
        if require is None:
            require = {}
        if isinstance(require, list):
            it = enumerate(require)
        else:
            it = require.items()
        for k, v in it:
            require[k] = process_reference(v, self)
            self.require = require

    def _get_hash(self) -> str:
        m = md5()
        # logger.info(str((self, self.code_file, self.task_dir, self.task_name)))
        m.update(open(self.code_file, 'rb').read())
        for f in self.depend_files:
            m.update(open(f, 'rb').read())
        return m.hexdigest()

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

    @cached_property
    def depend_files(self):
        result = []
        if self.inherent is not None:
            result.append(os.path.join(self.task_root, self.inherent_task[1:], 'task.py'))
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

    # @cached_property
    # def code_hash(self) -> str:

    @property
    def code_update_time(self) -> float:
        code_update = os.stat(self.code_file).st_mtime
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
        if self.expire > 0 and (self.end_time + self.expire < time.time()):
            return True
        code_update = self.code_update_time
        try:
            result_update = os.stat(self.result_file).st_mtime
            if code_update < result_update:
                return False
            else:
                code_hash, t = pickle.load(open(self.result_info, 'rb'))
                if code_hash == self.code_hash:
                    os.utime(self.result_file)
                    return False
        except FileNotFoundError:
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
                args[k] = v
            return args
        return kwargs

    def dump_result(self, result):
        pickle.dump(result, open(self.result_file, 'wb'))
        pickle.dump((self.code_hash, time.time()),
                    open(self.result_info, 'wb'))


class TaskSpecEndTask(TaskSpec):
    task_name = '_end'

    def __init__(self, dependent_tasks):
        super().__init__(task_name=self.task_name, task_root=None)
        self._dependent_tasks = dependent_tasks

    @property
    def dirty(self) -> bool:
        return True

    def _get_hash(self) -> str:
        return '0' * 32

    @property
    def dependent_tasks(self):
        return self._dependent_tasks


def process_path(task_name, path):
    if not path.startswith('/'):
        path = os.path.join(task_name, path)
    path_filtered = []
    for sub_path in path.split('/'):
        if sub_path == '.':
            continue
        if sub_path == '..':
            path_filtered.pop()
            continue
        path_filtered.append(sub_path)
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


class QUERY_EXIST:
    def __init__(self):
        raise Exception


class Graph:
    """a graph class to calculate task dependent relationships"""

    def __init__(self, nodes=None, edges=None):
        self.nodes: Set[str] = set()
        self.dag: Dict[str, Set[str]] = defaultdict(set)
        self.in_degree: Dict[str, Set[str]] = defaultdict(set)
        self.property: Dict[str, Dict[str, Any]] = defaultdict(dict)

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
        for node in (a, b):
            if node not in self.nodes:
                logger.warning(f'node {node} not in nodes')
        self.dag[a].add(b)
        self.in_degree[b].add(a)

    def remove_edge(self, a, b):
        """
        remove an edge from a to b
        """
        assert a in self.nodes
        assert b in self.nodes
        self.dag[a].remove(b)
        self.in_degree[b].remove(a)

    def remove_node(self, n):
        """
        remove node n from the graph
        """
        assert n in self.nodes
        for t in self.dag[n]:
            self.in_degree[t].remove(n)
        if n in self.dag:
            del self.dag[n]
        del self.property[n]
        self.nodes.remove(n)

    def _agg(self, map_func, reduce_func, node, result):
        if node not in self.nodes:
            logger.error(f'node {node} not in nodes')
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

    def aggregate(self, map_func, reduce_func, nodes=None, update=None):
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
        elif update is None:
            pass
        else:
            logger.error(f'error update value {update}')
            raise Exception()
        return result

    def any(self, map_func):
        """
        return list of nodes if any func(n) is True for n in its child nodes
        """
        return self.aggregate(map_func, any)

    def all(self, map_func):
        """
        return list of nodes if all func(n) is True for n in its child nodes
        """
        return self.aggregate(map_func, all)

    @staticmethod
    def match_query(prop: Dict[str, Any], query: Dict[str, Any]) -> bool:
        for k, v in query.items():
            if k not in prop:
                return False
            if v is QUERY_EXIST:
                return True
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

    def check_all(self, query: Dict[str, Any]):
        for task_name, prop in self.property.items():
            if not self.match_query(prop, query):
                return False
        return True

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
        self.processes: Dict[str, Tuple[asyncio.subprocess.Process, asyncio.Task]] = {}
        self.sock_addr = sock_addr
        self.sio: socketio.AsyncClient = socketio.AsyncClient()
        self.stop_new_job = False

        self.finished = False

        self.processes_lock: Union[asyncio.Lock, None] = None
        self.new_task_lock: Union[asyncio.Lock, None] = None
        tasks = parse_target(self.root)
        self.add_tasks(list(tasks.values()))

    def add_tasks(self, tasks: List[TaskSpec]):
        if '_end' in self._g.nodes:
            self._g.remove_node('_end')
        out_degree = defaultdict(lambda: 0)
        for t in tasks:
            assert t.task_name not in self.tasks
            self.tasks[t.task_name] = t
        targets = self.targets
        if targets is None:
            targets = self.tasks.keys()
        for t in targets:
            for dep_task in self.tasks[t].dependent_tasks:
                out_degree[dep_task] += 1
        sink_nodes = []
        for t in targets:
            if t not in out_degree and not self.tasks[t].virtual:
                sink_nodes.append(t)
        task_end = TaskSpecEndTask(sink_nodes)
        self.tasks[task_end.task_name] = task_end
        all_task = tasks + [task_end]
        for t in all_task:
            self._g.add_node(t.task_name)
            self._g.property[t.task_name]['dirty'] = t.dirty
            self._g.property[t.task_name]['virtual'] = t.virtual
            self._g.property[t.task_name]['name'] = t.task_name
            if t.expire >= 0:
                self._g.property[t.task_name]['expire'] = t.expire
        for t in self.tasks.values():
            for t2 in t.dependent_tasks:
                self._g.add_edge(t2, t.task_name)
        self._g.aggregate(
            lambda x: x.get('dirty', True),
            any,
            nodes=self.targets,
            update='dirty'
        )

        self._update_status()

    def to_pdf(self, path):
        self._g.to_pdf(path)

    def _update_status(self):
        def map_func(prop):
            if prop.get('virtual', False):
                return 'virtual'
            if 'status' not in prop:
                if prop['dirty']:
                    return 'pending'
                else:
                    return 'finished'
            else:
                return prop['status']

        def reduce_func(props: List[str]):
            ready = True
            # if props[-1] != 'pending':
            #     return props[-1]
            if 'virtual' in props:
                return 'virtual'

            for s in props[:-1]:
                if s != 'finished':
                    ready = False
            if ready and (props[-1] == 'pending'):
                return 'ready'
            if 'error' in props[:-1]:
                return 'error'
            if 'pending' in props[:-1]:
                return 'pending'
            if 'running' in props[:-1]:
                return 'pending'
            if 'ready' in props[:-1]:
                return 'pending'
            return props[-1]

        self._g.aggregate(
            map_func,
            reduce_func,
            nodes=self.targets,
            update='status'
        )
        # self.to_pdf('_output/scheduler')
        logger.debug(pprint_str(self._g.property))

    def get_ready(self):
        t = self._g.match_one(dict(status='ready'))
        return t

    def set_status(self, task: str, status: Union[str, None]):
        assert isinstance(task, str)
        if status is None and 'status' in self._g.property[task]:
            del self._g.property[task]['status']
        if isinstance(status, str):
            self._g.property[task]['status'] = status
        self._update_status()

    def set_running(self, task):
        self.set_status(task, 'running')

    def set_finished(self, task):
        self.set_status(task, 'finished')

    def set_error(self, task):
        self.set_status(task, 'error')

    async def create_process(self, ready_task: str):
        logger.debug('running task ' + ready_task)
        env = dict(os.environ)
        task = self.tasks.get(ready_task, None)
        if task is None:
            return None
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
        logger.debug('propagate tasks')
        asyncio.create_task(self.try_available_job())

    async def try_available_job(self):
        if self.new_task_lock.locked():
            logger.info('stopped')
            return
        async with self.new_task_lock, self.processes_lock:
            ready_job = self.get_ready()
            if ready_job == TaskSpecEndTask.task_name:
                # end of tasks
                asyncio.create_task(self.on_finished())
                return
            if ready_job in self.processes:
                logger.error(f'job already running {ready_job}')
                raise Exception()
            logger.debug(f'trying job {ready_job}')
            if ready_job is None:
                return
            logger.info(f'running {ready_job}')
            self.set_running(ready_job)
            p = await self.create_process(ready_job)
            # call other jobs if avail
            self.processes[ready_job] = (p, asyncio.current_task())
            self._init_new_job()
        if self.sio.connected:
            await self.sio.emit('graph_change')
        ret_code = await p.wait()
        async with self.processes_lock:
            if ret_code == 0:
                self.tasks[ready_job].end_time = time.time()
                self.set_finished(ready_job)
                self._init_new_job()
                logger.info(f'task finished {ready_job}')
            else:
                # del self.processes[ready_job]
                self.set_error(ready_job)
                logger.info(f'error task {ready_job} {ret_code}')
            del self.processes[ready_job]
            # check if all jobs finished
            # self.check_finish()
        if self.sio.connected:
            await self.sio.emit('graph_change')

    async def on_finished(self):
        async with self.processes_lock, self.new_task_lock:
            logger.info('finished all')
            expire_task_names = self._g.match_all(dict(expire=QUERY_EXIST))
            if len(expire_task_names) == 0:
                return
            expire_tasks = [self.tasks[i] for i in expire_task_names]
            expire_tasks = list(filter(lambda x: not x.virtual, expire_tasks))
            if len(expire_tasks) == 0:
                return
            now = time.time()
            expire_times = [t.expire - (now - t.end_time) for t in expire_tasks]
            task = expire_tasks[expire_times.index(min(expire_times))]
            logger.info(f'counting down for task {task} {task.expire}s')
            while not task.dirty:
                await asyncio.sleep(task.expire - (time.time() - task.end_time))
            logger.info('trigger expire task')
            self.set_status(task.task_name, 'ready')
            self.finished = False
            self._update_status()
        self._init_new_job()

    async def async_file_watcher_task(self):
        logger.info('file watcher started ...')
        logger.info(self.root)
        async for changes in awatch(
                self.root,
                recursive=True,
                step=500,
        ):
            logger.debug(changes)
            async with self.new_task_lock:
                tasks = parse_target(self.root)

                removed_keys = set(self.tasks.keys()) - set(tasks.keys())
                added_keys = set(tasks.keys()) - set(self.tasks.keys())
                changed_hash_keys = set()

                for key in set(tasks.keys()) & set(self.tasks.keys()):
                    if tasks[key].code_hash != self.tasks[key].code_hash:
                        changed_hash_keys.add(key)

                # terminate all tasks if needed
                async with self.processes_lock:
                    agg_res = self._g.aggregate(
                        lambda x: x['name'] in (changed_hash_keys | removed_keys),
                        any
                    )
                    process_to_end = []
                    for task_name, changed in agg_res.items():
                        if changed:
                            process_to_end.append(task_name)
                logger.info(process_to_end)
                for task_name in process_to_end:
                    if task_name not in self.processes:
                        continue
                    p, cr = self.processes[task_name]
                    p.send_signal(signal.SIGINT)
                    # p.terminate()
                    await cr

                # update status, and other things here
                async with self.processes_lock:
                    # remove nodes
                    for r in (removed_keys | changed_hash_keys):
                        del self.tasks[r]
                        self._g.remove_node(r)
                    # add new Nodes
                    task_to_add = [tasks[i] for i in (changed_hash_keys | added_keys)]
                    self.add_tasks(task_to_add)
                    for p in process_to_end:
                        if p in self.tasks and p in self._g.property and 'status' in self._g.property[p]:
                            self.set_status(p, 'pending')
                    self._update_status()
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
        logger.debug('on_get_task')
        g = nx.DiGraph()
        for task_name, task in self.tasks.items():
            for dep in task.dependent_tasks:
                g.add_edge(dep, task_name)

        pos = graphviz_layout(g, prog='dot')

        for task_name, task in self.tasks.items():
            position_xy = pos.get(task_name, (0, 0))
            prop = self._g.property.get(task_name, {})
            label = (task_name + '<br>' +
                     '<br>'.join(f'{a}:{b}' for a, b in prop.items()))
            bg_color = color_map.get(prop.get('status', None), None)
            if task_name == '_end':
                bg_color = 'gray'
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
        return {e['id']: e for e in elements}

    async def async_socket_on_connect(self):
        logger.info('connect')
        result = await self.sio.call('join', data=dict(room=self.root))
        logger.info(f'connected, {result}')

    async def async_socket_task(self):
        logger.info('socket task started ...')
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
        self.processes_lock = asyncio.Lock()
        self.new_task_lock = asyncio.Lock()
        asyncio.create_task(self.async_file_watcher_task())
        asyncio.create_task(self.async_socket_task())
        # self.check_finish()
        # start running
        self._init_new_job()
        while True:
            await asyncio.sleep(10)

    def serve(self):
        try:
            asyncio.run(self.async_serve_main())
        except KeyboardInterrupt:
            print("END")
            raise Exception()


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
    if task_info is None:
        task_info = {}
    task_info['task_root'] = root
    task_info['task_name'] = task_name
    tki = TaskSpec(**task_info)
    return tki


def parse_target(target: str) -> Dict[str, TaskSpec]:
    tasks: Dict[str, TaskSpec] = {}
    for dir_path, _, file_names in os.walk(target):
        for file in file_names:
            if file == 'task.py':
                file_path = os.path.join(dir_path, file)
                try:
                    task_info = extract_anno(target, file_path)
                    tasks[task_info.task_name] = task_info
                except Exception as e:
                    logger.error(f'Error parsing node {target}, {file_path}, {e}',
                                 exc_info=e, stack_info=True)
                    logger.error(str(os.stat(file_path)))
                    logger.error(os.path.abspath('.'))
                    # TODO send message about this error node
                    raise e
    return tasks


def serve_target(target: str, task=None, addr=None):
    if task is not None and isinstance(task, str):
        task = [task]
    logger.info(f'serve on: {target}, tasks: {task} ...')
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
