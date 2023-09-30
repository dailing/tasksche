import asyncio
import os
import typing
from asyncio import CancelledError
from typing import Callable, List, Optional
from typing import Dict

from fastapi.responses import RedirectResponse
from nicegui import Client, app, ui

from tasksche.logger import Logger
from tasksche.run import (
    Status, TaskScheduler, TaskSpec)

sche_storage: Dict[str, TaskScheduler] = {}
cb_storage = {}

logger = Logger()


def task_dict_to_dot(task_dict: Dict[str, TaskSpec]):
    """
    save graph to a pdf file using graphviz
    """
    lines = ['graph LR']
    color_map = {
        Status.STATUS_RUNNING: 'lightblue',
        Status.STATUS_FINISHED: 'lightgreen',
        Status.STATUS_ERROR: 'red',
        Status.STATUS_PENDING: 'yellow',
        Status.STATUS_READY: 'lightblue',
    }
    for k in task_dict.keys():
        lines.append(f'{k[1:]}["{k}"]')
    for node, spec in task_dict.items():
        for b in spec.depend_task:
            lines.append(f'{b[1:]} --> {node[1:]}')
    for k, v in task_dict.items():
        lines.append(f'style {k[1:]} fill:{color_map[v.status]}')
    code = '\n'.join(lines) + ';'
    return code


class CollectionTable(ui.table):
    def __init__(self, elements: Optional[List[str]] = None):
        self.root_selector_columns = [
            {
                'name': 'name',
                'label': 'Selected Items',
                'field': 'name',
                'required': True,
                'align': 'left'
            },
        ]
        self.root_selector_rows = []
        super().__init__(
            rows=self.root_selector_rows,
            columns=self.root_selector_columns)
        self.props('dense')
        self._on_click_func = self.remove
        self.on('rowClick', self._on_row_click)
        if elements is not None:
            for e in elements:
                self.add(e, update=False)
        self.update()

    def add(self, ele: str, update=True):
        for v in self.root_selector_rows:
            if v['name'] == ele:
                return
        self.root_selector_rows.append({'name': ele})
        if update:
            self.update()

    def extend(self, elements: List[str]):
        for e in elements:
            self.add(e, update=False)
        self.update()

    def set_elements(self, elements: List[str]):
        self.root_selector_rows.clear()
        for e in elements:
            self.add(e, update=False)
        self.update()

    def remove(self, ele: str):
        for v in self.root_selector_rows:
            if v['name'] == ele:
                self.root_selector_rows.remove(v)
                self.update()
                return

    def _on_row_click(self, e):
        ele, idx = e.args[1:]
        self._on_click_func(ele['name'])

    def on_row_click(self, func: Callable[[str], typing.Any]):
        self._on_click_func = func
        return self

    @property
    def items(self):
        return [x['name'] for x in self.root_selector_rows]


class PathSelection(ui.table):
    def __init__(self, root: Optional[str] = None):
        self.root_selector_columns = [
            {
                'name': 'name',
                'label': 'name',
                'field': 'name',
                'required': True,
                'align': 'left'
            },
        ]
        self.root_selector_rows = []
        if root is None or not os.path.exists(root):
            self.root = os.path.abspath('.')
        else:
            self.root = root
        super().__init__(
            rows=self.root_selector_rows,
            columns=self.root_selector_columns)
        self.props('dense')
        self._update(self.root)
        self.on('rowClick', self.row_click)
        self._on_add_file: Optional[Callable[[str], None]] = None

    def _update(self, root: str):
        if root.endswith('/') and len(root) > 1:
            root = root[:-1]
        self.root = root
        self.root_selector_rows.clear()
        self.root_selector_rows.append(
            {'name': '..'}
        )
        files = []
        for r in os.listdir(root):
            abs_p = os.path.join(root, r)
            if r.startswith('.'):
                continue
            if r.startswith('__output'):
                continue
            if os.path.isdir(abs_p):
                self.root_selector_rows.append({'name': abs_p + '/'})
            elif abs_p.endswith('.py'):
                files.append({'name': abs_p})
        self.root_selector_rows.extend(files)
        self._props['columns'][0]['label'] = self.root

    def row_click(self, e):
        ele, idx = e.args[1:]
        ele = ele['name']
        if ele == '..':
            self._update(os.path.dirname(self.root))
        elif os.path.isdir(ele):
            self._update(ele)
        elif self._on_add_file is not None:
            # ui.notify(ele)
            self._on_add_file(ele)
        self.update()

    def on_add_file(self, func: Callable[[str], None]):
        self._on_add_file = func
        return self


@ui.page('/')
async def root_page():
    logger.debug(app.storage.browser['id'])
    if app.storage.browser['id'] in sche_storage:
        return RedirectResponse('/task')
    path_selection = PathSelection(
        app.storage.user.get('_root', None))
    selected_path = CollectionTable(app.storage.user.get('selected', []))
    path_selection.on_add_file(selected_path.add)

    def confirm():
        ui.notify(selected_path.items)
        app.storage.user['selected'] = selected_path.items
        app.storage.user['_root'] = path_selection.root
        ui.open('/task')

    def clear():
        del app.storage.user['selected']
        del app.storage.user['_root']

    ui.button('O K', on_click=confirm, )
    ui.button('clear', on_click=clear)


@ui.page('/task')
async def task_page():
    ui.label(str(app.storage.user['selected']))
    selected_tasks = app.storage.user.get('selected', [])
    mermaid = ui.mermaid('')
    task_collection = CollectionTable()

    sche = sche_storage.get(app.storage.browser['id'], None)
    if sche is None:
        sche = TaskScheduler(selected_tasks)
        sche_storage[app.storage.browser['id']] = sche
        sche.run(once=False, daemon=True)
        logger.info('created Task Scheduler')
    else:
        logger.info('reusing Task Scheduler')

    async def cb():
        try:
            logger.info('running... event queue')
            while True:
                evt = await sche.sche_event_queue.get()
                logger.debug(f'got event {evt}')
                mermaid.content = task_dict_to_dot(sche.task_dict)
        except CancelledError:
            print('cancelled event queue')

    prev_task: asyncio.Task = cb_storage.get(app.storage.browser['id'], None)
    if prev_task is not None:
        prev_task.cancel()
        await prev_task.result()
    cb_storage[app.storage.browser['id']] = asyncio.create_task(cb())

    task_collection.set_elements(sche.task_dict.keys())
    task_collection.on_row_click(lambda t: asyncio.create_task(sche.clear(t, deep=False)))
    mermaid.content = task_dict_to_dot(sche.task_dict)

    async def on_stop():
        await sche.stop()
        del sche_storage[app.storage.browser['id']]
        ui.open('/')

    ui.button('stop', on_click=on_stop)

    def _update_graph():
        mermaid.content = task_dict_to_dot(sche.task_dict)

    ui.button('update', on_click=_update_graph)


def handle_shutdown():
    for k, v in sche_storage.items():
        print(f'shutdown {k}...')
        v.stop()
    print('shutdown...')


def handle_disconnect(client: Client):
    print('disconnect...')
    session_id = client.environ['asgi.scope']['session']['id']
    print(session_id, 'disconn')
    if session_id in cb_storage:
        cb_storage[session_id].cancel()
        del cb_storage[session_id]


def handle_connect(client: Client):
    print('connect', client.environ['asgi.scope']['session']['id'], '...')


app.on_shutdown(handle_shutdown)
app.on_disconnect(handle_disconnect)
app.on_connect(handle_connect)

# ui.button('shutdown', on_click=app.shutdown)
ui.run(reload=False, storage_secret='dfaiIUGHIUYgfiw21343!*(sadDSOAH)',
       show=False, dark=False)
