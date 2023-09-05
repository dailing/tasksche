from pprint import pprint
from typing import Dict
from nicegui import app, ui
import nicegui
import time
import threading
import os
from tasksche.run import TaskScheduler, TaskSpec
from python_mermaid.diagram import (
    MermaidDiagram,
    Node,
    Link
)
from nicegui.events import (
    ValueChangeEventArguments
)

root = os.path.abspath('.')
tree_ele = {
    root:
    {'id': root, 'label': root, 'children': [{'label': None}]}}
tree_show = [
    tree_ele[root],
]
tree_selected = []

columns = [
    {'name': 'task', 'label': 'task', 'field': 'task',
        'required': True, 'align': 'left'},
]
rows = [
    {}
]


def on_expand(e):
    print(e)
    if e.value is None:
        return
    exp_path = e.value[-1]
    ui.notify(exp_path)
    ele = tree_ele[exp_path]
    files = os.listdir(exp_path)
    ele['children'] = []
    for f in files:
        if f == '__pycache__':
            continue
        path = os.path.join(exp_path, f)
        c = {'id': path, 'label': f}
        if os.path.isdir(path):
            c['children'] = [{'label': None}]
        elif not path.endswith('.py'):
            continue
        elif f.startswith('.'):
            continue
        ele['children'].append(c)
        tree_ele[c['id']] = c


def on_tick(e):
    if e.value is None:
        return
    rows.clear()
    for k in e.value:
        if k is None:
            continue
        rows.append({'task': k})
    table.update()


def on_select(e):
    tree_instance = e.sender
    print(e)
    exp_path = e.value
    if os.path.isdir(exp_path):
        expanded = tree_instance._props.get('expanded', [])
        if exp_path not in expanded:
            expanded.append(exp_path)
            tree_instance._props['expanded'] = expanded
            tree_instance.update()
            e.value = expanded
            on_expand(e)
        else:
            expanded.remove(exp_path)
            tree_instance._props['expanded'] = expanded
    else:
        ticked = tree_instance._props.get('ticked', [])
        if e.value not in ticked:
            ticked.append(e.value)
        else:
            ticked.remove(e.value)
        tree_instance._props['ticked'] = ticked
        e.value = ticked
        on_tick(e)
    tree_instance._props['selected'] = None


ui.tree(
    tree_show,
    on_tick=on_tick,
    on_select=on_select,
    on_expand=on_expand,
    tick_strategy='leaf'
).props('dense')


table = ui.table(columns=columns, rows=rows, row_key='name').props('dense')

sche: TaskScheduler = None


def task_dict_to_dot(task_dict: Dict[str, TaskSpec]):
    """
    save graph to a pdf file using graphviz
    """

    nodes = {}
    # dot.attr(rankdir='LR')
    for k in task_dict.keys():
        nodes[k] = Node(k)
    edges = []
    for node, spec in task_dict.items():
        for b in spec.depend_task:
            edges.append(Link(nodes[b], nodes[node]))
    chart = MermaidDiagram(
        nodes=nodes.values(),
        links=edges,
        orientation="left to right",
    )
    return str(chart)


def start_running():
    global sche
    tasks = [x['task'] for x in rows]
    sche = TaskScheduler(tasks)
    src = task_dict_to_dot(sche.task_dict)
    mermaid.content = src


ui.button('Click me!', on_click=start_running)


mermaid = ui.mermaid('''
graph LR
meg["Meg"]
jo["Jo"]
beth["Beth"]
amy["Amy"]
robert_march["Robert March"]
robert_march ---> meg
robert_march ---> jo
robert_march ---> beth
robert_march ---> amy
''')


# stop_event = threading.Event()

# Define the thread function


# def update_age():
#     while not stop_event.is_set():
#         # Add 1 to the value of 'age'
#         data['age'] += 1
#         print(f"Updated age: {data['age']}")
#         time.sleep(1)


# # Create and start the thread
# thread = threading.Thread(target=update_age)
# thread.start()


def handle_shutdown():
    pass
    # stop_event.set()


# app.on_shutdown(handle_shutdown)
# app.on_disconnect(app.shutdown)


# ui.button('shutdown', on_click=app.shutdown)
ui.run(reload=True)
