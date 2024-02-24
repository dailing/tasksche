"""
RUN TASKS
"""

import os
from typing import List, Optional

import click

from .functional import search_for_root
from .scheduler import run as _run


@click.group()
def cli():
    pass


@click.command()
@click.argument("tasks", nargs=-1)
@click.option(
    "-s",
    "--storage-path",
    "storage_path",
    default=None,
)
@click.option(
    "-w",
    "--work-dir1",
    default=None,
)
def run(
    tasks: List[str],
    storage_path: Optional[str] = None,
    work_dir: Optional[str] = None,
):
    if storage_path is None:
        root = search_for_root(tasks[0])
        storage_path = f"file:{os.path.abspath(root)}/__default"
    else:
        storage_path = os.path.abspath(storage_path)
        storage_path = f"file:{storage_path}"
    if work_dir is not None:
        work_dir = os.path.abspath(work_dir)
    else:
        work_dir = os.path.abspath("__work_dir")
    if not os.path.exists(work_dir):
        os.makedirs(work_dir, exist_ok=True)
    tasks = [os.path.abspath(task) for task in tasks]
    _run(tasks, storage_path, work_dir)


cli.add_command(run)

if __name__ == "__main__":
    """run the cli by default"""
    cli()
