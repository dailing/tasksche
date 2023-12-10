"""
RUN TASKS
"""
import os
from typing import List, Optional

import click

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
def run(
    tasks: List[str],
    storage_path: Optional[str] = None,
):
    if storage_path is None:
        storage_path = f"file:{os.getcwd()}/__default"
    else:
        storage_path = os.path.abspath(storage_path)
        storage_path = f"file:{storage_path}"
    _run(tasks, storage_path)


cli.add_command(run)

if __name__ == "__main__":
    """run the cli by default"""
    cli()
