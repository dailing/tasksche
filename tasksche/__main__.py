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
        storage_path = f"{os.getcwd()}/__default"
    else:
        storage_path = os.path.abspath(storage_path)
        storage_path = f"{storage_path}"
    assert isinstance(tasks, tuple)
    _run(list(tasks))


cli.add_command(run)

if __name__ == "__main__":
    cli()
