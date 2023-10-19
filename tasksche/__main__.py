from typing import List, Optional

import click

from .scheduler import run as _run


@click.group()
def cli():
    pass


@click.command()
@click.argument('tasks', nargs=-1)
@click.option('-r', '--run-id', 'run_id', default=None, )
@click.option('-s', '--storage-result', 'storage_result', default='file:default', )
@click.option('--storage-status', 'storage_status', default='mem:default', )
def run(
        tasks: List[str],
        run_id: Optional[str] = None,
        storage_result: str = 'file:default',
        storage_status: str = 'mem:default',
):
    _run(tasks, run_id, storage_result, storage_status)


cli.add_command(run)

if __name__ == '__main__':
    cli()
