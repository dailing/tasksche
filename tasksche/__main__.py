import argparse
import inspect
from typing import List, get_args, get_origin

from .run import clean_target, new_task, serve_target, serve_target2


def _parser():
    parser = argparse.ArgumentParser('RUN_RUN')
    _subparser = parser.add_subparsers(
        title='cmd', help='run root', required=True)
    return locals()


if __name__ == '__main__':
    _parsers = _parser()

    def _command(*_args, **kwargs):
        _subparser = _parsers['_subparser']

        def wrap(func):
            func_name = func.__name__
            parser: argparse.ArgumentParser = _subparser.add_parser(
                func_name, *_args, **kwargs)
            for k_name, par in inspect.signature(func).parameters.items():
                required = par.default is inspect.Parameter.empty
                default = par.default \
                    if par.default is not inspect.Parameter.empty \
                    else None
                argument_kwargs = {'type': par.annotation, 'default': default}
                if par.annotation is bool:
                    argument_kwargs['action'] = 'store_true'
                    del argument_kwargs['type']
                elif par.annotation is str:
                    pass
                elif get_origin(par.annotation) is list:
                    argument_kwargs['nargs'] = '+'
                    argument_kwargs['type'] = get_args(par.annotation)[0]
                else:
                    raise TypeError("Should Not Be here")
                if not required:
                    argument_kwargs['required'] = False
                    k_name = '-' + k_name
                parser.add_argument(k_name,
                                    **argument_kwargs)
            parser.set_defaults(__func=func)

        return wrap

    @_command()
    def serve(target: str, task: List[str] = None, addr: str = None):
        serve_target(target, task, addr)

    @_command()
    def run(target: str, task: List[str] = None, addr: str = None):
        serve_target(target, task, addr, exit=True)

    @_command()
    def clean(target: str):
        clean_target(target)

    @_command()
    def new(target: str, task: str):
        new_task(target, task)

    @_command()
    def sv2(task: List[str] = None, include_subdir:bool=True):
        serve_target2(task)

    # _args = _parsers['parser'].parse_args(['run', 'export'])
    args = _parsers['parser'].parse_args()

    call_args = args.__dict__.copy()
    del call_args['__func']

    args.__func(**call_args)
