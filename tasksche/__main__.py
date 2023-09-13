import argparse
import inspect
from typing import List, get_args, get_origin

from .run import serve_target


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
    def serve(task: List[str] = None):
        serve_target(task)

    @_command()
    def clean(task: List[str] = None):
        from .run import path_to_task_spec
        tasks = path_to_task_spec(task)
        for v in tasks.values():
            v.clear()

    # _args = _parsers['parser'].parse_args(['run', 'export'])
    args = _parsers['parser'].parse_args()

    call_args = args.__dict__.copy()
    del call_args['__func']

    args.__func(**call_args)
