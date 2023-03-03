from .run import *

def _parser():
    parser = argparse.ArgumentParser('RUNRUN')
    _subparser = parser.add_subparsers(
        title='cmd', help='run target', required=True)
    return locals()


if __name__ == '__main__':
    _parsers = _parser()

    def _command(*args, **kwargs):
        _subparser = _parsers['_subparser']

        def wrap(func):
            func_name = func.__name__
            parser: argparse.ArgumentParser = _subparser.add_parser(
                func_name, *args, **kwargs)
            for k_name, par in inspect.signature(func).parameters.items():
                required = par.default is inspect.Parameter.empty
                default = par.default if par.default is not inspect.Parameter.empty else None
                # logger.info(
                #     f'{func_name} {k_name}, {default} req: {required} {par.annotation}')
                if required:
                    parser.add_argument(k_name, default=default,
                                        type=par.annotation)
                else:
                    k_name = '-' + k_name
                    parser.add_argument(k_name, default=default, required=required,
                                        type=par.annotation)
            parser.set_defaults(__func=func)

        return wrap

    @_command()
    def run(target: str, task: str = None):
        run_target(target, task)

    @_command()
    def clean(target: str):
        clean_target(target)

    @_command()
    def new(target: str, task: str):
        new_task(target, task)

    # args = _parsers['parser'].parse_args(['run', 'export'])
    args = _parsers['parser'].parse_args()

    call_args = args.__dict__.copy()
    del call_args['__func']

    args.__func(**call_args)
