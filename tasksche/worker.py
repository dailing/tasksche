import pickle
import sys
import importlib
import os

try:
    from tasksche.run import extract_anno, get_logger
except ImportError:
    sys.path.append(os.path.split(os.path.split(__file__)[0])[0])
    from tasksche.run import extract_anno, get_logger

import os
import contextlib
import pdb
import os
import shutil

logger = get_logger('Worker')


root, task = sys.argv[1:]

# logger.info(f'running {root} {task}, {os.path.abspath(".")}')
# env['PYTHONPATH']
# logger.info(f'running python path {os.environ.get("PYTHONPATH")}')
debug = (os.environ.get("DEBUG", None) == 'true')
task_info = extract_anno(root, task)
logger.info(f'running {task_info}',)

kwargs = task_info.call_arguments

error = None
output_dir = os.path.join('_output', task_info.task_dir)
std_out_dir = os.path.join(output_dir, 'stdout')
os.makedirs(output_dir, exist_ok=True)
os.makedirs(std_out_dir, exist_ok=True)


# logger.info(mod)
error = False
if debug:
    logger.info('IN DEBUG MODE')
try:
    std_out_file = f'{std_out_dir}/std_out_{os.getpid()}.txt'
    std_rd_file = f'{output_dir}/std_out.txt'
    with (open(std_out_file, 'w') as fout,
            # open(f'{output_dir}/std_err_{os.getpid()}.txt', 'w') as ferr
          ):
        if os.path.exists(std_rd_file):
            os.remove(std_rd_file)
        os.link(std_out_file, std_rd_file, )
        # if debug:
        #     fout = sys.stdout
        #     ferr = sys.stderr
        with (
            # contextlib.redirect_stderr(ferr),
                contextlib.redirect_stdout(fout)):
            mod = importlib.import_module(task_info.module_path)
            mod.__dict__['work_dir'] = os.path.join(output_dir)
            if isinstance(kwargs, dict):
                args, kwargs = [], kwargs
                # output = mod.run(**kwargs)
            else:
                args, kwargs = kwargs, {}
            if debug:
                pdb.run('output = mod.run(*args, **kwargs)')
            else:
                output = mod.run(*args, **kwargs)
                task_info.dump_result(output)
    os.utime(std_rd_file)
except KeyboardInterrupt:
    logger.info(f'{task_info.task_name} exiting...')
    error = True
except Exception as e:
    logger.error(e, exc_info=e, stack_info=True)
    error = True
if error:
    sys.exit(-1)

if task_info.export:
    export_dir = os.path.join(output_dir, 'export')
    os.makedirs(export_dir, exist_ok=True)
    logger.info(f'export task results to: {export_dir}')
    pickle.dump((args, kwargs), open(
        os.path.join(export_dir, 'input.pkl'), 'wb'))
    code = open(task_info.code_file, 'r').read()
    code = '\n'.join((
        code,
        'if __name__ == "__main__":',
        '\twork_dir="."',
        '\timport pickle',
        '\targs, kwargs = pickle.load(open("input.pkl", "rb"))',
        '\trun(*args, **kwargs)',
    ))
    with open(f'{export_dir}/task.py', 'w') as f:
        f.write(code)
    # print(code)
