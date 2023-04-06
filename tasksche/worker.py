import pickle
import sys
import importlib
from run import extract_anno
import time
from run import get_logger
import os
import contextlib
import pdb
import os


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
output = None
output_dir = os.path.join('_output', task_info.task_dir)
if not os.path.exists(output_dir):
    os.makedirs(output_dir)


# logger.info(mod)
if debug:
    logger.info('IN DEBUG MODE')
try:
    with open(f'{output_dir}/std_out_{os.getpid()}.txt', 'w') as fout, \
            open(f'{output_dir}/std_err_{os.getpid()}.txt', 'w') as ferr:
        # if debug:
        #     fout = sys.stdout
        #     ferr = sys.stderr
        with contextlib.redirect_stderr(ferr), contextlib.redirect_stdout(fout):
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
except KeyboardInterrupt:
    logger.info(f'{task_info.task_name} exiting...')
    # sys.exit(-1)
# pickle.dump(output, open(task_info.result_file, 'wb'))

# DUMP value
