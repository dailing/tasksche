import pickle
import sys
import importlib
from run import extract_anno
import time
from survivalreg.util.logger import get_logger
import os
import contextlib


logger = get_logger('Worker')


root, task = sys.argv[1:]

# logger.info(f'running {root} {task}, {os.path.abspath(".")}')
# env['PYTHONPATH']
# logger.info(f'running python path {os.environ.get("PYTHONPATH")}')
task_info = extract_anno(root, task)
logger.info(f'{task_info}, running {task_info.module_path}',)

kwargs = task_info.call_arguments

error = None
output = None
output_dir = os.path.join('_output', task_info.task_dir)
if not os.path.exists(output_dir):
    os.makedirs(output_dir)




# logger.info(mod)

with open(f'{output_dir}/std_out.txt', 'w') as fout, \
        open(f'{output_dir}/std_err.txt', 'w') as ferr:
    with contextlib.redirect_stderr(ferr), contextlib.redirect_stdout(fout):
        mod = importlib.import_module(task_info.module_path)
        mod.__dict__['work_dir'] = os.path.join(output_dir)
        if isinstance(kwargs, dict):
            output = mod.run(**kwargs)
        else:
            output = mod.run(*kwargs)

# pickle.dump(output, open(task_info.result_file, 'wb'))
task_info.dump_result(output)

# DUMP value
