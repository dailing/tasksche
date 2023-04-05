import inspect
import os.path
from pprint import pprint


def path_for(p) -> str:
    directory = os.path.split(inspect.stack()[1].filename)[0]
    return os.path.join(directory, p)
