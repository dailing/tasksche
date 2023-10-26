import abc
import os
import pickle
from hashlib import md5
from typing import Any, Optional, List


class KVStorageBase(abc.ABC):
    def __init__(self, storage_path: str):
        self.storage_path = storage_path
        self._init_storage(storage_path)

    @abc.abstractmethod
    def _init_storage(self, name: str = ''):
        raise NotImplementedError

    @abc.abstractmethod
    def store(self, key: str, value: Any):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, key: str):
        raise NotImplementedError

    @abc.abstractmethod
    def get_hash(self, key: str) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def __contains__(self, key: str) -> bool:
        raise NotImplementedError


class FileKVStorageBase(KVStorageBase):

    def _init_storage(self, name: str = ''):
        self.root_path = f'/tmp/storage_{name}'
        if not os.path.exists(self.root_path):
            os.makedirs(self.root_path)

    def _to_file_path(self, key: str) -> str:
        key = key.replace('/', '_')
        return os.path.join(
            self.root_path, key
        )

    def store(self, key: str, value: Any):
        with open(self._to_file_path(key), 'wb') as f:
            f.write(pickle.dumps(value))

    def get(self, key: str):
        with open(self._to_file_path(key), 'rb') as f:
            return pickle.loads(f.read())

    def get_hash(self, key: str):
        return md5(open(self._to_file_path(key), 'rb').read()).hexdigest()

    def __contains__(self, key: str) -> bool:
        return os.path.exists(self._to_file_path(key))


class MemKVStorage(KVStorageBase):
    _instance = {}

    def _init_storage(self, name: str = ''):
        if name not in self._instance:
            self._instance[name] = {}
        self.storage = self._instance[name]

    def store(self, key: str, value):
        self.storage[key] = value

    def get(self, key: str):
        return self.storage[key]

    def get_hash(self, key: str):
        raise NotImplementedError

    def __contains__(self, key: str) -> bool:
        return key in self.storage


def storage_factory(storage_path: str = 'file:default') -> KVStorageBase:
    storage_type, storage_name = storage_path.split(':')
    if storage_type == 'file':
        return FileKVStorageBase(storage_name)
    elif storage_type == 'mem':
        return MemKVStorage(storage_name)
    else:
        raise ValueError(f'{storage_type} error')


class ResultStorage:
    @classmethod
    def _key_for(cls, *args):
        """
        Generates a key based on the given arguments.

        Args:
            *args: Variable number of arguments.

        Returns:
            str: A string representing the generated key.
        """
        return '.'.join([str(x) for x in args])

    @classmethod
    def key_for(cls, task_name: str, run_id: str, i_iter: Optional[List[int]]):
        if i_iter is None:
            i_iter = []
        return cls._key_for(task_name, run_id, *i_iter)

    def __init__(self, storage_path: str = 'file:default', **kwargs):
        self.storage = storage_factory(storage_path)
        self.storage_path = storage_path

    def store(
            self,
            task_name: str,
            run_id: str,
            i_iter: Optional[List[int]],
            value: Any = 0):
        self.storage.store(self.key_for(task_name, run_id, i_iter), value=value)

    def get(self, task_name: str, run_id: str, i_iter: Optional[List[int]]):
        return self.storage.get(self.key_for(task_name, run_id, i_iter))

    def get_hash(self, task_name: str, run_id: str, i_iter: Optional[List[int]]):
        return self.storage.get_hash(self.key_for(task_name, run_id, i_iter))

    def has(self, task_name: str, run_id: str, i_iter: Optional[List[int]]):
        return self.key_for(task_name, run_id, i_iter) in self.storage


class StatusStorage(ResultStorage):
    def __init__(self, storage_path: str = 'mem:default'):
        super().__init__(storage_path)

    def get_hash(self, task_name: str, run_id: str, i_iter: int = -1):
        raise NotImplementedError
