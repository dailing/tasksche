import abc
import os
import pickle
from hashlib import md5
from typing import Any, Tuple, List


class IterRecord(list):
    @classmethod
    def check_object_type(cls, __object: Any) -> Tuple[str, int]:
        if not isinstance(__object, tuple):
            __object = tuple(__object)
        if len(__object) != 2:
            raise ValueError("Object must have exactly two elements")
        if not isinstance(__object[0], str):
            raise ValueError("First element of object must be a string")
        if not isinstance(__object[1], int):
            raise ValueError("Second element of object must be an integer")
        return __object

    def append(self, __object: Any) -> None:
        __object = self.check_object_type(__object)
        return super().append(__object)

    def __setitem__(self, index: int, value: Any) -> None:
        self.check_object_type(value)
        super().__setitem__(index, value)

    def __repr__(self) -> str:
        return ".".join([f"{ele[0]}_{ele[1]}" for ele in self])

    def __getitem__(self, index: Any) -> Any:
        if isinstance(index, slice):
            return IterRecord(super().__getitem__(index))
        return super().__getitem__(index)

    def __add__(
        self, other: "IterRecord" | List[Any] | Tuple[str, int]
    ) -> "IterRecord":
        if (
            isinstance(other, tuple)
            and len(other) == 2
            and isinstance(other[0], str)
            and isinstance(other[1], int)
        ):
            other = IterRecord([other])
        if isinstance(other, list):
            for i in other:
                assert isinstance(i, tuple)
                assert len(i) == 2
            other = IterRecord(other)
        if not isinstance(other, IterRecord):
            raise TypeError(
                f"Unsupported operand type(s) for +: "
                f"'IterRecord' and '{type(other).__name__}'"
            )
        return IterRecord(super().__add__(other))

    def copy(self) -> "IterRecord":
        return IterRecord(self)

    def iter_numbers(self) -> List[int]:
        return [ele[1] for ele in self]


class KVStorageBase(abc.ABC):
    def __init__(self, storage_path: str):
        self.storage_path = storage_path
        self._init_storage(storage_path)

    @abc.abstractmethod
    def _init_storage(self, name: str = ""):
        raise NotImplementedError

    @abc.abstractmethod
    def store(self, key: str, *, value: Any):
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
    def _init_storage(self, name: str = ""):
        self.root_path = os.path.abspath(name)
        act_storage = os.path.join(self.root_path, "results")
        if not os.path.exists(act_storage):
            os.makedirs(act_storage, exist_ok=True)

    def _to_file_path(self, key: str) -> str:
        key = key.replace("/", "_")
        return os.path.join(self.root_path, "results", key)

    def store(self, key: str, value: Any):
        with open(self._to_file_path(key), "wb") as f:
            f.write(pickle.dumps(value))

    def get(self, key: str):
        with open(self._to_file_path(key), "rb") as f:
            return pickle.loads(f.read())

    def get_hash(self, key: str):
        return md5(open(self._to_file_path(key), "rb").read()).hexdigest()

    def __contains__(self, key: str) -> bool:
        return os.path.exists(self._to_file_path(key))


class MemKVStorage(KVStorageBase):
    _instance = {}

    def _init_storage(self, name: str = ""):
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


def storage_factory(
    storage_path: str,
):
    from ..new_sch import Storage

    return Storage(storage_path)
    # storage_type, storage_name = storage_path.split(":")
    # if storage_type == "file":
    #     return FileKVStorageBase(storage_name)
    # elif storage_type == "mem":
    #     return MemKVStorage(storage_name)
    # else:
    #     raise ValueError(f"{storage_type} error")
