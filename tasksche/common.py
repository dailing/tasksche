import os
import pickle
from dataclasses import dataclass
from enum import Enum
from threading import RLock
from typing import Callable, Iterable
from typing import Dict

from .logger import Logger

logger = Logger()


class __INVALIDATE__:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(__INVALIDATE__, cls).__new__(cls)

        return cls._instance

    def __repr__(self) -> str:
        return '<INVALID>'


_INVALIDATE = __INVALIDATE__()


class Status(Enum):
    STATUS_READY = 'ready'
    STATUS_FINISHED = 'finished'
    STATUS_ERROR = 'error'
    STATUS_PENDING = 'pending'
    STATUS_RUNNING = 'running'


@dataclass
class ExecInfo:
    time: float
    depend_hash: Dict[str, str]


class DumpedType:
    def __init__(self, file_name, field_name, cache=True) -> None:
        self.file_name = file_name
        self.field_name = field_name
        self.cache = cache

    def __get__(self, instance, owner):
        """
        Get the value of the descriptor.

        This method is used to get the value of the descriptor when accessed
        through an instance or a class.

        Parameters:
        - instance: The instance of the class that the descriptor is accessed
            through.
        - owner: The class that owns the descriptor.

        Returns:
        - The value of the descriptor.
        """
        if instance is None:
            return self
        if self.cache and hasattr(instance, self.field_name):
            return getattr(instance, self.field_name)
        else:
            file_name = os.path.join(
                getattr(instance, 'output_dump_folder'), self.file_name)
            if not os.path.exists(file_name):
                return _INVALIDATE
            with open(file_name, 'rb') as f:
                result = pickle.load(f)
                if self.cache:
                    setattr(instance, self.field_name, result)
            return result

    def __set__(self, instance, value):
        """
        Set the value of the descriptor attribute.

        Args:
            instance: The instance of the class that the descriptor is being
                set on.
            value: The new value to set for the descriptor attribute.

        Returns:
            None
        """
        if self.cache:
            setattr(instance, self.field_name, value)
        dump_folder = getattr(instance, 'output_dump_folder')
        if not os.path.exists(dump_folder):
            os.makedirs(dump_folder, exist_ok=True)
        file_name = os.path.join(
            dump_folder, self.file_name)
        if value is _INVALIDATE:
            logger.debug(f'deleting {instance} file {file_name}')
            if os.path.exists(file_name):
                os.remove(file_name)
            if hasattr(instance, self.field_name):
                delattr(instance, self.field_name)
        else:
            with open(file_name, 'wb') as f:
                pickle.dump(value, f)


class CachedPropertyWithInvalidator:
    """
    Property decorator that caches the result of a function.
    When the property is assigned a new value, the cached value is invalidated,
    and all properties in the broadcaster are invalidated, the new value will
    be calculated on the next access.
    """

    def __init__(self, func):
        self.func = func
        self.attr_name = None
        self.broadcaster = None
        self.__doc__ = func.__doc__
        self.lock = RLock()

    def register_broadcaster(self, broadcaster: Callable[[], Iterable]):
        """
        Register a broadcaster function.

        :param broadcaster: A callable function that broadcasts messages.
        """
        assert self.broadcaster is None
        self.broadcaster = broadcaster
        return broadcaster

    def __set_name__(self, owner, name: str):
        if self.attr_name is None:
            self.attr_name = name
        elif name != self.attr_name:
            raise TypeError(
                "Cannot assign the to two different names"
                f"({self.attr_name!r} and {name!r})."
            )

    def __get__(self, instance, owner=None):
        if instance is None:
            return self
        if self.attr_name is None:
            raise TypeError(
                "Cannot use instance without __set_name__ on it.")
        try:
            cache = instance.__dict__
        # not all objects have __dict__ (e.g. class defines slots)
        except AttributeError:
            msg = (
                f"No '__dict__' attribute on {type(instance).__name__!r} "
                f"instance to cache {self.attr_name!r} property."
            )
            raise TypeError(msg) from None
        val = cache.get(self.attr_name, _INVALIDATE)
        if val is _INVALIDATE:
            with self.lock:
                # check if another thread filled cache while we awaited lock
                val = cache.get(self.attr_name, _INVALIDATE)
                if val is _INVALIDATE:
                    val = self.func(instance)
                    logger.debug(
                        f'updating value {instance}.{self.attr_name} to {val}')
                    try:
                        cache[self.attr_name] = val
                    except TypeError:
                        msg = (
                            f"The '__dict__' attribute on "
                            f"{type(instance).__name__!r} instance "
                            f"does not support item assignment for "
                            f"caching {self.attr_name!r} property."
                        )
                        raise TypeError(msg) from None
        return val

    def __set__(self, instance, value):
        # logger.debug(f'set value {value} to {instance}.{self.attr_name}')
        assert self.attr_name is not None
        with self.lock:
            if value == instance.__dict__.get(self.attr_name, _INVALIDATE):
                return
            instance.__dict__[self.attr_name] = value
            if self.broadcaster:
                # INVALIDATE ALL ATTR IN BROADCASTER
                for inst in self.broadcaster(instance):
                    setattr(inst, self.attr_name, _INVALIDATE)
