import os
import pickle
from socket import timeout
from venv import logger
import requests


class Storage:
    def __init__(self, storage_path):
        self._path = storage_path
        if not os.path.exists(self._path):
            os.makedirs(self._path, exist_ok=True)
        assert os.path.exists(self._path)

    def set(self, key, value):
        assert isinstance(key, str)
        with open(os.path.join(self._path, key), "wb") as f:
            pickle.dump(value, f)

    def get(self, key):
        assert isinstance(key, str)
        with open(os.path.join(self._path, key), "rb") as f:
            return pickle.load(f)

    def __contains__(self, key):
        return os.path.exists(os.path.join(self._path, key))


class HttpStorage(Storage):
    def __init__(self, storage_path):
        self._path = storage_path

    def set(self, key, value):
        url = f"{self._path}/{key}"
        serialized_value = pickle.dumps(value)
        response = requests.post(url, data=serialized_value)
        response.raise_for_status()

    def get(self, key):
        url = f"{self._path}/{key}"
        try:
            response = requests.get(url, timeout=5)
        except requests.Timeout:
            logger.warning(f"Failed to get {key} from {self._path}")
            return None
        response.raise_for_status()
        return pickle.loads(response.content)

    def __contains__(self, key):
        url = f"{self._path}/{key}"
        response = requests.head(url)
        return response.status_code == 200


def storage_factory(
    storage_path: str,
):
    if storage_path.startswith("http://"):
        return HttpStorage(storage_path)
    return Storage(storage_path)
