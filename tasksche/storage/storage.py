import os
import pickle


class Storage:
    def __init__(self, storage_path):
        self.path = storage_path
        if not os.path.exists(self.path):
            os.makedirs(self.path, exist_ok=True)
        assert os.path.exists(self.path)

    def set(self, key, value):
        assert isinstance(key, str)
        with open(os.path.join(self.path, key), "wb") as f:
            pickle.dump(value, f)

    def get(self, key):
        assert isinstance(key, str)
        with open(os.path.join(self.path, key), "rb") as f:
            return pickle.load(f)

    def __contains__(self, key):
        return os.path.exists(os.path.join(self.path, key))


def storage_factory(
    storage_path: str,
):
    return Storage(storage_path)
