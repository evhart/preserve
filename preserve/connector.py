import collections.abc
from abc import abstractmethod

from pydantic import BaseModel


# TODO Add typing
class Connector(BaseModel, collections.abc.MutableMapping):
    """Similar to Shelve code."""

    @staticmethod
    @abstractmethod
    def scheme() -> str:
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def from_uri(uri: str) -> "Connector":
        raise NotImplementedError()

    @abstractmethod
    def __iter__(self):
        raise NotImplementedError()

    @abstractmethod
    def __len__(self):
        raise NotImplementedError()

    @abstractmethod
    def __contains__(self, key):
        raise NotImplementedError()

    @abstractmethod
    def get(self, key, default=None):
        raise NotImplementedError()

    @abstractmethod
    def __getitem__(self, key):
        raise NotImplementedError()

    @abstractmethod
    def __setitem__(self, key, value):
        raise NotImplementedError()

    @abstractmethod
    def __delitem__(self, key):
        raise NotImplementedError()

    @abstractmethod
    def __enter__(self):
        raise NotImplementedError()

    @abstractmethod
    def __exit__(self, type, value, traceback):
        raise NotImplementedError()

    @abstractmethod
    def close(self):
        raise NotImplementedError()

    @abstractmethod
    def __del__(self):
        raise NotImplementedError()

    @abstractmethod
    def sync(self):
        raise NotImplementedError()

    def __setattr__(self, attr, value):
        if attr in self.__slots__:
            object.__setattr__(self, attr, value)
        else:
            super().__setattr__(attr, value)
