import argparse
import sys
import logging
from abc import ABCMeta, abstractmethod

import collections.abc
from contextlib import contextmanager

from pydantic import BaseModel
from typing import List

from preserve import __version__

__author__ = "Grégoire Burel"
__copyright__ = "Grégoire Burel"
__license__ = "mit"

_logger = logging.getLogger(__name__)

# TODO Add typing
class Connector(BaseModel, collections.abc.MutableMapping):
    """
    Similar to Shelve code.
    """

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


from .utils import Singleton
import importlib
import pkgutil

# from preserve import connectors

import preserve.connectors as _connectors

import pkg_resources
import inspect


def _iter_namespace(ns_pkg):
    # Specifying the second argument (prefix) to iter_modules makes the
    # returned name an absolute name instead of a relative one. This allows
    # import_module to work without having to do additional modification to
    # the name.
    return pkgutil.iter_modules(ns_pkg.__path__, ns_pkg.__name__ + ".")


class Preserve(object, metaclass=Singleton):
    # import preserve.jars

    def __init__(self):
        self._connectors = {}

        # Discover new plugins (see https://packaging.python.org/guides/creating-and-discovering-plugins/):
        # TODO Check if it actually works
        for entry_point in pkg_resources.iter_entry_points("preserve.connectors"):
            print(entry_point.load())
            self._connectors[entry_point.name] = entry_point.load()

        # Register all the connectors that are defined in the package:
        for finder, name, ispkg in _iter_namespace(_connectors):
            module = importlib.import_module(name)
            for _, cls in inspect.getmembers(
                sys.modules[module.__name__],
                predicate=lambda o: inspect.isclass(o)
                and issubclass(o, Connector)
                and o != Connector,
            ):
                self._connectors[cls.scheme()] = cls

        # print(self._connectors)

    def open(self, format: str, **kwargs) -> Connector:
        if format not in self._connectors:
            raise ValueError(format)

        return self._connectors[format](**kwargs)

    def from_uri(self, uri: str) -> Connector:
        if not ":" in uri:
            raise ValueError(uri)

        f = uri.split(":", 1)[0]
        if f not in self._connectors:
            raise ValueError(f)

        return self._connectors[f].from_uri(uri)

    def register(self, format: str, connector):
        self._connectors[format] = connector

    def is_registerd(self, format: str, connector) -> bool:
        if format in self._connectors[format] and self._connectors[format] == connector:
            return True
        return False

    def connectors(self) -> List[Connector]:
        return self._connectors.values()


def open(format: str, **kwargs) -> Connector:
    return Preserve().open(format, **kwargs)


def from_uri(uri: str) -> Connector:
    return Preserve().from_uri(uri)


def connectors() -> List[Connector]:
    return Preserve().connectors()
