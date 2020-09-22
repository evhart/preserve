import importlib
import inspect
import logging
import pkgutil
import sys
from abc import abstractmethod
from typing import Dict, List, Type

import pkg_resources
from pydantic import BaseModel

import preserve.connectors as _connectors
from preserve.connector import Connector

from .utils import Singleton


_logger = logging.getLogger(__name__)


def _iter_namespace(ns_pkg):
    # Specifying the second argument (prefix) to iter_modules makes the
    # returned name an absolute name instead of a relative one. This allows
    # import_module to work without having to do additional modification to
    # the name.
    return pkgutil.iter_modules(ns_pkg.__path__, ns_pkg.__name__ + ".")


class Preserve(object, metaclass=Singleton):
    # import preserve.jars

    def __init__(self):
        self._connectors: Dict[str, Type[Connector]] = {}

        # Discover new plugins
        # https://packaging.python.org/guides/creating-and-discovering-plugins/:
        # TODO Check if it actually works
        for entry_point in pkg_resources.iter_entry_points(
            "preserve.connectors"
        ):
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
        if ":" not in uri:
            raise ValueError(uri)

        f = uri.split(":", 1)[0]
        if f not in self._connectors:
            raise ValueError(f)

        return self._connectors[f].from_uri(uri)

    def register(self, format: str, connector):
        self._connectors[format] = connector

    def is_registerd(self, format: str, connector: Type[Connector]) -> bool:
        if (
            format in self._connectors
            and self._connectors[format] == connector
        ):
            return True
        return False

    def connectors(self) -> List[Type[Connector]]:
        return list(self._connectors.values())


def open(format: str, **kwargs) -> Connector:
    return Preserve().open(format, **kwargs)


def from_uri(uri: str) -> Connector:
    return Preserve().from_uri(uri)


def connectors() -> List[Type[Connector]]:
    return Preserve().connectors()
