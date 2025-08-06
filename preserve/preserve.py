import importlib
import inspect
import logging
import pkgutil
import sys
from typing import Dict, List, Type

import pkg_resources

import preserve.connectors as _connectors
from preserve.connector import Connector

from .utils import Singleton

_logger = logging.getLogger(__name__)


def _iter_namespace(ns_pkg):
    """Iterates over all modules in the given namespace package.

    Args:
        ns_pkg: The namespace package (a module object) whose modules should be iterated.

    Returns:
        An iterator of ModuleInfo objects for all modules found in the namespace package,
        with their names as absolute names (including the namespace prefix).

    Notes:
        The function uses pkgutil.iter_modules with the namespace package's __path__ and
        __name__ to ensure that discovered module names are absolute, making them suitable
        for direct use with importlib.import_module.
    """
    return pkgutil.iter_modules(ns_pkg.__path__, ns_pkg.__name__ + ".")


class Preserve(object, metaclass=Singleton):
    """Preserve is a singleton class responsible for managing and discovering Connector plugins.

    This class provides mechanisms to:
    - Discover and register Connector plugins via Python entry points and package introspection.
    - Open connectors by format or URI.
    - Register new connectors at runtime.
    - Check if a connector is registered.
    - List all registered connectors.

    Attributes:
        _connectors (Dict[str, Type[Connector]]): A mapping from connector format names to Connector classes.

    Methods:
        open(format: str, **kwargs) -> Connector:
            Instantiate and return a Connector for the given format.

        from_uri(uri: str) -> Connector:
            Instantiate and return a Connector based on the URI scheme.

        register(format: str, connector):
            Register a new Connector class under the specified format.

        is_registerd(format: str, connector: Type[Connector]) -> bool:
            Check if a connector is registered under the given format.

        connectors() -> List[Type[Connector]]:
            Return a list of all registered Connector classes.
    """

    def __init__(self):
        """Initializes the connector registry by discovering and registering available connector plugins.

        This constructor performs the following steps:
        1. Initializes an empty dictionary to store connector classes, keyed by their scheme or entry point name.
        2. Discovers and loads external connector plugins registered under the "preserve.connectors" entry point using
            `pkg_resources`.
        3. Registers all connector classes defined within the package by iterating through the package namespace,
            importing modules, and identifying subclasses of `Connector` (excluding the base `Connector` class itself).
        """
        self._connectors: Dict[str, Type[Connector]] = {}

        # Discover new plugins
        # https://packaging.python.org/guides/creating-and-discovering-plugins/:
        for entry_point in pkg_resources.iter_entry_points("preserve.connectors"):
            print(entry_point.load())
            self._connectors[entry_point.name] = entry_point.load()

        # Register all the connectors that are defined in the package:
        for _, name, _ in _iter_namespace(_connectors):
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
        """Opens a connector for the specified format.

        Args:
            format (str): The format for which to open a connector.
            **kwargs: Additional keyword arguments to pass to the connector.

        Returns:
            Connector: An instance of the connector for the specified format.

        Raises:
            ValueError: If the specified format is not supported.
        """
        if format not in self._connectors:
            raise ValueError(format)

        return self._connectors[format](**kwargs)

    def from_uri(self, uri: str) -> Connector:
        """Creates and returns a Connector instance based on the given URI.

        The method parses the URI to determine the connector type, validates its existence,
        and delegates the creation of the connector to the appropriate handler.

        Args:
            uri (str): The URI string specifying the connector type and resource.

        Returns:
            Connector: An instance of the appropriate connector class.

        Raises:
            ValueError: If the URI does not contain a connector type or if the specified
                        connector type is not registered.
        """
        if ":" not in uri:
            raise ValueError(uri)

        f = uri.split(":", 1)[0]
        if f not in self._connectors:
            raise ValueError(f)

        return self._connectors[f].from_uri(uri)

    def register(self, format: str, connector):
        """Registers a connector for a specific format.

        Args:
            format (str): The format identifier to associate with the connector.
            connector: The connector object or function to handle the specified format.

        Raises:
            None

        Example:
            register('json', JsonConnector())
        """
        self._connectors[format] = connector

    def is_registerd(self, format: str, connector: Type[Connector]) -> bool:
        """Check if a specific connector is registered for a given format.

        Args:
            format (str): The format to check for registration.
            connector (Type[Connector]): The connector class to check.

        Returns:
            bool: True if the connector is registered for the given format, False otherwise.
        """
        if format in self._connectors and self._connectors[format] == connector:
            return True
        return False

    def connectors(self) -> List[Type[Connector]]:
        """Returns a list of all registered connector classes.

        Returns:
            List[Type[Connector]]: A list containing the types of all connectors currently registered in the instance.
        """
        return list(self._connectors.values())


def open(format: str, **kwargs) -> Connector:
    """Opens a connector using the specified format.

    Args:
        format (str): The format to use for opening the connector.
        **kwargs: Additional keyword arguments to pass to the underlying connector.

    Returns:
        Connector: An instance of the Connector for the specified format.

    Raises:
        ValueError: If the specified format is not supported.
    """
    return Preserve().open(format, **kwargs)


def from_uri(uri: str) -> Connector:
    """Creates and returns a Connector instance from the given URI.

    Args:
        uri (str): The URI string used to initialize the Connector.

    Returns:
        Connector: An instance of Connector initialized from the provided URI.
    """
    return Preserve().from_uri(uri)


def connectors() -> List[Type[Connector]]:
    """Returns a list of available Connector classes.

    This function instantiates a Preserve object and retrieves all registered Connector types.

    Returns:
        List[Type[Connector]]: A list of Connector class types.
    """
    return Preserve().connectors()
