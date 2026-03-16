from __future__ import annotations

import importlib.metadata
import logging

from preserve.connector import Connector, MultiConnector

_logger = logging.getLogger(__name__)

# Module-level registry — shared across all Preserve instances.
_REGISTRY: dict[str, type[Connector]] = {}
_DISCOVERED: bool = False

# Multi-connector registry
_MULTI_REGISTRY: dict[str, type[MultiConnector]] = {}
_MULTI_DISCOVERED: bool = False


def _discover() -> None:
    """Load all connectors registered under the ``preserve.connectors`` entry-point group.

    Runs once per process. Built-in connectors (memory, shelf, sqlite, mongodb) are
    registered via ``[project.entry-points."preserve.connectors"]`` in ``pyproject.toml``
    alongside any third-party connectors installed in the same environment, making both
    paths identical. Connectors whose ``is_available()`` returns ``False`` are silently
    skipped, which handles optional dependencies (e.g. pymongo not installed).
    """
    global _DISCOVERED
    if _DISCOVERED:
        return
    _DISCOVERED = True

    for ep in importlib.metadata.entry_points(group="preserve.connectors"):
        try:
            connector_cls = ep.load()
        except Exception:
            _logger.warning("Failed to load connector entry point %r.", ep.name, exc_info=True)
            continue

        if not connector_cls.is_available():
            _logger.debug("Skipping %r: optional dependencies not available.", ep.name)
            continue

        scheme = connector_cls.scheme()
        _REGISTRY[scheme] = connector_cls
        _logger.debug("Registered connector %r -> %s.", scheme, connector_cls.__qualname__)


def _reset() -> None:
    """Reset the registry, forcing re-discovery on the next access.

    Intended for use in tests only.
    """
    global _DISCOVERED, _MULTI_DISCOVERED
    _REGISTRY.clear()
    _DISCOVERED = False
    _MULTI_REGISTRY.clear()
    _MULTI_DISCOVERED = False


def _discover_multi() -> None:
    """Load all multi-connectors registered under ``preserve.multi_connectors``.

    Mirrors :func:`_discover` but uses the ``preserve.multi_connectors`` entry-point
    group and populates :data:`_MULTI_REGISTRY`.
    """
    global _MULTI_DISCOVERED
    if _MULTI_DISCOVERED:
        return
    _MULTI_DISCOVERED = True

    for ep in importlib.metadata.entry_points(group="preserve.multi_connectors"):
        try:
            connector_cls = ep.load()
        except Exception:
            _logger.warning("Failed to load multi-connector entry point %r.", ep.name, exc_info=True)
            continue

        if not connector_cls.is_available():
            _logger.debug("Skipping multi %r: optional dependencies not available.", ep.name)
            continue

        scheme = connector_cls.scheme()
        _MULTI_REGISTRY[scheme] = connector_cls
        _logger.debug("Registered multi-connector %r -> %s.", scheme, connector_cls.__qualname__)


class Preserve:
    """Registry and factory for Preserve connectors.

    All ``Preserve`` instances share the same module-level registry. Discovery via
    ``preserve.connectors`` entry points runs automatically on first access — there is
    no need to call anything explicitly.

    Third-party connectors can be installed by declaring an entry point in their own
    ``pyproject.toml``::

        [project.entry-points."preserve.connectors"]
        mybackend = "mypackage.mymodule:MyConnector"

    Methods:
        open(format, **kwargs): Instantiate a connector by scheme name.
        from_uri(uri): Instantiate a connector from a URI.
        register(format, connector): Register a connector class at runtime.
        is_registered(format, connector): Check registration.
        connectors(): List all registered connector classes.
    """

    def open(self, format: str, **kwargs) -> Connector:
        """Instantiate a connector for the given scheme name.

        Args:
            format (str): The scheme name (e.g. ``"sqlite"``).
            **kwargs: Passed directly to the connector constructor.

        Returns:
            Connector: A new connector instance.

        Raises:
            ValueError: If no connector is registered for *format*.
        """
        _discover()
        if format not in _REGISTRY:
            raise ValueError(f"Unknown connector format: {format!r}")
        return _REGISTRY[format](**kwargs)

    def from_uri(self, uri: str) -> Connector:
        """Instantiate a connector from a URI, dispatching on its scheme.

        Args:
            uri (str): A URI such as ``sqlite:///path/to/db`` or ``mongodb://host/db``.

        Returns:
            Connector: A new connector instance.

        Raises:
            ValueError: If *uri* has no scheme or the scheme is not registered.
        """
        _discover()
        if ":" not in uri:
            raise ValueError(f"Invalid URI (no scheme): {uri!r}")
        scheme = uri.split(":", 1)[0]
        if scheme not in _REGISTRY:
            raise ValueError(f"Unknown connector scheme: {scheme!r}")
        return _REGISTRY[scheme].from_uri(uri)

    def register(self, format: str, connector: type[Connector]) -> None:
        """Register a connector class under *format* at runtime.

        This is useful for connectors that are not distributed as installable packages.
        The registration is global and immediately visible to all ``Preserve`` instances.

        Args:
            format (str): The scheme name to register the connector under.
            connector (type[Connector]): A concrete subclass of ``Connector``.

        Raises:
            TypeError: If *connector* is not a subclass of ``Connector``.
        """
        if not (isinstance(connector, type) and issubclass(connector, Connector)):
            raise TypeError(f"connector must be a subclass of Connector, got {connector!r}")
        _REGISTRY[format] = connector

    def is_registered(self, format: str, connector: type[Connector]) -> bool:
        """Return ``True`` if *connector* is registered under *format*.

        Args:
            format (str): The scheme name to check.
            connector (type[Connector]): The connector class to compare against.

        Returns:
            bool: Whether *connector* is the registered class for *format*.
        """
        return _REGISTRY.get(format) is connector

    def connectors(self) -> list[type[Connector]]:
        """Return all registered connector classes.

        Returns:
            list[type[Connector]]: One entry per registered scheme.
        """
        _discover()
        return list(_REGISTRY.values())

    def open_multi(self, format: str, **kwargs) -> MultiConnector:
        """Instantiate a multi-connector for the given scheme name.

        Args:
            format (str): The scheme name (e.g. ``"sqlite"`` or ``"memory"``).
            **kwargs: Passed directly to the multi-connector constructor.

        Returns:
            MultiConnector: A new multi-connector instance.

        Raises:
            ValueError: If no multi-connector is registered for *format*.
        """
        _discover_multi()
        if format not in _MULTI_REGISTRY:
            raise ValueError(f"Unknown multi-connector format: {format!r}")
        return _MULTI_REGISTRY[format](**kwargs)

    def from_uri_multi(self, uri: str) -> MultiConnector:
        """Instantiate a multi-connector from a URI, dispatching on its scheme.

        Args:
            uri (str): A URI such as ``sqlite:///path/to/db``.

        Returns:
            MultiConnector: A new multi-connector instance.

        Raises:
            ValueError: If *uri* has no scheme or the scheme is not registered.
        """
        _discover_multi()
        if ":" not in uri:
            raise ValueError(f"Invalid URI (no scheme): {uri!r}")
        scheme = uri.split(":", 1)[0]
        if scheme not in _MULTI_REGISTRY:
            raise ValueError(f"Unknown multi-connector scheme: {scheme!r}")
        return _MULTI_REGISTRY[scheme].from_uri(uri)

    def multi_connectors(self) -> list[type[MultiConnector]]:
        """Return all registered multi-connector classes.

        Returns:
            list[type[MultiConnector]]: One entry per registered scheme.
        """
        _discover_multi()
        return list(_MULTI_REGISTRY.values())


def open(format: str, **kwargs) -> Connector:
    """Open a connector by scheme name. Shortcut for ``Preserve().open()``.

    Args:
        format (str): The scheme name.
        **kwargs: Passed to the connector constructor.

    Returns:
        Connector: A new connector instance.
    """
    return Preserve().open(format, **kwargs)


def from_uri(uri: str) -> Connector:
    """Open a connector from a URI. Shortcut for ``Preserve().from_uri()``.

    Args:
        uri (str): A URI identifying the backend and connection parameters.

    Returns:
        Connector: A new connector instance.
    """
    return Preserve().from_uri(uri)


def connectors() -> list[type[Connector]]:
    """Return all registered connector classes. Shortcut for ``Preserve().connectors()``.

    Returns:
        list[type[Connector]]: One entry per registered scheme.
    """
    return Preserve().connectors()


def open_multi(format: str, **kwargs) -> MultiConnector:
    """Open a multi-connector by scheme name. Shortcut for ``Preserve().open_multi()``.

    Args:
        format (str): The scheme name (e.g. ``"sqlite"`` or ``"memory"``).
        **kwargs: Passed to the multi-connector constructor.

    Returns:
        MultiConnector: A new multi-connector instance.
    """
    return Preserve().open_multi(format, **kwargs)


def from_uri_multi(uri: str) -> MultiConnector:
    """Open a multi-connector from a URI. Shortcut for ``Preserve().from_uri_multi()``.

    Args:
        uri (str): A URI identifying the backend and connection parameters.

    Returns:
        MultiConnector: A new multi-connector instance.
    """
    return Preserve().from_uri_multi(uri)


def multi_connectors() -> list[type[MultiConnector]]:
    """Return all registered multi-connector classes.

    Returns:
        list[type[MultiConnector]]: One entry per registered scheme.
    """
    return Preserve().multi_connectors()
