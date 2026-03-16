from __future__ import annotations

import collections.abc
import functools
import importlib
import logging
from abc import abstractmethod
from typing import Any

import pydantic_core
from pydantic import BaseModel, ConfigDict, TypeAdapter, model_validator

_logger = logging.getLogger(__name__)


@functools.lru_cache(maxsize=64)
def _get_type_adapter(tp: Any) -> TypeAdapter[Any]:
    """Return a cached ``TypeAdapter`` for *tp*.

    Constructing a ``TypeAdapter`` is not free, so we cache one per type.
    The cache is module-level and shared across all connector instances.
    """
    return TypeAdapter(tp)  # noqa: F821  (tp is a runtime type object)


class Connector(BaseModel, collections.abc.MutableMapping):
    """Abstract base class for key-value storage connectors for Preserve.

    Connectors behave as mutable mappings with additional lifecycle methods
    (:meth:`close`, :meth:`sync`, context-manager support).

    **Implementing a new connector** — subclasses must define:
    :meth:`scheme`, :meth:`from_uri`, :meth:`__iter__`, :meth:`__len__`,
    :meth:`__contains__`, :meth:`__getitem__`, :meth:`__setitem__`,
    :meth:`__delitem__`, :meth:`close`, and :meth:`sync`.

    :meth:`__enter__`, :meth:`__exit__`, and :meth:`__del__` are concrete here
    and delegate to :meth:`close` — subclasses do **not** need to override them.

    **Serialization** — :meth:`_serialize_value` and :meth:`_deserialize_value`
    are provided for connectors that need JSON-based storage.  They handle
    ``datetime``, ``UUID``, ``Decimal``, ``bytes``, ``Enum``, dataclasses, and
    ``BaseModel`` subclasses with full round-trip fidelity.  Connectors backed
    by richer storage (pickle, BSON, native Python) can ignore them.

    Attributes:
        key_types: Per-key type mapping for :meth:`get` coercion.
        default_key_type: Fallback type for keys absent from :attr:`key_types`.
        default_value_type: Uniform coercion type for all :meth:`get` calls.
    """

    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)

    key_types: dict[Any, Any] | None = None
    """Per-key type mapping used to coerce values returned by :meth:`get`.

    When set at connection time, :meth:`get` looks up the requested key in
    this mapping and coerces through the corresponding ``TypeAdapter``.
    Keys absent from the mapping fall back to :attr:`default_key_type`, then
    to no coercion.

    Mutually exclusive with :attr:`default_value_type` — setting both raises
    a ``ValueError`` at connection time.

    Example::

        import datetime, preserve

        db = preserve.open("sqlite", filename="f.db",
                           key_types={"score": float, "created_at": datetime.datetime},
                           default_key_type=str)
        db.get("score")        # float
        db.get("created_at")  # datetime
        db.get("name")        # str  (default_key_type)
    """

    default_key_type: Any = None
    """Fallback type for keys absent from :attr:`key_types`.

    Only meaningful alongside :attr:`key_types`.  Setting this field without
    :attr:`key_types` raises a ``ValueError`` at connection time.

    A per-call ``default_key_type`` kwarg on :meth:`get` overrides this.
    """

    default_value_type: Any = None
    """Uniform type used to coerce all values returned by :meth:`get`.

    When set at connection time, every :meth:`get` call that does not supply
    its own ``value_type`` kwarg will coerce through
    ``TypeAdapter(self.default_value_type)`` automatically.  Useful for
    homogeneous stores where all values share the same type.

    Mutually exclusive with :attr:`key_types` — setting both raises a
    ``ValueError`` at connection time.

    A per-call ``value_type`` kwarg always takes priority over this default.
    :meth:`__getitem__` (i.e. ``db[key]``) is never affected.

    Example::

        import datetime, preserve

        db = preserve.open("sqlite", filename="times.db",
                           default_value_type=datetime.datetime)
        db["now"] = datetime.datetime(2025, 3, 16, 12, 0)
        db.get("now")                               # datetime(2025, 3, 16, 12, 0)
        db.get("now", value_type=str)               # '2025-03-16T12:00:00'  (override)
        db["now"]                                   # '2025-03-16T12:00:00'  (unaffected)
    """

    @model_validator(mode="after")
    def _check_type_fields(self) -> "Connector":
        if self.key_types is not None and self.default_value_type is not None:
            raise ValueError("'key_types' and 'default_value_type' are mutually exclusive; use one or the other.")
        if self.default_key_type is not None and self.key_types is None:
            raise ValueError("'default_key_type' requires 'key_types' to be set.")
        return self

    @staticmethod
    @abstractmethod
    def scheme() -> str:
        """Returns the URI scheme as a string.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def from_uri(uri: str) -> "Connector":
        """Creates a Connector instance from the given URI.

        Args:
            uri (str): The URI string used to initialize the Connector.

        Returns:
            Connector: An instance of the Connector class.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError()

    @abstractmethod
    def __iter__(self) -> collections.abc.Generator[tuple[str, Any], None, None]:
        """Returns an iterator that yields key-value pairs from the object.

        Yields:
            tuple[str, Any]: Tuples containing a string key and its associated value.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError()

    @abstractmethod
    def __len__(self) -> int:
        """Return the number of items in the collection.

        Returns:
            int: The number of items.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError()

    @abstractmethod
    def __contains__(self, key: Any) -> bool:
        """Check if the specified key exists in the collection.

        Args:
            key (Any): The key to check for existence.

        Returns:
            bool: True if the key exists, False otherwise.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError()

    @abstractmethod
    def __getitem__(self, key: Any) -> Any:
        """Retrieve the value associated with the given key.

        Args:
            key (Any): The key to look up.

        Returns:
            Any: The value associated with the key.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError()

    @abstractmethod
    def __setitem__(self, key: Any, value: Any) -> None:
        """Sets the value associated with the specified key in the object.

        Args:
            key (Any): The key under which the value should be stored.
            value (Any): The value to associate with the key.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError()

    @abstractmethod
    def __delitem__(self, key: Any) -> None:
        """Remove the item with the specified key from the collection.

        Args:
            key (Any): The key of the item to be removed.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError()

    def __enter__(self) -> "Connector":
        """Enter the runtime context, returning *self*."""
        return self

    def __exit__(
        self,
        type: type[BaseException] | None,
        value: BaseException | None,
        traceback: Any | None,
    ) -> bool | None:
        """Exit the runtime context, calling :meth:`close`.

        Args:
            type (type[BaseException] | None): Exception type, or ``None``.
            value (BaseException | None): Exception instance, or ``None``.
            traceback (Any | None): Traceback object, or ``None``.

        Returns:
            bool | None: ``None`` — exceptions are not suppressed.
        """
        self.close()
        return None

    @abstractmethod
    def close(self) -> None:
        """Closes the connector and releases any associated resources.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError()

    def __del__(self) -> None:
        """Ensure :meth:`close` is called when the object is garbage-collected."""
        try:
            self.close()
        except Exception:
            pass

    @abstractmethod
    def sync(self) -> None:
        """Synchronizes data between the source and destination.

        This method should be implemented by subclasses to define the specific
        synchronization logic. Raises NotImplementedError if not overridden.
        """
        raise NotImplementedError()

    @staticmethod
    def is_available() -> bool:
        """Return True if this connector's optional dependencies are installed.

        Override in connectors that require optional packages (e.g. pymongo)
        to allow the plugin system to skip registration when they are absent.

        Returns:
            bool: True by default; False when required packages are missing.
        """
        return True

    @staticmethod
    def _serialize_value(value: Any) -> str:
        """Serialize *value* to a JSON string using pydantic-core.

        Handles stdlib types that plain ``json.dumps`` cannot: ``datetime``,
        ``date``, ``time``, ``UUID``, ``Decimal``, ``bytes``, ``Enum``,
        dataclasses, etc.  ``BaseModel`` instances are tagged with their full
        class path so that :meth:`_deserialize_value` can reconstruct them
        with full type fidelity.

        Args:
            value (Any): Any Python value to serialize.

        Returns:
            str: A JSON-encoded string representation of *value*.

        Raises:
            ValueError: If *value* cannot be serialized.
        """
        if isinstance(value, BaseModel):
            type_tag = f"{type(value).__module__}.{type(value).__qualname__}"
            payload = {
                "__pydantic_type__": type_tag,
                "__data__": value.model_dump(mode="json"),
            }
            return pydantic_core.to_json(payload).decode()
        try:
            return pydantic_core.to_json(value, serialize_unknown=True).decode()
        except Exception as exc:
            raise ValueError(f"Value is not serializable: {exc}") from exc

    @staticmethod
    def _deserialize_value(json_str: str) -> Any:
        """Deserialize a JSON string produced by :meth:`_serialize_value`.

        ``BaseModel`` instances tagged during serialization are reconstructed
        to their original class when the class is importable.  All other
        values are returned as plain Python objects (``dict``, ``list``,
        ``str``, ``int``, ``float``, ``bool``, ``None``).

        To coerce a retrieved value to a specific type at read time, pass
        ``value_type`` to :meth:`get`.

        Args:
            json_str (str): A JSON string previously produced by
                :meth:`_serialize_value`.

        Returns:
            Any: The deserialized Python object.
        """
        obj = pydantic_core.from_json(json_str)
        if isinstance(obj, dict) and "__pydantic_type__" in obj and "__data__" in obj:
            type_path: str = obj["__pydantic_type__"]
            module_path, _, cls_name = type_path.rpartition(".")
            try:
                module = importlib.import_module(module_path)
                cls = getattr(module, cls_name)
                if isinstance(cls, type) and issubclass(cls, BaseModel):
                    return cls.model_validate(obj["__data__"])
            except Exception:
                _logger.debug(
                    "Could not reconstruct Pydantic type %r; returning raw data.",
                    type_path,
                    exc_info=True,
                )
        return obj

    def get(  # type: ignore[override]
        self,
        key: Any,
        default: Any = None,
        *,
        value_type: Any = None,
        key_types: dict[Any, Any] | None = None,
        default_key_type: Any = None,
    ) -> Any:
        """Return the value for *key*, optionally coerced to a type.

        Coercion priority:

        1. Per-call ``value_type`` — highest priority; ``key_types`` and
           ``default_key_type`` are silently ignored when this is set.  A
           warning is logged if they are passed alongside it.
        2. Effective ``key_types`` (per-call overrides instance) — looks up
           *key* in the mapping.
        3. Effective ``default_key_type`` (per-call overrides instance) —
           fallback for keys absent from ``key_types``.
        4. Instance :attr:`default_value_type` — uniform fallback (only when
           ``key_types`` mode is not active).
        5. No coercion — raw deserialized value returned as-is.

        Any type accepted by ``pydantic.TypeAdapter`` is valid: builtin types,
        ``datetime.datetime``, ``uuid.UUID``, ``decimal.Decimal``, any
        ``BaseModel`` subclass, generic aliases such as ``list[MyModel]``, etc.

        :meth:`__getitem__` (``db[key]``) is never affected by any of these
        settings.

        Args:
            key (Any): The key to look up.
            default (Any, optional): Returned when *key* is absent. Defaults
                to ``None``.
            value_type (Any, optional): Per-call type override; ignores all
                other type parameters when set.
            key_types (dict[Any, Any] | None, optional): Per-call per-key type
                mapping; overrides the instance :attr:`key_types` when set.
            default_key_type (Any, optional): Per-call fallback for keys absent
                from *key_types*; overrides the instance :attr:`default_key_type`
                when set.

        Returns:
            Any: The value coerced to the resolved type, or *default*.

        Example::

            import datetime, preserve

            # Uniform coercion via default_value_type:
            db = preserve.open("sqlite", filename="times.db",
                               default_value_type=datetime.datetime)
            db["now"] = datetime.datetime(2025, 3, 16, 12, 0)
            db.get("now")                               # datetime(2025, 3, 16, 12, 0)
            db.get("now", value_type=str)               # '2025-03-16T12:00:00'  (override)
            db["now"]                                   # '2025-03-16T12:00:00'  (unaffected)

            # Per-key coercion via key_types:
            db2 = preserve.open("sqlite", filename="f.db",
                                key_types={"score": float, "ts": datetime.datetime},
                                default_key_type=str)
            db2.get("score")                            # float
            db2.get("ts")                               # datetime
            db2.get("name")                             # str  (default_key_type)
            db2.get("score", value_type=int)            # int  (value_type overrides)
        """
        try:
            obj = self[key]
        except KeyError:
            return default

        # value_type wins unconditionally; warn if redundant parameters were passed
        if value_type is not None:
            if key_types is not None or default_key_type is not None:
                _logger.warning("get(): 'value_type' is set; 'key_types' and 'default_key_type' are ignored.")
            return _get_type_adapter(value_type).validate_python(obj)

        # Resolve effective key_types / default_key_type (per-call > instance)
        eff_key_types = key_types if key_types is not None else self.key_types
        eff_default_key_type = default_key_type if default_key_type is not None else self.default_key_type

        if eff_key_types is not None:
            tp = eff_key_types.get(key, eff_default_key_type)
            if tp is None:
                return obj
            return _get_type_adapter(tp).validate_python(obj)

        # Uniform fallback
        if self.default_value_type is not None:
            return _get_type_adapter(self.default_value_type).validate_python(obj)

        return obj

    def __setattr__(self, attr: str, value: Any) -> None:
        """Override of the __setattr__ method to control attribute assignment.

        If the attribute name is defined in __slots__, assigns the value directly using the base object's __setattr__.
        Otherwise, delegates attribute assignment to the superclass's __setattr__ method.

        Args:
            attr (str): The name of the attribute to set.
            value (Any): The value to assign to the attribute.
        """
        if attr in self.__slots__:
            object.__setattr__(self, attr, value)
        else:
            super().__setattr__(attr, value)


class MultiConnector(BaseModel):
    """Abstract base class for multi-collection connectors.

    A ``MultiConnector`` exposes a ``db["collection"]`` interface where each
    collection name maps to a full :class:`Connector` scoped to that namespace.
    The underlying storage (file, database, directory) is shared across all
    collections opened from the same ``MultiConnector`` instance.

    **Implementing a new multi-connector** — subclasses must define:
    :meth:`scheme`, :meth:`from_uri`, :meth:`__getitem__`, :meth:`collections`,
    and :meth:`close`.

    :meth:`__enter__`, :meth:`__exit__`, and :meth:`__del__` are concrete here
    and delegate to :meth:`close`.

    Example::

        db = preserve.open_multi("sqlite", filename="app.db")
        db["users"]["alice"] = {"age": 30}
        db["products"]["widget"] = {"price": 9.99}
        db["users"]["alice"]   # → {"age": 30}
    """

    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)

    key_types: dict[Any, Any] | None = None
    """Per-key type mapping forwarded to every sub-connector created by :meth:`__getitem__`.

    Mutually exclusive with :attr:`default_value_type`.
    """

    default_key_type: Any = None
    """Fallback type for keys absent from :attr:`key_types`.  Only meaningful
    alongside :attr:`key_types`.
    """

    default_value_type: Any = None
    """Uniform coercion type forwarded to every sub-connector created by
    :meth:`__getitem__`.  Mutually exclusive with :attr:`key_types`.
    """

    @model_validator(mode="after")
    def _check_type_fields(self) -> "MultiConnector":
        if self.key_types is not None and self.default_value_type is not None:
            raise ValueError("'key_types' and 'default_value_type' are mutually exclusive; use one or the other.")
        if self.default_key_type is not None and self.key_types is None:
            raise ValueError("'default_key_type' requires 'key_types' to be set.")
        return self

    @staticmethod
    @abstractmethod
    def scheme() -> str:
        """Return the URI scheme handled by this multi-connector."""
        raise NotImplementedError()

    @staticmethod
    @abstractmethod
    def from_uri(uri: str) -> "MultiConnector":
        """Create a ``MultiConnector`` instance from a URI."""
        raise NotImplementedError()

    @abstractmethod
    def __getitem__(self, collection: str) -> "Connector":
        """Return a :class:`Connector` scoped to *collection*, creating it if needed."""
        raise NotImplementedError()

    @abstractmethod
    def collections(self) -> list[str]:
        """Return the names of all known collections in this store."""
        raise NotImplementedError()

    @abstractmethod
    def close(self) -> None:
        """Close all open sub-connectors and release any shared resources."""
        raise NotImplementedError()

    @abstractmethod
    def _evict(self, collection: str) -> None:
        """Remove *collection* from the sub-connector cache, if present.

        Called by :meth:`open` before constructing a fresh sub-connector with
        updated coercion settings.
        """
        raise NotImplementedError()

    def open(
        self,
        collection: str,
        *,
        key_types: dict[Any, Any] | None = None,
        default_key_type: Any = None,
        default_value_type: Any = None,
    ) -> "Connector":
        """Return a :class:`Connector` for *collection* with per-collection coercion settings.

        Collection-level settings override the instance-level defaults set at
        construction time.  Once :meth:`open` is called for a collection, the
        chosen settings are sticky — subsequent ``db[collection]`` access will
        also use them.

        If no type arguments are given, this is equivalent to ``db[collection]``.

        Args:
            collection (str): The collection name.
            key_types (dict[Any, Any] | None, optional): Per-key type mapping for
                this collection; mutually exclusive with *default_value_type*.
            default_key_type (Any, optional): Fallback type for keys absent from
                *key_types*; requires *key_types* to be set.
            default_value_type (Any, optional): Uniform coercion type for all
                :meth:`~Connector.get` calls on this collection; mutually exclusive
                with *key_types*.

        Returns:
            Connector: A sub-connector scoped to *collection* with the given
            coercion settings applied.

        Raises:
            ValueError: If *key_types* and *default_value_type* are both set, or
                if *default_key_type* is set without *key_types*.

        Example::

            db = preserve.open_multi("sqlite", filename="app.db")
            scores = db.open("scores", default_value_type=float)
            tags   = db.open("tags", key_types={"color": str}, default_key_type=str)
            raw    = db.open("raw")  # same as db["raw"]
        """
        if key_types is not None and default_value_type is not None:
            raise ValueError("'key_types' and 'default_value_type' are mutually exclusive; use one or the other.")
        if default_key_type is not None and key_types is None:
            raise ValueError("'default_key_type' requires 'key_types' to be set.")
        if key_types is not None or default_key_type is not None or default_value_type is not None:
            self._collection_overrides[collection] = {
                "key_types": key_types,
                "default_key_type": default_key_type,
                "default_value_type": default_value_type,
            }
            self._evict(collection)
        return self[collection]

    @staticmethod
    def is_available() -> bool:
        """Return ``True`` if this connector's optional dependencies are installed."""
        return True

    def __enter__(self) -> "MultiConnector":
        """Enter the runtime context, returning *self*."""
        return self

    def __exit__(
        self,
        type: type[BaseException] | None,
        value: BaseException | None,
        traceback: Any | None,
    ) -> None:
        """Exit the runtime context, calling :meth:`close`."""
        self.close()

    def __del__(self) -> None:
        """Ensure :meth:`close` is called when the object is garbage-collected."""
        try:
            self.close()
        except Exception:
            pass

    def __setattr__(self, attr: str, value: Any) -> None:
        if attr in self.__slots__:
            object.__setattr__(self, attr, value)
        else:
            super().__setattr__(attr, value)
