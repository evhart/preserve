from __future__ import annotations

import collections.abc
from typing import Any, cast
from urllib import parse

from preserve.connector import Connector, MultiConnector


class Memory(Connector):
    """Memory-based preserve connector.

    This class implements an in-memory key-value store that acts as a connector for the preserve framework.
    Keys are encoded using the specified `keyencoding` (default: "utf-8") and stored as bytes internally.
    Supports standard dictionary-like operations, context management, and resource cleanup.

    Class Attributes:
        keyencoding (str): Encoding used for keys (default: "utf-8").

    Methods:
        scheme() -> str:
            Returns the URI scheme for this connector ("memory").
        from_uri(uri: str) -> "Memory":
            Instantiates a Memory connector from a URI.
        __init__(*args, **kwargs):
            Initializes the in-memory dictionary.
        __iter__():
            Iterates over the decoded keys in the store.
        __len__():
            Returns the number of items in the store.
        __contains__(key):
            Checks if a key exists in the store.
        __getitem__(key):
            Retrieves the value for a key.
        __setitem__(key, value):
            Sets the value for a key.
        __delitem__(key):
            Deletes a key from the store.
        close():
            Closes the store and releases resources.
        sync():
            Placeholder for syncing data (no-op for memory backend).
    """

    keyencoding: str = "utf-8"

    __slots__ = ["_dict"]

    @staticmethod
    def scheme() -> str:
        """Returns the scheme identifier for the memory connector.

        Returns:
            str: The string "memory" representing the scheme.
        """
        return "memory"

    @staticmethod
    def from_uri(uri: str) -> "Memory":
        """Create a Memory instance from a URI string.

        Parses the given URI, validates that the scheme matches the Memory class,
        and constructs a Memory object using the query parameters from the URI.

        Args:
            uri (str): The URI string to parse and convert into a Memory instance.

        Returns:
            Memory: An instance of the Memory class initialized with parameters from the URI.

        Raises:
            ValueError: If the URI scheme does not match the expected Memory scheme.
        """
        p = parse.urlsplit(uri)
        if p.scheme != Memory.scheme():
            raise ValueError()

        return cast("Memory", Memory.model_validate(dict(parse.parse_qsl(p.query))))

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initializes the object, calling the parent constructor with any provided arguments."""
        super().__init__(*args, **kwargs)
        self._dict: dict[bytes, Any] | None = {}

    def __iter__(self) -> collections.abc.Generator[tuple[str, Any], None, None]:
        """Iterates over the items in the internal dictionary, yielding (key, value) tuples with decoded keys.

        Yields:
            tuple[str, Any]: Tuples of (decoded key, value) from the internal dictionary.

        Returns:
            None: If the internal dictionary is None.
        """
        if self._dict is None:
            return
        for k, v in self._dict.items():
            yield (k.decode(self.keyencoding), v)

    def __len__(self) -> int:
        """Return the number of items stored in the internal dictionary.

        Returns:
            int: The number of items if the internal dictionary exists, otherwise 0.
        """
        if self._dict is None:
            return 0
        return len(self._dict)

    def __contains__(self, key: str) -> bool:
        """Check if the given key exists in the internal dictionary.

        Args:
            key (str): The key to check for existence.

        Returns:
            bool: True if the encoded key exists in the dictionary, False otherwise.
        """
        return self._dict is not None and key.encode(self.keyencoding) in self._dict

    def __getitem__(self, key: str) -> Any:
        """Retrieve the value associated with the given key using dictionary-style access.

        Args:
            key: The key whose value is to be retrieved.

        Returns:
            The value associated with the specified key.

        Raises:
            KeyError: If the key is not found.
        """
        if self._dict is not None and key.encode(self.keyencoding) in self._dict:
            return self._dict[key.encode(self.keyencoding)]
        raise KeyError(key)

    def __setitem__(self, key: str, value: Any) -> None:
        """Sets the value associated with the given key in the memory store.

        Args:
            key (str): The key to set in the memory store.
            value (Any): The value to associate with the key.

        Raises:
            RuntimeError: If the memory store is closed.
        """
        if self._dict is not None:
            self._dict[key.encode(self.keyencoding)] = value
        else:
            raise RuntimeError("Memory store is closed.")

    def __delitem__(self, key: str) -> None:
        """Remove the item with the specified key from the memory store.

        Args:
            key (str): The key of the item to remove.

        Raises:
            KeyError: If the key does not exist in the store.
            RuntimeError: If the memory store is closed.
        """
        if self._dict is not None:
            del self._dict[key.encode(self.keyencoding)]
        else:
            raise RuntimeError("Memory store is closed.")

    def close(self) -> None:
        """Closes the memory connector by syncing any pending changes and releasing resources.

        If the internal dictionary is already None, the method returns immediately.
        Otherwise, it attempts to synchronize data and then sets the internal dictionary to None,
        suppressing any exceptions that may occur during cleanup.
        """
        if self._dict is None:
            return
        try:
            self.sync()
        finally:
            try:
                self._dict = None
            except Exception:
                pass

    def sync(self) -> None:
        """Synchronizes the in-memory data with the persistent storage or external source."""
        pass


class MultiMemory(MultiConnector):
    """Multi-collection in-memory connector.

    Each collection is an isolated :class:`Memory` store.  All data is lost
    when :meth:`close` is called or the object is garbage-collected.
    """

    __slots__ = ["_collections", "_collection_overrides"]

    @staticmethod
    def scheme() -> str:
        """Return ``"memory"``."""
        return "memory"

    @staticmethod
    def from_uri(uri: str) -> "MultiMemory":
        """Create a :class:`MultiMemory` from a ``memory://\u2026`` URI (parameters ignored)."""
        return MultiMemory()

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._collections: dict[str, Memory] = {}
        self._collection_overrides: dict[str, dict[str, Any]] = {}

    def _evict(self, collection: str) -> None:
        self._collections.pop(collection, None)

    def __getitem__(self, collection: str) -> "Memory":
        """Return (and cache) a :class:`Memory` for *collection*.

        Args:
            collection (str): An arbitrary collection identifier.

        Returns:
            Memory: A connector scoped to the named in-memory store.
        """
        if collection not in self._collections:
            overrides = self._collection_overrides.get(collection)
            if overrides is not None:
                kt, dkt, dvt = overrides["key_types"], overrides["default_key_type"], overrides["default_value_type"]
            else:
                kt, dkt, dvt = self.key_types, self.default_key_type, self.default_value_type
            self._collections[collection] = Memory(
                key_types=kt,
                default_key_type=dkt,
                default_value_type=dvt,
            )
        return self._collections[collection]

    def collections(self) -> list[str]:
        """Return the names of all collections that have been opened.

        Returns:
            list[str]: Collection names in insertion order.
        """
        return list(self._collections.keys())

    def close(self) -> None:
        """Close (and clear) all open sub-connectors."""
        for connector in self._collections.values():
            connector.close()
        self._collections.clear()
