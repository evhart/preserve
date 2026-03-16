from __future__ import annotations

import collections.abc
import os
import shelve
from typing import Any, cast
from urllib import parse

from preserve.connector import Connector, MultiConnector


class Shelf(Connector):
    """Shelf connector for persistent dictionary-like storage using Python's shelve module.

    Attributes:
        filename (str): Path to the shelf database file.
        protocol (Optional[str]): Pickle protocol version to use (as string or None).
        writeback (bool): Whether to cache entries for write-back on sync/close.
        keyencoding (str): Encoding to use for keys (default: "utf-8").

    Slots:
        _shelf: Internal shelve.DbfilenameShelf instance.

    Methods:
        scheme() -> str:
            Returns the URI scheme for this connector ("shelf").

        __init__(*args, **kwargs):
            Initializes the Shelf connector and opens the shelf database.

        from_uri(uri: str) -> "Shelf":
            Instantiates a Shelf object from a URI.

        __iter__() -> Generator[tuple[str, Any], None, None]:
            Iterates over (key, value) pairs in the shelf.

        __len__() -> int:
            Returns the number of items in the shelf.

        __contains__(key: str) -> bool:
            Checks if a key exists in the shelf.

        __getitem__(key: str) -> object:
            Retrieves a value by key.

        __setitem__(key: str, value: object) -> None:
            Sets a value for a given key.

        __delitem__(key: str) -> None:
            Deletes a key-value pair.

        close() -> None:
            Closes the shelf database.

        sync() -> None:
            Synchronizes the in-memory state with the persistent storage.
    """

    filename: str
    protocol: str | None = None
    writeback: bool = False
    keyencoding: str = "utf-8"

    __slots__ = ["_shelf"]

    @staticmethod
    def scheme() -> str:
        """Returns the scheme identifier for the shelf connector.

        Returns:
            str: The string "shelf" representing the scheme.
        """
        return "shelf"

    # example: shelf://filename?protocol=?,writeback=?, keyencoding="utf-8"
    def __init__(self, *args, **kwargs) -> None:
        """Initializes the shelf connector.

        Args:
            *args: Variable length argument list passed to the superclass.
            **kwargs: Arbitrary keyword arguments passed to the superclass.

        Attributes:
            _shelf (shelve.DbfilenameShelf): The underlying shelf database instance,
                initialized with the specified filename, flag, protocol, and writeback settings.
        """
        super().__init__(*args, **kwargs)
        self._shelf = shelve.DbfilenameShelf(
            self.filename,
            flag="c",
            protocol=int(self.protocol) if self.protocol is not None else None,
            writeback=self.writeback,
        )

    @staticmethod
    def from_uri(uri: str) -> "Shelf":
        """Create a Shelf instance from a URI string.

        Parses the given URI, validates the scheme, extracts parameters, and constructs
        a Shelf object using the parsed values.

        Args:
            uri (str): The URI string to parse and convert into a Shelf instance.

        Returns:
            Shelf: An instance of Shelf initialized with parameters extracted from the URI.

        Raises:
            ValueError: If the URI scheme does not match the expected Shelf scheme.
        """
        p = parse.urlsplit(uri)
        if p.scheme != Shelf.scheme():
            raise ValueError()
        params = {}
        params["filename"] = f"{p.netloc}/{p.path}" if p.path != "" else p.netloc
        params.update(dict(parse.parse_qsl(p.query)))

        return cast("Shelf", Shelf.model_validate(params))

    def __iter__(self) -> collections.abc.Generator[tuple[str, Any], None, None]:
        """Iterates over the items in the shelf.

        Yields:
            tuple[str, Any]: A tuple containing the key and its corresponding value from the shelf.
        """
        for key in self._shelf:
            yield (key, self._shelf[key])

    def __len__(self) -> int:
        """Return the number of items stored in the shelf.

        Returns:
            int: The number of items in the shelf.
        """
        return self._shelf.__len__()

    def __contains__(self, key: str) -> bool:
        """Check if the specified key exists in the shelf.

        Args:
            key (str): The key to check for existence in the shelf.

        Returns:
            bool: True if the key exists in the shelf, False otherwise.
        """
        return self._shelf.__contains__(key)

    def __getitem__(self, key: str) -> object:
        """Retrieve the value associated with the given key from the shelf.

        Args:
            key (str): The key whose corresponding value is to be retrieved.

        Returns:
            object: The value associated with the specified key.

        Raises:
            KeyError: If the key is not found in the shelf.
        """
        return self._shelf.__getitem__(key)

    def __setitem__(self, key: str, value: object) -> None:
        """Set the value associated with the given key in the shelf.

        Args:
            key (str): The key under which the value will be stored.
            value (object): The value to store in the shelf.

        Returns:
            None
        """
        self._shelf.__setitem__(key, value)

    def __delitem__(self, key: str) -> None:
        """Remove the item with the specified key from the shelf.

        Args:
            key (str): The key of the item to remove.

        Raises:
            KeyError: If the key is not found in the shelf.
        """
        self._shelf.__delitem__(key)

    def close(self) -> None:
        """Closes the underlying shelf database.

        This method ensures that any changes made to the shelf are written to disk
        and releases any resources associated with the shelf.
        """
        self._shelf.close()

    def sync(self) -> None:
        """Synchronize the in-memory state of the shelf with the persistent storage.

        This method ensures that any changes made to the shelf are written to disk,
        keeping the persistent storage up to date with the current state.
        """
        self._shelf.sync()


class MultiShelf(MultiConnector):
    """Multi-collection shelf connector backed by a directory.

    Each collection maps to a separate shelf file inside *directory*.  The
    directory is created automatically on initialisation if it does not exist.

    Attributes:
        directory (str): Path to the directory containing the shelf files.
        protocol (str | None): Pickle protocol version to use (forwarded to
            :class:`Shelf`).
        writeback (bool): Whether to enable write-back caching (default:
            ``False``).
        keyencoding (str): Encoding used for keys (default: ``"utf-8"``).
    """

    directory: str
    protocol: str | None = None
    writeback: bool = False
    keyencoding: str = "utf-8"

    __slots__ = ["_shelves", "_collection_overrides"]

    @staticmethod
    def scheme() -> str:
        """Return ``"shelf"``."""
        return "shelf"

    @staticmethod
    def from_uri(uri: str) -> "MultiShelf":
        """Create a :class:`MultiShelf` from a ``shelf://\u2026`` URI.

        The path component is used as the *directory*.
        """
        p = parse.urlsplit(uri)
        params: dict[str, Any] = {}
        params["directory"] = f"{p.netloc}{p.path}" if p.path else p.netloc
        params.update(dict(parse.parse_qsl(p.query)))
        return cast("MultiShelf", MultiShelf.model_validate(params))

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        os.makedirs(self.directory, exist_ok=True)
        self._shelves: dict[str, Shelf] = {}
        self._collection_overrides: dict[str, dict[str, Any]] = {}

    def _evict(self, collection: str) -> None:
        self._shelves.pop(collection, None)

    def __getitem__(self, collection: str) -> "Shelf":
        """Return (and cache) a :class:`Shelf` for *collection*.

        The shelf file is stored at ``{directory}/{collection}``.

        Args:
            collection (str): An arbitrary collection identifier used as the
                filename within the directory.

        Returns:
            Shelf: A connector scoped to the named shelf file.
        """
        if collection not in self._shelves:
            overrides = self._collection_overrides.get(collection)
            if overrides is not None:
                kt, dkt, dvt = overrides["key_types"], overrides["default_key_type"], overrides["default_value_type"]
            else:
                kt, dkt, dvt = self.key_types, self.default_key_type, self.default_value_type
            self._shelves[collection] = Shelf(
                filename=os.path.join(self.directory, collection),
                protocol=self.protocol,
                writeback=self.writeback,
                keyencoding=self.keyencoding,
                key_types=kt,
                default_key_type=dkt,
                default_value_type=dvt,
            )
        return self._shelves[collection]

    def collections(self) -> list[str]:
        """Return the names of all collections that have been opened.

        Returns:
            list[str]: Collection names in insertion order.
        """
        return list(self._shelves.keys())

    def close(self) -> None:
        """Sync and close all open shelf files."""
        for shelf in self._shelves.values():
            shelf.close()
        self._shelves.clear()
