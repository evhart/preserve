from __future__ import annotations

import collections.abc
import re
import sqlite3
from typing import Any, cast
from urllib import parse

from pydantic import field_validator

from preserve.connector import Connector, MultiConnector

_VALID_COLLECTION_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class SQLite(Connector):
    """SQLite connector for managing a simple key-value store using an SQLite database.

    Values are serialized to JSON using :meth:`Connector._serialize_value` (pydantic-core),
    which handles datetimes, UUIDs, Decimals, bytes, Enums, dataclasses, and
    ``BaseModel`` instances with full round-trip fidelity for the latter.

    Attributes:
        filename (str): The path to the SQLite database file.
        protocol (str | None): The protocol used for the connection (default: None).

    Class Methods:
        scheme() -> str:
            Returns the URI scheme for SQLite connectors ("sqlite").
        from_uri(uri: str) -> "SQLite":
            Creates an SQLite connector instance from a given URI.

    Instance Methods:
        __init__(**kwargs):
            Initializes the SQLite connector and ensures the required table exists.
        __iter__() -> Generator[tuple[str, Any], None, None]:
            Iterates over all key-value pairs in the database.
        iteritems() -> collections.abc.Iterator[tuple[Any, Any]]:
            Iterates over all key-value pairs in the database.
        __len__() -> int:
            Returns the number of items in the database.
        __contains__(key: Any) -> bool:
            Checks if a key exists in the database.
        __getitem__(key: Any) -> Any:
            Retrieves an item by key, raising KeyError if not found.
        __setitem__(key: Any, value: Any) -> None:
            Inserts or updates an item in the database.
        __delitem__(key: Any) -> None:
            Deletes an item by key from the database.
        close() -> None:
            Closes the SQLite connection.
        sync() -> None:
            Synchronizes the database (no-op for SQLite).

    Usage:
        Use this class to interact with an SQLite-backed key-value store,
            supporting context management and dictionary-like operations.
    """

    filename: str
    collection: str = "preserve"
    protocol: str | None = None

    __slots__ = ["_sqlite"]

    @field_validator("collection")
    @classmethod
    def _validate_collection(cls, v: str) -> str:
        """Ensure the collection name is a valid SQL identifier.

        Only letters, digits, and underscores are allowed; the name must start
        with a letter or underscore.  This prevents SQL injection via the table
        name, which cannot be supplied as a bind parameter.

        Args:
            v (str): The proposed collection name.

        Returns:
            str: The validated collection name.

        Raises:
            ValueError: If *v* is not a valid SQL identifier.
        """
        if not _VALID_COLLECTION_RE.match(v):
            raise ValueError(
                f"Invalid collection name {v!r}: must start with a letter or "
                "underscore and contain only letters, digits, and underscores."
            )
        return v

    @staticmethod
    def scheme() -> str:
        """Returns the URI scheme used for SQLite database connections.

        Returns:
            str: The string "sqlite", representing the SQLite URI scheme.
        """
        return "sqlite"

    @staticmethod
    def from_uri(uri: str) -> "SQLite":
        """Creates an instance of the SQLite class from a URI string.

        Parses the given URI to extract the database filename and any query parameters,
        validates the scheme, and constructs the SQLite object using the extracted parameters.

        Args:
            uri (str): The URI string representing the SQLite database connection.

        Returns:
            SQLite: An instance of the SQLite class initialized with parameters from the URI.

        Raises:
            ValueError: If the URI scheme is not supported (i.e., not 'sqlite' or 'file').
        """
        p = parse.urlsplit(uri)
        if p.scheme != SQLite.scheme() and p.scheme != "file":
            raise ValueError()

        params = {}
        params["filename"] = f"{p.netloc}/{p.path}" if p.path != "" else p.netloc
        params.update(dict(parse.parse_qsl(p.query)))

        return cast("SQLite", SQLite.model_validate(params))

    def __init__(self, **kwargs):
        """Initializes the SQLite connector.

        Establishes a connection to the SQLite database using the provided filename,
        and ensures that the 'preserve' table exists with the appropriate schema.
        Additional keyword arguments are passed to the superclass initializer.

        Args:
            **kwargs: Arbitrary keyword arguments passed to the superclass.
        """
        super().__init__(**kwargs)
        self._sqlite = sqlite3.connect(self.filename)

        # Initialise the SQLite database if needed
        cursor = self._sqlite.cursor()
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS "{self.collection}" (
                _id TEXT PRIMARY KEY,
                _content TEXT
            )
        """
        )
        self._sqlite.commit()

    def __iter__(self) -> collections.abc.Generator[tuple[str, Any], None, None]:
        """Iterates over all records in the collection table, yielding each row.

        Yields:
            tuple[str, Any]: A tuple where the first element is the record's '_id' and
                the second element is its '_content'.
        """
        cursor = self._sqlite.cursor()
        cursor.execute(f'SELECT _id, _content FROM "{self.collection}"')
        for row in cursor.fetchall():
            yield (row[0], self._deserialize_value(row[1]))

    def iteritems(self) -> collections.abc.Iterator[tuple[Any, Any]]:
        """Iterates over the items in the SQLite database.

        Yields:
            tuple[Any, Any]: A tuple containing the item's unique identifier and its content.
        """
        cursor = self._sqlite.cursor()
        cursor.execute(f'SELECT _id, _content FROM "{self.collection}"')
        for row in cursor.fetchall():
            yield (row[0], self._deserialize_value(row[1]))

    def __len__(self) -> int:
        """Returns the number of items in the SQLite database.

        This method executes a SQL query to count the total number of records
        in the 'preserve' table and returns the result as an integer.

        Returns:
            int: The number of items in the 'preserve' table.
        """
        cursor = self._sqlite.cursor()
        cursor.execute(f'SELECT COUNT(*) FROM "{self.collection}"')
        return cursor.fetchone()[0]

    def __contains__(self, key: Any) -> bool:
        """Check if the specified key exists in the SQLite database.

        Args:
            key (Any): The key to check for existence in the database.

        Returns:
            bool: True if the key exists, False otherwise.
        """
        cursor = self._sqlite.cursor()
        cursor.execute(f'SELECT COUNT(*) FROM "{self.collection}" WHERE _id = ?', (str(key),))
        return cursor.fetchone()[0] > 0

    def __getitem__(self, key: Any) -> Any:
        """Retrieve an item from the SQLite database by key.

        Args:
            key (Any): The key used to look up the item in the database.

        Returns:
            Any: The item associated with the given key.

        Raises:
            KeyError: If the key is not found in the SQLite database.
        """
        cursor = self._sqlite.cursor()
        cursor.execute(f'SELECT _content FROM "{self.collection}" WHERE _id = ?', (str(key),))
        row = cursor.fetchone()
        if row is None:
            raise KeyError(key)
        return self._deserialize_value(row[0])

    def __setitem__(self, key: Any, value: Any) -> None:
        """Inserts or updates an item in the SQLite database.

        Args:
            key (Any): The key identifying the item to insert or update.
            value (Any): The value to be stored.

        This method serializes the value to JSON via :meth:`Connector._serialize_value`
        and stores it in the ``_content`` column of the ``preserve`` table,
        using the provided key as the ``_id``. If an entry with the same key exists,
        it is replaced.
        """
        cursor = self._sqlite.cursor()
        value_json = self._serialize_value(value)

        cursor.execute(
            f'INSERT OR REPLACE INTO "{self.collection}" (_id, _content) VALUES (?, ?)',
            (key, value_json),
        )
        self._sqlite.commit()

    def __delitem__(self, key: Any) -> None:
        """Removes the item associated with the given key from the SQLite database.

        Args:
            key (Any): The key identifying the item to be deleted.

        Raises:
            KeyError: If the key does not exist in the database.
        """
        cursor = self._sqlite.cursor()
        cursor.execute(f'DELETE FROM "{self.collection}" WHERE _id = ?', (str(key),))
        self._sqlite.commit()
        if cursor.rowcount == 0:
            raise KeyError(key)

    def close(self) -> None:
        """Closes the underlying SQLite database connection.

        This method ensures that all resources associated with the connection are released.
        After calling this method, the connection object should not be used for further operations.
        """
        self._sqlite.close()

    def sync(self) -> None:
        """Synchronizes the SQLite database.

        For SQLite, this method is a no-op because all changes are written to disk immediately.
        """
        pass


class MultiSQLite(MultiConnector):
    """Multi-collection SQLite connector.

    Each collection maps to a separate table in the same ``.db`` file.  Tables
    are created on first access.

    Attributes:
        filename (str): Path to the SQLite database file.  Pass ``":memory:"``
            for a transient in-process database.
    """

    filename: str

    __slots__ = ["_collections", "_collection_overrides"]

    @staticmethod
    def scheme() -> str:
        """Return ``"sqlite"`` — the scheme used in the multi-connector registry."""
        return "sqlite"

    @staticmethod
    def from_uri(uri: str) -> "MultiSQLite":
        """Create a :class:`MultiSQLite` from a ``sqlite://\u2026`` URI."""
        p = parse.urlsplit(uri)
        params: dict[str, Any] = {}
        params["filename"] = f"{p.netloc}{p.path}" if p.path else p.netloc
        params.update(dict(parse.parse_qsl(p.query)))
        return cast("MultiSQLite", MultiSQLite.model_validate(params))

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._collections: dict[str, SQLite] = {}
        self._collection_overrides: dict[str, dict[str, Any]] = {}

    def _evict(self, collection: str) -> None:
        self._collections.pop(collection, None)

    def __getitem__(self, collection: str) -> "SQLite":
        """Return (and cache) a :class:`SQLite` for *collection*.

        The table is created the first time any key is written to the collection.

        Args:
            collection (str): Collection (table) name — must be a valid SQL
                identifier (letters, digits, underscores; starts with letter or
                underscore).

        Returns:
            SQLite: A connector scoped to the named table.
        """
        if collection not in self._collections:
            overrides = self._collection_overrides.get(collection)
            if overrides is not None:
                kt, dkt, dvt = overrides["key_types"], overrides["default_key_type"], overrides["default_value_type"]
            else:
                kt, dkt, dvt = self.key_types, self.default_key_type, self.default_value_type
            self._collections[collection] = SQLite(
                filename=self.filename,
                collection=collection,
                key_types=kt,
                default_key_type=dkt,
                default_value_type=dvt,
            )
        return self._collections[collection]

    def collections(self) -> list[str]:
        """Return the names of all tables accessible from this multi-connector.

        For in-memory databases (``filename=":memory:"``) each sub-connector
        has its own independent database, so only collections opened in this
        session are reported.  For file-based databases, all tables present in
        the file are returned.

        Returns:
            list[str]: Table names in alphabetical order.
        """
        if self.filename == ":memory:":
            return sorted(self._collections.keys())
        conn = sqlite3.connect(self.filename)
        try:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            return [row[0] for row in cur.fetchall()]
        finally:
            conn.close()

    def close(self) -> None:
        """Close all open sub-connectors."""
        for connector in self._collections.values():
            connector.close()
        self._collections.clear()
