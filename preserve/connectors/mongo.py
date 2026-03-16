from __future__ import annotations

import collections.abc
from typing import Any, cast
from urllib import parse

from pydantic import field_validator

from preserve.connector import Connector, MultiConnector


class Mongo(Connector):
    """MongoDB backend connector for the preserve framework.

    This class provides a dictionary-like interface to a MongoDB collection,
    allowing storage and retrieval of key-value pairs where keys are used as
    MongoDB document `_id` fields.

    Attributes:
        database (str): Name of the MongoDB database to use. Defaults to "db".
        collection (str | None): Name of the MongoDB collection. If not set, defaults to the database name.
        host (str): Hostname or IP address of the MongoDB server. Defaults to "127.0.0.1".
        port (int): Port number of the MongoDB server. Defaults to 27017.

    Methods:
        from_uri(uri: str) -> "Mongo":
            Create a Mongo instance from a MongoDB URI.

        scheme() -> str:
            Return the URI scheme supported by this connector ("mongodb").

        __init__(*args, **kwargs):
            Initialize the Mongo connector and connect to the specified MongoDB instance.

        __iter__() -> Generator[tuple[str, Any], None, None]:
            Iterate over all documents in the collection, yielding (key, value) pairs.

        iteritems() -> collections.abc.Iterator[tuple[Any, dict[str, Any]]]:
            Iterate over all documents in the collection, yielding (key, value) pairs.

        __len__() -> int:
            Return the number of documents in the collection.

        __contains__(key: Any) -> bool:
            Check if a document with the given key exists in the collection.

        __getitem__(key: Any) -> dict[str, Any] | None:
            Retrieve a document by key.

        __setitem__(key: Any, value: Dict[str, Any]) -> None:
            Insert or update a document with the given key and value.

        __delitem__(key: Any) -> None:
            Delete a document by key.

        __enter__() -> "Mongo":
            Enter the runtime context related to this object.

        __exit__(type, value, traceback) -> None:
            Exit the runtime context and close the MongoDB connection.

        close() -> None:
            Close the MongoDB connection.

        sync() -> None:
            Placeholder for interface compatibility; does nothing.
    """

    database: str = "db"
    collection: str | None = None
    host: str = "127.0.0.1"
    port: int = 27017

    __slots__ = ["_client", "_collection"]

    @field_validator("collection")
    @classmethod
    def _validate_collection(cls, v: str | None) -> str | None:
        """Reject MongoDB collection names that contain unsafe characters.

        MongoDB itself forbids null bytes, ``$`` signs, and names that start
        with ``system.`` (reserved prefix).  Validating here gives a clear
        error before pymongo raises its own ``InvalidName``.

        Args:
            v (str | None): The proposed collection name, or ``None``.

        Returns:
            str | None: The validated name unchanged.

        Raises:
            ValueError: If *v* contains a null byte or ``$``, or starts with
                ``"system."``.
        """
        if v is None:
            return v
        if "\x00" in v:
            raise ValueError("MongoDB collection name must not contain a null byte.")
        if "$" in v:
            raise ValueError("MongoDB collection name must not contain '$'.")
        if v.startswith("system."):
            raise ValueError("MongoDB collection name must not start with 'system.' (reserved prefix).")
        return v

    @staticmethod
    def is_available() -> bool:
        """Return True if pymongo is installed."""
        try:
            import pymongo  # noqa: F401

            return True
        except ImportError:
            return False

    @staticmethod
    def from_uri(uri: str) -> "Mongo":
        """Creates a Mongo instance from a URI string.

        Parses the given URI to extract connection parameters such as host,
        port, database, and any additional query parameters.
        Validates that the URI scheme matches the expected Mongo scheme.

        Raises:
            ValueError: If the URI scheme does not match the expected Mongo scheme.

        Args:
            uri (str): The MongoDB connection URI.

        Returns:
            Mongo: An instance of the Mongo class initialized with parameters extracted from the URI.
        """
        p = parse.urlsplit(uri)
        if p.scheme != Mongo.scheme():
            raise ValueError()

        params: dict[str, Any] = {}
        if p.hostname:
            params["host"] = p.hostname

        if p.port:
            params["port"] = p.port

        if p.path and len(p.path.split("/", 1)) > 1:
            params["database"] = p.path.split("/", 1)[1]

        params.update(dict(parse.parse_qsl(p.query)))

        return cast("Mongo", Mongo.model_validate(params))

    @staticmethod
    def scheme() -> str:
        """Returns the URI scheme for MongoDB connections.

        Returns:
            str: The string "mongodb" representing the MongoDB URI scheme.
        """
        return "mongodb"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initializes the MongoDB connector.

        Attempts to import the ``pymongo`` package and raises an ImportError
        with installation instructions if not found.
        Calls the superclass initializer with provided arguments.
        If ``self.collection`` is not set, defaults it to the value of
        ``self.database``. Initializes a ``MongoClient`` using the specified
        host and port, and sets up the collection handle for database
        operations.

        Args:
            *args: Additional positional arguments passed to the superclass.
            **kwargs: Additional keyword arguments passed to the superclass.

        Raises:
            ImportError: If the `pymongo` package is not installed.
        """
        try:
            from pymongo import MongoClient
        except ImportError as err:
            raise ImportError(
                "MongoDB connector requires pymongo package. Please install it with `pip install pymongo`."
            ) from err

        super().__init__(*args, **kwargs)

        if not self.collection:
            self.collection = self.database

        self._client = MongoClient(self.host, self.port)
        self._collection = self._client[self.database][self.collection]

    def __iter__(self) -> collections.abc.Generator[tuple[str, Any], None, None]:
        """Iterates over all documents in the collection.

        Yields each document's ID and the document data (excluding the
        ``'_id'`` field).

        Yields:
            tuple[str, Any]: A tuple containing the document's ``'_id'``
            and the document data as a dictionary without the ``'_id'``
            field.
        """
        for item in self._collection.find():
            _id = item["_id"]
            del item["_id"]
            yield _id, item

    def iteritems(self) -> collections.abc.Iterator[tuple[Any, dict[str, Any]]]:
        """Yields each document in the collection as a (id, data) tuple.

        The ``_id`` field is excluded from the returned data dictionary.

        Yields:
            tuple[Any, dict[str, Any]]: A tuple where the first element is
            the document's ``_id`` and the second element is the document
            data dictionary without the ``_id`` key.
        """
        for item in self._collection.find():
            _id = item["_id"]
            del item["_id"]
            yield _id, item

    def __len__(self) -> int:
        """Returns the number of documents in the collection.

        Returns:
            int: The total count of documents in the collection.
        """
        return self._collection.count_documents({})

    def __contains__(self, key: Any) -> bool:
        """Check if a document with the specified key exists in the MongoDB collection.

        Args:
            key (Any): The key to check for existence in the collection.

        Returns:
            bool: True if a document with the given key exists, False otherwise.
        """
        return self._collection.count_documents({"_id": key}, limit=1) != 0

    def __getitem__(self, key: Any) -> dict[str, Any] | None:
        """Retrieve an item from the MongoDB collection by its key.

        Args:
            key (Any): The key (typically the MongoDB document's _id) used to look up the item.

        Returns:
            dict[str, Any]: The document as a dictionary without the '_id' field.

        Raises:
            KeyError: If no document with the given key exists.
        """
        item = self._collection.find_one({"_id": key})
        if item is None:
            raise KeyError(key)
        del item["_id"]
        return item

    def __setitem__(self, key: Any, value: dict[str, Any]) -> None:
        """Sets the item in the MongoDB collection with the specified key and value.

        If an item with the given key (_id) exists, it is updated with the provided value.
        If it does not exist, a new item is inserted with the key and value.

        Args:
            key (Any): The key to identify the item in the collection
                (used as the MongoDB ``_id``).
            value (dict[str, Any]): The value to store in the collection.
                This dictionary will be updated with the key as its
                ``"_id"`` field.

        Returns:
            None
        """
        item = dict(value)
        item["_id"] = key
        self._collection.update_one({"_id": key}, {"$set": item}, upsert=True)

    def __delitem__(self, key: Any) -> None:
        """Removes the document with the specified key from the MongoDB collection.

        Args:
            key (Any): The key identifying the document to delete.

        Raises:
            KeyError: If no document with the specified key exists.
        """
        result = self._collection.delete_one({"_id": key})
        if result.deleted_count == 0:
            raise KeyError(key)

    def close(self) -> None:
        """Closes the connection to the MongoDB client."""
        self._client.close()

    def sync(self) -> None:
        pass


class MultiMongo(MultiConnector):
    """Multi-collection MongoDB connector.

    Each collection maps to a separate MongoDB collection inside the same
    database.  All sub-connectors share the same ``MongoClient`` connection
    pool.

    Requires ``pymongo`` to be installed (``pip install pymongo``).

    Attributes:
        database (str): Name of the MongoDB database (default: ``"db"``).
        host (str): MongoDB server hostname (default: ``"127.0.0.1"``).
        port (int): MongoDB server port (default: ``27017``).
    """

    database: str = "db"
    host: str = "127.0.0.1"
    port: int = 27017

    __slots__ = ["_client", "_collections", "_collection_overrides"]

    @staticmethod
    def is_available() -> bool:
        """Return ``True`` if *pymongo* is installed."""
        try:
            import pymongo  # noqa: F401

            return True
        except ImportError:
            return False

    @staticmethod
    def scheme() -> str:
        """Return ``"mongodb"``."""
        return "mongodb"

    @staticmethod
    def from_uri(uri: str) -> "MultiMongo":
        """Create a :class:`MultiMongo` from a ``mongodb://\u2026`` URI."""
        p = parse.urlsplit(uri)
        params: dict[str, Any] = {}
        if p.hostname:
            params["host"] = p.hostname
        if p.port:
            params["port"] = p.port
        if p.path and len(p.path.split("/", 1)) > 1:
            params["database"] = p.path.split("/", 1)[1]
        params.update(dict(parse.parse_qsl(p.query)))
        return cast("MultiMongo", MultiMongo.model_validate(params))

    def __init__(self, **kwargs: Any) -> None:
        try:
            from pymongo import MongoClient
        except ImportError as err:
            raise ImportError("MultiMongo requires pymongo. Install it with: pip install pymongo") from err
        super().__init__(**kwargs)
        self._client = MongoClient(self.host, self.port)
        self._collections: dict[str, Connector] = {}
        self._collection_overrides: dict[str, dict[str, Any]] = {}

    def _evict(self, collection: str) -> None:
        self._collections.pop(collection, None)

    def __getitem__(self, collection: str) -> Connector:
        """Return (and cache) a :class:`Mongo` for *collection*.

        Args:
            collection (str): The MongoDB collection name.

        Returns:
            Connector: A connector scoped to the named MongoDB collection.
        """
        if collection not in self._collections:
            overrides = self._collection_overrides.get(collection)
            if overrides is not None:
                kt, dkt, dvt = overrides["key_types"], overrides["default_key_type"], overrides["default_value_type"]
            else:
                kt, dkt, dvt = self.key_types, self.default_key_type, self.default_value_type
            self._collections[collection] = Mongo(
                database=self.database,
                collection=collection,
                host=self.host,
                port=self.port,
                key_types=kt,
                default_key_type=dkt,
                default_value_type=dvt,
            )
        return self._collections[collection]

    def collections(self) -> list[str]:
        """Return the names of all collections in the database as reported by MongoDB.

        Returns:
            list[str]: Collection names.
        """
        return self._client[self.database].list_collection_names()

    def close(self) -> None:
        """Close all sub-connectors and the shared :class:`MongoClient`."""
        for connector in self._collections.values():
            connector.close()
        self._collections.clear()
        self._client.close()
