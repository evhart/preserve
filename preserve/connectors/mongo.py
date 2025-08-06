from typing import Any, Dict, Generator, Iterator, Optional, Tuple, cast
from urllib import parse

from preserve.connector import Connector


class Mongo(Connector):
    """MongoDB backend connector for the preserve framework.

    This class provides a dictionary-like interface to a MongoDB collection,
    allowing storage and retrieval of key-value pairs where keys are used as
    MongoDB document `_id` fields.

    Attributes:
        database (str): Name of the MongoDB database to use. Defaults to "db".
        collection (Optional[str]): Name of the MongoDB collection. If not set, defaults to the database name.
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

        iteritems() -> Iterator[Tuple[Any, Dict[str, Any]]]:
            Iterate over all documents in the collection, yielding (key, value) pairs.

        __len__() -> int:
            Return the number of documents in the collection.

        __contains__(key: Any) -> bool:
            Check if a document with the given key exists in the collection.

        get(key: Any, default: Optional[Any] = None) -> Optional[Dict[str, Any]]:
            Retrieve a document by key, returning default if not found.

        __getitem__(key: Any) -> Optional[Dict[str, Any]]:
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

        __del__() -> None:
            Ensure the MongoDB connection is closed upon object deletion.

        sync() -> None:
            Placeholder for interface compatibility; does nothing.
    """

    database: str = "db"
    collection: Optional[str] = None
    host: str = "127.0.0.1"
    port: int = 27017

    __slots__ = ["_client", "_collection"]

    @staticmethod
    def from_uri(uri: str) -> "Mongo":
        """Creates a Mongo instance from a URI string.

        Parses the given URI to extract connection parameters such as host, port, database, and any additional query parameters.
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

        params: Dict[str, Any] = {}
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

        Attempts to import the `pymongo` package and raises an ImportError with installation instructions if not found.
        Calls the superclass initializer with provided arguments.
        If `self.collection` is not set, defaults it to the value of `self.database`.
        Initializes a `MongoClient` using the specified host and port, and sets up the collection handle for database operations.

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

    def __iter__(self) -> "Generator[tuple[str, Any], None, None]":
        """Iterates over all documents in the collection, yielding each document's ID and the document data (excluding the '_id' field).

        Yields:
            tuple[str, Any]: A tuple containing the document's '_id' and the document data as a dictionary without the '_id' field.
        """
        for item in self._collection.find():
            _id = item["_id"]
            del item["_id"]
            yield _id, item

    def iteritems(self) -> Iterator[Tuple[Any, Dict[str, Any]]]:
        """Yields each document in the collection as a tuple containing the document's _id and the document data (excluding the _id field).

        Yields:
            Tuple[Any, Dict[str, Any]]: A tuple where the first element is the document's _id and the second element is the document data dictionary without the _id key.
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
            key (Any): The key (typically the document's _id) to check for existence in the collection.

        Returns:
            bool: True if a document with the given key exists, False otherwise.
        """
        if self._collection.count_documents({"_id": key}, limit=1) != 0:
            return True
        else:
            return False

    def get(self, key: Any, default: Optional[Any] = None) -> Optional[Dict[str, Any]]:
        """Retrieve an item from the collection by key.

        Args:
            key (Any): The key to look up in the collection.
            default (Optional[Any], optional): The value to return if the key is not found. Defaults to None.

        Returns:
            Optional[Dict[str, Any]]: The item associated with the key if found, otherwise the default value.
        """
        item = self.__getitem__(key)
        return item if item else default

    def __getitem__(self, key: Any) -> Optional[Dict[str, Any]]:
        """Retrieve an item from the MongoDB collection by its key.

        Args:
            key (Any): The key (typically the MongoDB document's _id) used to look up the item.

        Returns:
            Optional[Dict[str, Any]]: The document as a dictionary without the '_id' field if found, otherwise None.
        """
        item = self._collection.find_one({"_id": key})
        if item:
            del item["_id"]
        return item

    def __setitem__(self, key: Any, value: Dict[str, Any]) -> None:
        """Sets the item in the MongoDB collection with the specified key and value.

        If an item with the given key (_id) exists, it is updated with the provided value.
        If it does not exist, a new item is inserted with the key and value.

        Args:
            key (Any): The key to identify the item in the collection (used as the MongoDB _id).
            value (Dict[str, Any]): The value to store in the collection. This dictionary will be updated with the key as its "_id" field.

        Returns:
            None
        """
        item = dict(value)
        item["_id"] = key
        self._collection.update_one({"_id": key}, {"$set": item}, upsert=True)

    def __delitem__(self, key: Any) -> None:
        """Removes the document with the specified key from the MongoDB collection.

        Args:
            key (Any): The key (typically the document's _id) identifying the document to delete.

        Raises:
            KeyError: If the document with the specified key does not exist.
        """
        self._collection.delete_one({"_id": key})

    def __enter__(self) -> "Mongo":
        """Enter the runtime context related to this object.

        Returns:
            Mongo: The current instance of the Mongo class.
        """
        return self

    def __exit__(self, type: Any, value: Any, traceback: Any) -> None:
        """Handles cleanup when exiting the context manager by closing the MongoDB client connection.

        Args:
            type (Any): The exception type, if an exception was raised.
            value (Any): The exception value, if an exception was raised.
            traceback (Any): The traceback object, if an exception was raised.

        Returns:
            None
        """
        self._client.close()

    def close(self) -> None:
        """Closes the connection to the MongoDB client."""
        self._client.close()

    def __del__(self) -> None:
        self.close()

    def sync(self) -> None:
        pass
