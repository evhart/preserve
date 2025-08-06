import json
import sqlite3
from typing import Any, Dict, Generator, Iterator, Optional, Tuple, cast
from urllib import parse

from preserve.connector import Connector


class SQLite(Connector):
    """SQLite connector for managing a simple key-value store using an SQLite database.

    Attributes:
        filename (str): The path to the SQLite database file.
        protocol (Optional[str]): The protocol used for the connection (default: None).

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
        iteritems() -> Iterator[Tuple[Any, Dict[str, Any]]]:
            Iterates over all key-value pairs in the database.
        __len__() -> int:
            Returns the number of items in the database.
        __contains__(key: Any) -> bool:
            Checks if a key exists in the database.
        get(key: Any, default: Optional[Any] = None) -> Optional[Dict[str, Any]]:
            Retrieves an item by key, returning default if not found.
        __getitem__(key: Any) -> Optional[Dict[str, Any]]:
            Retrieves an item by key, raising KeyError if not found.
        __setitem__(key: Any, value: Dict[str, Any]) -> None:
            Inserts or updates an item in the database.
        __delitem__(key: Any) -> None:
            Deletes an item by key from the database.
        __enter__() -> "SQLite":
            Enters the runtime context for the connector.
        __exit__(type: Any, value: Any, traceback: Any) -> None:
            Exits the runtime context and closes the connection.
        close() -> None:
            Closes the SQLite connection.
        __del__() -> None:
            Destructor to ensure the connection is closed.
        sync() -> None:
            Synchronizes the database (no-op for SQLite).

    Usage:
        Use this class to interact with an SQLite-backed key-value store,
            supporting context management and dictionary-like operations.
    """

    filename: str
    protocol: Optional[str] = None

    __slots__ = ["_sqlite"]

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
            """
            CREATE TABLE IF NOT EXISTS preserve (
                _id TEXT PRIMARY KEY,
                _content TEXT
            )
        """
        )
        self._sqlite.commit()

    def __iter__(self) -> "Generator[tuple[str, Any], None, None]":
        """Iterates over all records in the 'preserve' table, yielding each row.

        Yields:
            tuple[str, Any]: A tuple where the first element is the record's '_id' and
                the second element is its '_content'.
        """
        cursor = self._sqlite.cursor()
        cursor.execute("SELECT _id, _content FROM preserve")
        for row in cursor.fetchall():
            yield (row[0], row[1])

    def iteritems(self) -> Iterator[Tuple[Any, Dict[str, Any]]]:
        """Iterates over the items in the SQLite database.

        Yields:
            Tuple[Any, Dict[str, Any]]: A tuple containing the item's unique identifier and its content as a dictionary.
        """
        cursor = self._sqlite.cursor()
        cursor.execute("SELECT _id, _content FROM preserve")
        for row in cursor.fetchall():
            yield (row[0], row[1])

    def __len__(self) -> int:
        """Returns the number of items in the SQLite database.

        This method executes a SQL query to count the total number of records
        in the 'preserve' table and returns the result as an integer.

        Returns:
            int: The number of items in the 'preserve' table.
        """
        cursor = self._sqlite.cursor()
        cursor.execute("SELECT COUNT(*) FROM preserve")
        return cursor.fetchone()[0]

    def __contains__(self, key: Any) -> bool:
        """Check if the specified key exists in the SQLite database.

        Args:
            key (Any): The key to check for existence in the database.

        Returns:
            bool: True if the key exists, False otherwise.
        """
        cursor = self._sqlite.cursor()
        cursor.execute("SELECT COUNT(*) FROM preserve WHERE _id = ?", (str(key),))
        return cursor.fetchone()[0] > 0

    def get(self, key: Any, default: Optional[Any] = None) -> Optional[Dict[str, Any]]:
        """Retrieve an item from the SQLite database by its key.

        Args:
            key (Any): The key identifying the item to retrieve.
            default (Optional[Any], optional): The value to return if the key is not found. Defaults to None.

        Returns:
            Optional[Dict[str, Any]]: The deserialized item if found, otherwise the default value.
        """
        cursor = self._sqlite.cursor()
        cursor.execute("SELECT _content FROM preserve WHERE _id = ?", (str(key),))
        row = cursor.fetchone()
        return json.loads(row[0]) if row else default

    def __getitem__(self, key: Any) -> Optional[Dict[str, Any]]:
        """Retrieve an item from the SQLite database by key.

        Args:
            key (Any): The key used to look up the item in the database.

        Returns:
            Optional[Dict[str, Any]]: The item associated with the given key, if found.

        Raises:
            KeyError: If the key is not found in the SQLite database.
        """
        item = self.get(key)
        if item is None:
            raise KeyError(f"Key {str(key)} not found in SQLite database.")
        return item

    def __setitem__(self, key: Any, value: Dict[str, Any]) -> None:
        """Inserts or updates an item in the SQLite database.

        Args:
            key (Any): The key identifying the item to insert or update.
            value (Dict[str, Any]): The value to be stored, represented as a dictionary.

        This method serializes the value to JSON and stores it in the '_content' column of the 'preserve' table,
        using the provided key as the '_id'. If an entry with the same key exists, it is replaced.
        """
        cursor = self._sqlite.cursor()
        value_json = json.dumps(value)

        cursor.execute(
            "INSERT OR REPLACE INTO preserve (_id, _content) VALUES (?, ?)",
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
        cursor.execute("DELETE FROM preserve WHERE _id = ?", (str(key),))
        self._sqlite.commit()

    def __enter__(self) -> "SQLite":
        """Enter the runtime context for the SQLite connector.

        Returns:
            SQLite: The current instance of the SQLite connector.
        """
        return self

    def __exit__(self, type: Any, value: Any, traceback: Any) -> None:
        """Exit the runtime context related to this object.

        This method is called when exiting a 'with' statement block.
        It ensures that the SQLite connection is properly closed, releasing any resources held by the connection.

        Args:
            type (Any): The exception type, if an exception was raised, otherwise None.
            value (Any): The exception value, if an exception was raised, otherwise None.
            traceback (Any): The traceback object, if an exception was raised, otherwise None.

        Returns:
            None
        """
        self._sqlite.close()

    def close(self) -> None:
        """Closes the underlying SQLite database connection.

        This method ensures that all resources associated with the connection are released.
        After calling this method, the connection object should not be used for further operations.
        """
        self._sqlite.close()

    def __del__(self) -> None:
        """Destructor method that ensures the SQLite connection is properly closed."""
        self.close()

    def sync(self) -> None:
        """Synchronizes the SQLite database.

        For SQLite, this method is a no-op because all changes are written to disk immediately.
        """
        pass
