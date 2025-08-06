import shelve
from typing import Any, Generator, Optional, cast
from urllib import parse

from preserve.connector import Connector


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

        get(key: str, default: object = None) -> object:
            Retrieves a value by key, returning default if not found.

        __getitem__(key: str) -> object:
            Retrieves a value by key.

        __setitem__(key: str, value: object) -> None:
            Sets a value for a given key.

        __delitem__(key: str) -> None:
            Deletes a key-value pair.

        __enter__() -> "Shelf":
            Enters a context manager, returning self.

        __exit__(type, value, traceback) -> None:
            Exits the context manager and closes the shelf.

        close() -> None:
            Closes the shelf database.

        __del__() -> None:
            Ensures the shelf is closed on deletion.

        sync() -> None:
            Synchronizes the in-memory state with the persistent storage.
    """

    filename: str
    protocol: Optional[str] = None
    writeback: bool = False
    keyencoding: str = "utf-8"

    __slots__ = ["_shelf"]

    def __setattr__(self, attr: str, value: object) -> None:
        """Set an attribute on the instance.

        If the attribute name is defined in __slots__, set it directly using the base object's __setattr__.
        Otherwise, delegate to the superclass's __setattr__ method.

        Args:
            attr (str): The name of the attribute to set.
            value (object): The value to assign to the attribute.
        """
        if attr in self.__slots__:
            object.__setattr__(self, attr, value)
        else:
            super().__setattr__(attr, value)

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

    def __iter__(self) -> "Generator[tuple[str, Any], None, None]":
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

    def get(self, key: str, default: object = None) -> object:
        """Retrieve the value associated with the given key from the shelf.

        Args:
            key (str): The key to look up in the shelf.
            default (object, optional): The value to return if the key is not found. Defaults to None.

        Returns:
            object: The value associated with the key if it exists, otherwise the default value.
        """
        return self._shelf.get(key, default)

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

    def __enter__(self) -> "Shelf":
        """Enter the runtime context related to this object.

        Returns:
            Shelf: The shelf instance itself, allowing usage with the 'with' statement.
        """
        return self

    def __exit__(self, type, value, traceback) -> None:
        """Handles cleanup actions when exiting the context manager.

        Closes the underlying shelf storage to ensure all changes are saved and resources are released.

        Args:
            type (Optional[Type[BaseException]]): The exception type, if an exception was raised.
            value (Optional[BaseException]): The exception instance, if an exception was raised.
            traceback (Optional[TracebackType]): The traceback object, if an exception was raised.

        Returns:
            None
        """
        self._shelf.close()

    def close(self) -> None:
        """Closes the underlying shelf database.

        This method ensures that any changes made to the shelf are written to disk
        and releases any resources associated with the shelf.
        """
        self._shelf.close()

    def __del__(self) -> None:
        """Destructor method that ensures the connector is properly closed when the object is garbage collected."""
        self.close()

    def sync(self) -> None:
        """Synchronize the in-memory state of the shelf with the persistent storage.

        This method ensures that any changes made to the shelf are written to disk,
        keeping the persistent storage up to date with the current state.
        """
        self._shelf.sync()
