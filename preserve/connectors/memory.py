from typing import Any, Generator, Optional, cast
from urllib import parse

from preserve.connector import Connector


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
        get(key, default=None):
            Retrieves the value for a key, or returns default if not found.
        __getitem__(key):
            Retrieves the value for a key.
        __setitem__(key, value):
            Sets the value for a key.
        __delitem__(key):
            Deletes a key from the store.
        __enter__():
            Enters a context manager.
        __exit__(type, value, traceback):
            Exits a context manager and closes the store.
        close():
            Closes the store and releases resources.
        __del__():
            Ensures the store is closed upon deletion.
        sync():
            Placeholder for syncing data (no-op for memory backend).
    """

    keyencoding: str = "utf-8"

    __slots__ = "_dict"

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
        self._dict: Optional[dict[bytes, Any]] = {}

    def __iter__(self) -> Generator[tuple[str, Any], None, None]:
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

    def get(self, key: str, default: Any = None) -> Any:
        """Retrieve the value associated with the given key from the internal dictionary.

        Args:
            key: The key to look up in the dictionary.
            default: The value to return if the key is not found. Defaults to None.

        Returns:
            The value associated with the key if it exists, otherwise the default value.
        """
        if self._dict is not None and key.encode(self.keyencoding) in self._dict:
            return self._dict[key.encode(self.keyencoding)]
        return default

    def __getitem__(self, key: str) -> Any:
        """Retrieve the value associated with the given key using dictionary-style access.

        Args:
            key: The key whose value is to be retrieved.

        Returns:
            The value associated with the specified key.

        Raises:
            KeyError: If the key is not found.
        """
        return self.get(key)

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
            RuntimeError: If the memory store is closed.

        Notes:
            If the key does not exist in the store, the method silently ignores the KeyError.
        """
        if self._dict is not None:
            try:
                del self._dict[key.encode(self.keyencoding)]
            except KeyError:
                pass
        else:
            raise RuntimeError("Memory store is closed.")

    def __enter__(self) -> "Memory":
        """Enter the runtime context related to this object.

        Returns:
            self: Returns the instance itself to be used as the context manager.
        """
        return self

    def __exit__(
        self,
        type: Optional[type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[Any],
    ) -> None:
        """Handles cleanup actions when exiting a context.

        Ensures that resources are properly released by calling the `close` method.

        Args:
            type (Type[BaseException] or None): The exception type, if an exception was raised, otherwise None.
            value (BaseException or None): The exception instance, if an exception was raised, otherwise None.
            traceback (TracebackType or None): The traceback object, if an exception was raised, otherwise None.
        """
        self.close()

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

    def __del__(self) -> None:
        """Destructor method that ensures resources are properly released by calling the close() method."""
        self.close()

    def sync(self) -> None:
        """Synchronizes the in-memory data with the persistent storage or external source."""
        pass
