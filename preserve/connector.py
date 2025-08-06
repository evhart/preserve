import collections.abc
from abc import abstractmethod
from typing import Any, Optional, Type, TypeVar

from pydantic import BaseModel, ConfigDict

T = TypeVar("T", bound="Connector")


class Connector(BaseModel, collections.abc.MutableMapping):
    """Abstract base class for key-value storage connectors for Preserve.

    This class defines the required interface for implementing a connector that behaves like a mutable mapping (dictionary),
    with additional methods for resource management and synchronization. Subclasses must implement all abstract methods.

    Attributes:
        model_config (ConfigDict): Configuration for the model, set to allow attribute-based initialization.

    Methods:
        scheme() -> str:
            Return the URI scheme handled by this connector (e.g., 'file').
        from_uri(uri: str) -> "Connector":
            Create and return a connector instance from a given URI.
        __iter__() -> collections.abc.Generator[tuple[str, Any], None, None]:
            Iterate over key-value pairs in the storage.
        __len__() -> int:
            Return the number of items in the storage.
        __contains__(key: Any) -> bool:
            Return True if the key exists in the storage.
        get(key: Any, default: Optional[Any] = None) -> Any:
            Return the value for the given key, or default if not found.
        __getitem__(key: Any) -> Any:
            Retrieve the value associated with the given key.
        __setitem__(key: Any, value: Any) -> None:
            Set the value for the given key.
        __delitem__(key: Any) -> None:
            Remove the item with the given key.
        __enter__() -> "Connector":
            Enter the runtime context related to this object.
        __exit__(type, value, traceback) -> Optional[bool]:
            Exit the runtime context and perform cleanup.
        close() -> None:
            Close the connector and release any resources.
        __del__() -> None:
            Destructor to ensure cleanup.
        sync() -> None:
            Synchronize the storage, persisting any changes.
        __setattr__(attr: str, value: Any) -> None:
            Set an attribute, respecting __slots__ if defined.
    """

    model_config = ConfigDict(from_attributes=True)

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
    def get(self, key: Any, default: Optional[Any] = None) -> Any:
        """Retrieve the value associated with the given key.

        Args:
            key (Any): The key to look up in the connector.
            default (Optional[Any], optional): The value to return if the key is not found. Defaults to None.

        Returns:
            Any: The value associated with the key, or the default value if the key is not found.

        Raises:
            NotImplementedError: If the method is not implemented by a subclass.
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

    @abstractmethod
    def __enter__(self) -> "Connector":
        """Enter the runtime context related to this object.

        Returns:
            Connector: The connector instance itself.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError()

    @abstractmethod
    def __exit__(
        self,
        type: Optional[Type[BaseException]],
        value: Optional[BaseException],
        traceback: Optional[Any],
    ) -> Optional[bool]:
        """Exit the runtime context and handle any exception that occurred.

        Args:
            type (Optional[Type[BaseException]]): The exception type, if an exception was raised, otherwise None.
            value (Optional[BaseException]): The exception instance, if an exception was raised, otherwise None.
            traceback (Optional[Any]): The traceback object, if an exception was raised, otherwise None.

        Returns:
            Optional[bool]: If True, suppresses the exception; otherwise, the exception is propagated.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError()

    @abstractmethod
    def close(self) -> None:
        """Closes the connector and releases any associated resources.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError()

    @abstractmethod
    def __del__(self) -> None:
        """Destructor method called when the instance is about to be destroyed.

        Raises:
            NotImplementedError: This method must be implemented by subclasses.
        """
        raise NotImplementedError()

    @abstractmethod
    def sync(self) -> None:
        """Synchronizes data between the source and destination.

        This method should be implemented by subclasses to define the specific
        synchronization logic. Raises NotImplementedError if not overridden.
        """
        raise NotImplementedError()

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
