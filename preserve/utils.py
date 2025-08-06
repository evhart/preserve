from typing import Any


class Singleton(type):
    """A metaclass that implements the Singleton design pattern.

    Classes using this metaclass will only have one instance throughout the application's lifetime.
    If an instance of the class already exists, subsequent instantiations will return the same instance.

    Attributes:
        _instances (dict): A dictionary mapping classes to their singleton instances.

    Methods:
        __call__(cls, *args, **kwargs): Returns the singleton instance of the class, creating it if necessary.
    """

    _instances: Any = {}

    def __call__(cls, *args, **kwargs):
        """Creates or returns the singleton instance of the class.

        If an instance of the class does not already exist, it creates one using the provided arguments.
        Subsequent calls return the same instance, ensuring only one instance exists.

        Args:
            *args: Positional arguments to pass to the class constructor.
            **kwargs: Keyword arguments to pass to the class constructor.

        Returns:
            object: The singleton instance of the class.
        """
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
