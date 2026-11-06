from abc import abstractmethod, ABC


class BaseStorage(ABC):
    """Abstract storage interface. Subclasses must implement all methods."""

    @abstractmethod
    def put(self, key: str, data: bytes) -> None:
        """Store bytes under key."""
        ...

    @abstractmethod
    def get(self, key: str) -> bytes:
        """Return bytes stored under key."""
        ...

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Return True if key has a value stored."""
        ...

    @abstractmethod
    def prefix_exists(self, prefix: str) -> bool:
        """Return True if at least one object exists under the prefix"""
        ...
