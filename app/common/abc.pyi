"""Type stub for app.common.abc — core storage contract ABCs."""

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Self

from app.types import Key, SeqNum, Value

class StorageEngine(ABC):
    @abstractmethod
    async def put(self, key: Key, value: Value) -> None: ...
    @abstractmethod
    async def get(self, key: Key) -> Value | None: ...
    @abstractmethod
    async def delete(self, key: Key) -> None: ...
    @abstractmethod
    async def close(self) -> None: ...

class Serializable(ABC):
    @abstractmethod
    def to_bytes(self) -> bytes: ...
    @classmethod
    @abstractmethod
    def from_bytes(cls, data: bytes) -> Self: ...

class MemTable(ABC):
    @abstractmethod
    def put(self, key: Key, seq: SeqNum, timestamp_ms: int, value: Value) -> None: ...
    @abstractmethod
    def get(self, key: Key) -> tuple[SeqNum, Value] | None: ...
    @abstractmethod
    def items(self) -> Iterator[tuple[Key, SeqNum, int, Value]]: ...
    @abstractmethod
    def freeze(self) -> list[tuple[Key, SeqNum, int, Value]]: ...
