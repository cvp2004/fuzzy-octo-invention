"""Type stub for app.types — type aliases, constants, enums, and protocols."""

import enum
from collections.abc import Iterator
from typing import Final, Protocol, Self, runtime_checkable

class OpType(enum.IntEnum):
    PUT = 1
    DELETE = 2

type Key = bytes
type Value = bytes
type SeqNum = int
type Offset = int
type BlockSize = int
type FileID = str
type Level = int
type SnapshotID = str
type TableID = str

TOMBSTONE: Final[bytes]
BLOCK_SIZE_DEFAULT: Final[int]
MAX_MEMTABLE_SIZE: Final[int]
L0_COMPACTION_THRESHOLD: Final[int]
IMMUTABLE_QUEUE_MAX: Final[int]
COMPACTION_CHECK_INTERVAL: Final[float]
PARALLELISM_MODE: Final[str]

@runtime_checkable
class BloomFilterProtocol(Protocol):
    def add(self, key: Key) -> None: ...
    def may_contain(self, key: Key) -> bool: ...
    def to_bytes(self) -> bytes: ...
    @classmethod
    def from_bytes(cls, data: bytes) -> Self: ...

@runtime_checkable
class KVIteratorProtocol(Protocol):
    def seek(self, key: Key) -> None: ...
    def __iter__(self) -> Iterator[tuple[Key, Value]]: ...
    def __next__(self) -> tuple[Key, Value]: ...
