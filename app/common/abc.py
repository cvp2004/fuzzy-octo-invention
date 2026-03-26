"""Abstract base classes defining core storage contracts.

Three contracts are defined here:

- :class:`StorageEngine` — async key-value store (put/get/delete/close)
- :class:`Serializable` — byte-level serialization (to_bytes/from_bytes)
- :class:`MemTable` — mutable in-memory sorted table (put/get/items/freeze)

These are **nominal** contracts (subclass to comply).  The existing
:class:`~app.types.BloomFilterProtocol` and
:class:`~app.types.KVIteratorProtocol` remain as **structural** contracts
(duck-typing via ``Protocol``).  A class may satisfy both — e.g.
``BloomFilter`` inherits ``Serializable`` *and* structurally matches
``BloomFilterProtocol``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import Self

from app.types import Key, SeqNum, Value


class StorageEngine(ABC):
    """Async key-value store contract.

    Any class that provides durable, crash-safe storage of byte key-value
    pairs should inherit from this ABC and implement all four methods.
    """

    @abstractmethod
    async def put(self, key: Key, value: Value) -> None:
        """Persist a key-value pair.

        Args:
            key: The lookup key (raw bytes).
            value: The value to associate with *key* (raw bytes).
        """

    @abstractmethod
    async def get(self, key: Key) -> Value | None:
        """Retrieve the value for *key*.

        Args:
            key: The lookup key.

        Returns:
            The stored value, or ``None`` if the key does not exist or
            has been deleted.
        """

    @abstractmethod
    async def delete(self, key: Key) -> None:
        """Delete *key* from the store.

        Args:
            key: The key to remove.
        """

    @abstractmethod
    async def close(self) -> None:
        """Flush pending data and release all resources.

        After ``close()`` returns, subsequent calls to :meth:`put`,
        :meth:`get`, or :meth:`delete` should raise an error.
        """


class Serializable(ABC):
    """Byte-level serialization contract.

    Any class that can round-trip its state through ``bytes`` should
    inherit from this ABC.  The ``from_bytes`` class method must
    reconstruct an equivalent instance from output of ``to_bytes``.
    """

    @abstractmethod
    def to_bytes(self) -> bytes:
        """Serialize this instance to a compact byte string.

        Returns:
            A ``bytes`` object that can be passed to :meth:`from_bytes`
            to reconstruct an equivalent instance.
        """

    @classmethod
    @abstractmethod
    def from_bytes(cls, data: bytes) -> Self:
        """Reconstruct an instance from serialized *data*.

        Args:
            data: Bytes previously produced by :meth:`to_bytes`.

        Returns:
            A new instance equivalent to the one that produced *data*.
        """


class MemTable(ABC):
    """Mutable in-memory sorted table contract.

    Defines the minimal interface that a writable memtable must expose:
    insert, point lookup, sorted iteration, and freeze-to-snapshot.
    """

    @abstractmethod
    def put(
        self, key: Key, seq: SeqNum, timestamp_ms: int, value: Value,
    ) -> None:
        """Insert or update *key* in the table.

        Args:
            key: The lookup key (raw bytes).
            seq: Monotonically increasing sequence number.
            timestamp_ms: Wall-clock timestamp in milliseconds.
            value: The value to store (or ``TOMBSTONE`` for deletions).
        """

    @abstractmethod
    def get(self, key: Key) -> tuple[SeqNum, Value] | None:
        """Look up *key* and return its latest value.

        Args:
            key: The lookup key.

        Returns:
            A ``(seq, value)`` tuple, or ``None`` if the key is not
            present in this table.
        """

    @abstractmethod
    def items(self) -> Iterator[tuple[Key, SeqNum, int, Value]]:
        """Yield all entries in ascending key order.

        Yields:
            Tuples of ``(key, seq, timestamp_ms, value)``.
        """

    @abstractmethod
    def freeze(self) -> list[tuple[Key, SeqNum, int, Value]]:
        """Return a sorted snapshot of all entries and mark the table as frozen.

        Returns:
            A list of ``(key, seq, timestamp_ms, value)`` tuples sorted
            by key ascending.

        Raises:
            SnapshotEmptyError: If the table contains no entries.
        """
