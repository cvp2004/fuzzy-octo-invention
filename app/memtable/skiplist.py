"""Concurrent skip list — O(log n) sorted data structure backing ActiveMemTable.

Uses fine-grained per-node locking for concurrent writes into
non-overlapping key ranges.  Reads (``get``, ``__iter__``) are
lock-free — they rely on ``fully_linked`` and ``marked`` boolean
flags being atomic under CPython's GIL.

This module is a private implementation detail of the memtable package.
External code should use :class:`ActiveMemTable` instead.
"""

from __future__ import annotations

import random
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Final

from app.common.errors import SkipListInsertError, SkipListKeyError
from app.types import TOMBSTONE, Key, SeqNum, Value

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_LEVEL: Final[int] = 16
"""Maximum skip list height — supports ~65 536 entries at P=0.5."""

PROBABILITY: Final[float] = 0.5
"""Geometric level distribution — each level half as likely as the one below."""

_MAX_RETRIES: Final[int] = 64
"""Safety bound on insert retries before raising SkipListInsertError."""


# ---------------------------------------------------------------------------
# _Node — private, never exposed outside this module
# ---------------------------------------------------------------------------


class _Node:
    """A single node in the skip list."""

    __slots__ = (
        "key", "seq", "timestamp_ms", "value",
        "forward", "lock", "marked", "fully_linked",
    )

    def __init__(
        self,
        key: Key,
        seq: SeqNum,
        timestamp_ms: int,
        value: Value,
        level: int,
    ) -> None:
        """Create a skip list node.

        Args:
            key: The lookup key for this entry.
            seq: Monotonically increasing sequence number.
            timestamp_ms: Wall-clock timestamp in milliseconds.
            value: The stored value (or ``TOMBSTONE`` for deletions).
            level: The randomly chosen height of this node, determining
                the number of forward pointers (``level + 1``).
        """
        self.key = key
        self.seq = seq
        self.timestamp_ms = timestamp_ms
        self.value = value
        self.forward: list[_Node | None] = [None] * (level + 1)
        self.lock = threading.Lock()
        self.marked = False
        self.fully_linked = False


# ---------------------------------------------------------------------------
# SkipList
# ---------------------------------------------------------------------------


class SkipList:
    """Thread-safe sorted map from :type:`Key` to ``(SeqNum, ts, Value)``.

    Concurrent writers lock only the predecessor nodes at the insertion
    boundary.  Readers acquire no locks — boolean flag reads are atomic
    under CPython's GIL.
    """

    def __init__(self) -> None:
        """Initialize an empty skip list with a sentinel head node.

        The head node spans all levels and is never visible to iterators.
        """
        self._head = _Node(key=b"", seq=0, timestamp_ms=0, value=b"", level=MAX_LEVEL)
        self._head.fully_linked = True
        self._level = 0
        self._size = 0
        self._size_lock = threading.Lock()
        self._count = 0
        self._count_lock = threading.Lock()

    # ── internal helpers ──────────────────────────────────────────────────

    @staticmethod
    def _random_level() -> int:
        """Return a random level with geometric distribution."""
        level = 0
        while random.random() < PROBABILITY and level < MAX_LEVEL:
            level += 1
        return level

    def _find(self, key: Key) -> tuple[list[_Node], list[_Node | None]]:
        """Traverse the list and return predecessors and successors at every level.

        Pure navigation — acquires no locks.
        """
        preds: list[_Node] = [self._head] * (MAX_LEVEL + 1)
        succs: list[_Node | None] = [None] * (MAX_LEVEL + 1)

        pred = self._head
        for level in range(self._level, -1, -1):
            curr = pred.forward[level]
            while curr is not None and curr.key < key:
                pred = curr
                curr = pred.forward[level]
            preds[level] = pred
            succs[level] = curr

        return preds, succs

    @staticmethod
    @contextmanager
    def _lock_nodes(nodes: list[_Node]) -> Iterator[None]:
        """Acquire locks on *nodes* in ascending ``id()`` order.

        Deterministic ordering prevents deadlock between concurrent inserts.
        """
        sorted_nodes = sorted(set(nodes), key=id)
        for node in sorted_nodes:
            node.lock.acquire()
        try:
            yield
        finally:
            for node in reversed(sorted_nodes):
                node.lock.release()

    # ── public API: put ───────────────────────────────────────────────────

    def put(self, key: Key, seq: SeqNum, timestamp_ms: int, value: Value) -> None:
        """Insert or update *key* in the skip list.

        If the key already exists (``fully_linked=True``, ``marked=False``),
        updates seq, timestamp_ms, and value in place.  Otherwise inserts
        a new node.

        Raises :class:`SkipListKeyError` if *key* is empty.
        Raises :class:`SkipListInsertError` if retries are exhausted.
        """
        if not key:
            raise SkipListKeyError("Key must not be empty")

        top_level = self._random_level()

        for _attempt in range(_MAX_RETRIES):
            preds, succs = self._find(key)

            # ── update path: key already exists ───────────────────────
            for lv in range(MAX_LEVEL + 1):
                node = succs[lv]
                if (
                    node is not None
                    and node.key == key
                    and node.fully_linked
                    and not node.marked
                ):
                    with node.lock:
                        if node.marked:
                            break  # concurrently deleted — retry
                        old_value_len = len(node.value)
                        node.seq = seq
                        node.timestamp_ms = timestamp_ms
                        node.value = value
                        with self._size_lock:
                            self._size += len(value) - old_value_len
                    return
                if node is None or node.key != key:
                    break

            # ── insert path ───────────────────────────────────────────
            pred_nodes = [preds[lv] for lv in range(top_level + 1)]

            with self._lock_nodes(pred_nodes):
                # Validate — predecessors still point to expected successors
                valid = True
                for lv in range(top_level + 1):
                    if preds[lv].marked or preds[lv].forward[lv] is not succs[lv]:
                        valid = False
                        break
                if not valid:
                    continue  # retry from scratch

                new_node = _Node(key, seq, timestamp_ms, value, top_level)

                for lv in range(top_level + 1):
                    new_node.forward[lv] = succs[lv]
                    preds[lv].forward[lv] = new_node

                # Last step — now visible to readers
                new_node.fully_linked = True

                if top_level > self._level:
                    self._level = top_level

                with self._size_lock:
                    self._size += len(key) + len(value)
                with self._count_lock:
                    self._count += 1

            return

        raise SkipListInsertError(
            f"put() failed after {_MAX_RETRIES} retries for key={key!r}"
        )

    # ── public API: get ───────────────────────────────────────────────────

    def get(self, key: Key) -> tuple[SeqNum, Value] | None:
        """Look up *key* without acquiring any lock.

        Returns ``(seq, value)`` or ``None``.  Does **not** return
        ``timestamp_ms`` — only ``seq`` is needed for MVCC ordering.
        """
        _, succs = self._find(key)
        node = succs[0]
        if (
            node is not None
            and node.key == key
            and node.fully_linked
            and not node.marked
        ):
            return (node.seq, node.value)
        return None

    # ── public API: delete ────────────────────────────────────────────────

    def delete(self, key: Key, seq: SeqNum, timestamp_ms: int) -> bool:
        """Logically delete *key* by writing a ``TOMBSTONE``.

        Returns ``True`` if the key existed and was marked, ``False`` if
        the key was not found (a tombstone is still inserted so the
        delete propagates to SSTables).
        """
        if not key:
            raise SkipListKeyError("Key must not be empty")

        _, succs = self._find(key)
        node = succs[0]

        if node is None or node.key != key or not node.fully_linked:
            # Key not found — insert a tombstone so delete propagates to SSTables
            self.put(key, seq, timestamp_ms, TOMBSTONE)
            return False

        with node.lock:
            if node.marked:
                return False
            node.marked = True
            node.seq = seq
            node.timestamp_ms = timestamp_ms
            node.value = TOMBSTONE

        with self._count_lock:
            self._count -= 1

        return True

    # ── public API: iteration ─────────────────────────────────────────────

    def __iter__(self) -> Iterator[tuple[Key, SeqNum, int, Value]]:
        """Yield ``(key, seq, timestamp_ms, value)`` in sorted key order.

        Lock-free.  Skips nodes that are not ``fully_linked`` or are
        ``marked``.  Safe to call concurrently with ``put()`` and
        ``delete()`` — boolean flag reads are atomic under the GIL.
        """
        curr = self._head.forward[0]
        while curr is not None:
            if curr.fully_linked and not curr.marked:
                yield curr.key, curr.seq, curr.timestamp_ms, curr.value
            curr = curr.forward[0]

    def snapshot(self) -> list[tuple[Key, SeqNum, int, Value]]:
        """Materialise the iterator as a sorted list for freezing.

        Used by :meth:`ActiveMemTable.freeze` to produce a stable
        point-in-time copy.
        """
        data = list(self)
        return data

    # ── public API: properties ────────────────────────────────────────────

    @property
    def size_bytes(self) -> int:
        """Estimated size in bytes (sum of key + value lengths)."""
        with self._size_lock:
            return self._size

    @property
    def count(self) -> int:
        """Number of visible (non-deleted) entries."""
        with self._count_lock:
            return self._count
