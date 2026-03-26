"""KWayMergeIterator — merge N sorted iterators into one globally sorted stream.

Used by:
  - compaction: merge multiple L0 SSTables into one L1 file
  - range queries (future): merge memtable + SSTable results

Ordering:   sorted by key ascending
Dedup:      for equal keys, highest seq wins — lower-seq records dropped
Tombstones: configurable via skip_tombstones and seq_cutoff
"""

from __future__ import annotations

import heapq
from collections.abc import Iterator
from dataclasses import dataclass

from app.types import TOMBSTONE, Key, SeqNum, Value


@dataclass(order=False)
class MergeRecord:
    """One record in the merge heap.

    Attributes:
        key: The lookup key.
        seq: Sequence number for MVCC ordering.
        timestamp_ms: Wall-clock timestamp in milliseconds.
        value: The stored value (or ``TOMBSTONE`` for deletions).
        source_idx: Index of the source iterator that produced this record.
    """

    key: Key
    seq: SeqNum
    timestamp_ms: int
    value: Value
    source_idx: int

    def __lt__(self, other: MergeRecord) -> bool:
        """Compare records for min-heap ordering.

        Sorts by key ascending. For equal keys, the record with the
        higher sequence number (newer) is considered smaller so it is
        popped first.

        Args:
            other: The record to compare against.

        Returns:
            True if this record should be popped before *other*.
        """
        # Min-heap: smallest key first.
        # For equal keys: highest seq first (newest = highest priority).
        if self.key != other.key:
            return self.key < other.key
        return self.seq > other.seq

    def __eq__(self, other: object) -> bool:
        """Check equality by key and sequence number.

        Args:
            other: The object to compare against.

        Returns:
            True if *other* is a ``MergeRecord`` with the same key and seq.
            ``NotImplemented`` if *other* is not a ``MergeRecord``.
        """
        if not isinstance(other, MergeRecord):
            return NotImplemented
        return self.key == other.key and self.seq == other.seq


class KWayMergeIterator:
    """Merge N sorted iterators into one globally sorted stream.

    Used by compaction to merge multiple SSTables and by future range
    queries to merge memtable + SSTable results.

    Args:
        iterators: Source iterators, each yielding ``(Key, SeqNum, int,
            Value)`` tuples in strictly ascending key order.
        seq_cutoff: Tombstones with ``seq < seq_cutoff`` are dropped
            entirely (garbage collected). Pass ``0`` to preserve all
            tombstones (safe default).
        skip_tombstones: If True, tombstoned keys are omitted from
            output (useful for range queries). If False, tombstones
            above *seq_cutoff* are preserved (compaction).
        deduplicate: If True (default), only the highest-seq record per
            key is yielded.
    """

    def __init__(
        self,
        iterators: list[Iterator[tuple[Key, SeqNum, int, Value]]],
        seq_cutoff: SeqNum = 0,
        skip_tombstones: bool = False,
        deduplicate: bool = True,
    ) -> None:
        """Initialize the k-way merge iterator.

        Args:
            iterators: Source iterators, each yielding ``(key, seq,
                timestamp_ms, value)`` tuples in ascending key order.
            seq_cutoff: Tombstones with ``seq < seq_cutoff`` are garbage-
                collected (dropped). Pass ``0`` to preserve all.
            skip_tombstones: If True, tombstone records are omitted from
                the output entirely (useful for range queries).
            deduplicate: If True, only the highest-seq record per key is
                yielded.
        """
        self._iters = list(iterators)
        self._seq_cutoff = seq_cutoff
        self._skip_tombstones = skip_tombstones
        self._deduplicate = deduplicate
        self._heap: list[MergeRecord] = []
        self._last_key: Key | None = None

    def _init_heap(self) -> None:
        """Seed the min-heap with the first record from each source iterator."""
        for idx in range(len(self._iters)):
            self._push_next(idx)

    def _push_next(self, source_idx: int) -> None:
        """Advance source iterator *source_idx* and push its next record onto the heap.

        Args:
            source_idx: Index into ``self._iters`` identifying which
                source to advance. Silently does nothing if that source
                is exhausted.
        """
        try:
            key, seq, ts, value = next(self._iters[source_idx])
            heapq.heappush(
                self._heap,
                MergeRecord(
                    key=key,
                    seq=seq,
                    timestamp_ms=ts,
                    value=value,
                    source_idx=source_idx,
                ),
            )
        except StopIteration:
            pass

    def __iter__(self) -> Iterator[tuple[Key, SeqNum, int, Value]]:
        """Yield ``(key, seq, timestamp_ms, value)`` in globally sorted order.

        Deduplication, tombstone filtering, and garbage collection are
        applied according to the constructor parameters.

        Yields:
            Tuples of ``(key, seq, timestamp_ms, value)`` in ascending
            key order with at most one record per key when *deduplicate*
            is True.
        """
        self._init_heap()

        while self._heap:
            rec = heapq.heappop(self._heap)

            # Dedup: skip lower-seq records for an already-yielded key
            if self._deduplicate and rec.key == self._last_key:
                self._push_next(rec.source_idx)
                continue

            self._last_key = rec.key

            # Tombstone handling
            if rec.value == TOMBSTONE:
                if self._skip_tombstones:
                    self._push_next(rec.source_idx)
                    continue
                if rec.seq < self._seq_cutoff:
                    self._push_next(rec.source_idx)
                    continue

            yield (rec.key, rec.seq, rec.timestamp_ms, rec.value)
            self._push_next(rec.source_idx)
