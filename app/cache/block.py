"""BlockCache — thread-safe LRU cache for SSTable data blocks.

Keyed by ``(file_id, block_offset)`` so blocks from different
SSTables share the same pool.
"""

from __future__ import annotations

import threading

from cachetools import LRUCache

from app.types import FileID, Offset


class BlockCache:
    """Thread-safe LRU block cache."""

    def __init__(self, maxsize: int = 256) -> None:
        self._cache: LRUCache[tuple[FileID, Offset], bytes] = LRUCache(
            maxsize=maxsize,
        )
        self._lock = threading.Lock()

    def get(self, file_id: FileID, offset: Offset) -> bytes | None:
        """Return cached block or None."""
        with self._lock:
            result: bytes | None = self._cache.get((file_id, offset))
            return result

    def put(self, file_id: FileID, offset: Offset, block: bytes) -> None:
        """Insert a block into the cache."""
        with self._lock:
            self._cache[(file_id, offset)] = block

    def invalidate(self, file_id: FileID) -> None:
        """Evict all blocks belonging to *file_id*."""
        with self._lock:
            keys_to_remove = [k for k in self._cache if k[0] == file_id]
            for k in keys_to_remove:
                del self._cache[k]
