"""SSTableReader — read-only access to one SSTable via mmap.

Provides point lookups: bloom → sparse index bisect → mmap scan.
"""

from __future__ import annotations

import asyncio
import mmap
from pathlib import Path

from app.bloom.filter import BloomFilter
from app.cache.block import BlockCache
from app.common.encoding import iter_block
from app.index.sparse import SparseIndex
from app.observability import get_logger
from app.sstable.meta import SSTableMeta
from app.types import FileID, Key, Level, SeqNum, Value

logger = get_logger(__name__)


class SSTableReader:
    """Read-only access to one SSTable."""

    def __init__(
        self,
        directory: Path,
        file_id: FileID,
        meta: SSTableMeta,
        index: SparseIndex,
        bloom: BloomFilter,
        cache: BlockCache | None,
        mm: mmap.mmap,
        fd: int,
    ) -> None:
        self._dir = directory
        self._file_id = file_id
        self._meta = meta
        self._index = index
        self._bloom = bloom
        self._cache = cache
        self._mm = mm
        self._fd = fd

    @property
    def meta(self) -> SSTableMeta:
        """Return the SSTable metadata."""
        return self._meta

    @property
    def file_id(self) -> FileID:
        """Return the file ID."""
        return self._file_id

    # ── factory ─────────────────────────────────────────────────────────

    @classmethod
    async def open(
        cls,
        directory: Path,
        file_id: FileID,
        cache: BlockCache | None = None,
        level: Level = 0,
    ) -> SSTableReader:
        """Open an SSTable for reading. Index + bloom loaded concurrently."""
        meta_path = directory / "meta.json"
        meta = SSTableMeta.from_json(meta_path.read_text(encoding="utf-8"))

        # Load index + bloom concurrently
        index_data, bloom_data = await asyncio.gather(
            asyncio.to_thread(_read_file, directory / meta.index_file),
            asyncio.to_thread(_read_file, directory / meta.filter_file),
        )

        index = SparseIndex.from_bytes(index_data)
        bloom = BloomFilter.from_bytes(bloom_data)

        # mmap the data file
        data_path = directory / meta.data_file
        import os

        fd = os.open(str(data_path), os.O_RDONLY)
        size = os.fstat(fd).st_size
        if size == 0:
            mm = mmap.mmap(-1, 1)  # empty placeholder
        else:
            mm = mmap.mmap(fd, 0, access=mmap.ACCESS_READ)

        reader = cls(
            directory=directory,
            file_id=file_id,
            meta=meta,
            index=index,
            bloom=bloom,
            cache=cache,
            mm=mm,
            fd=fd,
        )

        logger.info(
            "SSTable opened",
            file_id=file_id,
            records=meta.record_count,
            level=level,
        )
        return reader

    # ── point lookup ────────────────────────────────────────────────────

    def get(self, key: Key) -> tuple[SeqNum, int, Value] | None:
        """Look up *key*. Returns ``(seq, timestamp_ms, value)`` or None.

        Flow: bloom check → sparse index bisect → block scan.
        """
        # 1. Bloom filter — fast negative
        if not self._bloom.may_contain(key):
            return None

        # 2. Sparse index — find candidate block
        block_offset = self._index.floor_offset(key)
        if block_offset is None:
            return None

        # 3. Determine block end (next block offset or EOF)
        block_end = self._find_block_end(block_offset)

        # 4. Scan the block
        mv = memoryview(self._mm)
        best: tuple[SeqNum, int, Value] | None = None
        for rec in iter_block(mv, block_offset, block_end):
            if rec.key == key:
                if best is None or rec.seq > best[0]:
                    best = (rec.seq, rec.timestamp_ms, rec.value)
            elif rec.key > key:
                break  # keys are sorted, no point continuing

        return best

    def scan_all(self) -> list[tuple[Key, SeqNum, int, Value]]:
        """Return all records in this SSTable as a sorted list.

        Used by the ``disk`` command to display SSTable contents.
        """
        if self._meta.size_bytes == 0:
            return []
        mv = memoryview(self._mm)
        return [
            (rec.key, rec.seq, rec.timestamp_ms, rec.value)
            for rec in iter_block(mv, 0, self._meta.size_bytes)
        ]

    def _find_block_end(self, block_offset: int) -> int:
        """Return the end offset of the block starting at *block_offset*."""
        next_offset = self._index.next_offset_after(block_offset)
        if next_offset is not None:
            return next_offset
        return self._meta.size_bytes

    # ── cleanup ─────────────────────────────────────────────────────────

    def close(self) -> None:
        """Release mmap and file descriptor. Never raises."""
        import contextlib
        import os

        with contextlib.suppress(Exception):
            self._mm.close()
        with contextlib.suppress(Exception):
            os.close(self._fd)

        logger.debug("SSTable closed", file_id=self._file_id)


def _read_file(path: Path) -> bytes:
    """Read an entire file — used via asyncio.to_thread."""
    return path.read_bytes()
