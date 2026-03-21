"""Tests for app.sstable.writer — SSTableWriter."""

from __future__ import annotations

from pathlib import Path

import pytest

from app.common.errors import SSTableWriteError
from app.sstable.writer import SSTableWriter


@pytest.fixture()
def sst_dir(tmp_path: Path) -> Path:
    return tmp_path / "sst" / "test_file"


class TestSSTableWriter:
    async def test_basic_write(self, sst_dir: Path) -> None:
        writer = SSTableWriter(
            directory=sst_dir,
            file_id="test1",
            snapshot_id="snap1",
            level=0,
        )
        writer.put(b"aaa", 1, 100, b"val_a")
        writer.put(b"bbb", 2, 200, b"val_b")
        writer.put(b"ccc", 3, 300, b"val_c")

        meta = await writer.finish()

        assert meta.record_count == 3
        assert meta.min_key == b"aaa"
        assert meta.max_key == b"ccc"
        assert meta.seq_min == 1
        assert meta.seq_max == 3

    async def test_produces_four_files(self, sst_dir: Path) -> None:
        writer = SSTableWriter(
            directory=sst_dir,
            file_id="test2",
            snapshot_id="snap2",
            level=0,
        )
        writer.put(b"key", 1, 100, b"value")
        await writer.finish()

        assert (sst_dir / "data.bin").exists()
        assert (sst_dir / "index.bin").exists()
        assert (sst_dir / "filter.bin").exists()
        assert (sst_dir / "meta.json").exists()

    async def test_out_of_order_key_raises(self, sst_dir: Path) -> None:
        writer = SSTableWriter(
            directory=sst_dir,
            file_id="test3",
            snapshot_id="snap3",
            level=0,
        )
        writer.put(b"bbb", 1, 100, b"v1")

        with pytest.raises(SSTableWriteError, match="ascending"):
            writer.put(b"aaa", 2, 200, b"v2")

    async def test_duplicate_key_raises(self, sst_dir: Path) -> None:
        writer = SSTableWriter(
            directory=sst_dir,
            file_id="test4",
            snapshot_id="snap4",
            level=0,
        )
        writer.put(b"key", 1, 100, b"v1")

        with pytest.raises(SSTableWriteError, match="ascending"):
            writer.put(b"key", 2, 200, b"v2")

    def test_finish_sync(self, sst_dir: Path) -> None:
        writer = SSTableWriter(
            directory=sst_dir,
            file_id="test5",
            snapshot_id="snap5",
            level=1,
        )
        writer.put(b"x", 1, 100, b"y")
        meta = writer.finish_sync()

        assert meta.record_count == 1
        assert (sst_dir / "meta.json").exists()

    async def test_multiple_blocks(self, sst_dir: Path) -> None:
        writer = SSTableWriter(
            directory=sst_dir,
            file_id="test6",
            snapshot_id="snap6",
            level=0,
            block_size=64,  # tiny blocks to force multiple
        )
        for i in range(20):
            key = f"key_{i:04d}".encode()
            writer.put(key, i + 1, 100, b"v" * 20)

        meta = await writer.finish()
        assert meta.block_count > 1
        assert meta.record_count == 20
