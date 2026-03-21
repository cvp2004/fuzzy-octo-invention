"""Unit tests for MemTableManager."""

from __future__ import annotations

import threading
from pathlib import Path

import pytest

from app.engine.config import LSMConfig
from app.engine.memtable_manager import MemTableManager
from app.types import TOMBSTONE, OpType
from app.wal.writer import WALEntry


def _entry(seq: int, key: bytes = b"k", value: bytes = b"v") -> WALEntry:
    return WALEntry(seq=seq, timestamp_ms=1000, op=OpType.PUT, key=key, value=value)


@pytest.fixture()
def mgr(test_config: LSMConfig) -> MemTableManager:
    """MemTableManager with default test config."""
    return MemTableManager(test_config)


def _small_config(tmp_path: Path) -> LSMConfig:
    """Config with 1MB size limit for easy freeze triggering (prod mode)."""
    cfg = LSMConfig.load(tmp_path / "data")
    cfg.set("env", "prod")
    cfg.set("max_memtable_size_mb", 1)
    return cfg


class TestMemTableManagerPutGet:
    """Basic write + read."""

    def test_put_then_get(self, mgr: MemTableManager) -> None:
        mgr.put(b"name", seq=1, timestamp_ms=100, value=b"alice")
        assert mgr.get(b"name") == (1, b"alice")

    def test_get_missing(self, mgr: MemTableManager) -> None:
        assert mgr.get(b"nope") is None

    def test_tombstone_returned_raw(self, mgr: MemTableManager) -> None:
        mgr.put(b"k", seq=1, timestamp_ms=100, value=TOMBSTONE)
        result = mgr.get(b"k")
        assert result is not None
        assert result[1] == TOMBSTONE

    def test_put_multiple_keys(self, mgr: MemTableManager) -> None:
        mgr.put(b"a", seq=1, timestamp_ms=100, value=b"1")
        mgr.put(b"b", seq=2, timestamp_ms=200, value=b"2")
        assert mgr.get(b"a") == (1, b"1")
        assert mgr.get(b"b") == (2, b"2")


class TestMemTableManagerGetOrder:
    """get() checks active first, then immutable queue newest→oldest."""

    def test_active_shadows_immutable(self, tmp_path: Path) -> None:
        cfg = _small_config(tmp_path)
        mgr = MemTableManager(cfg)

        big_value = b"x" * (1024 * 1024)  # 1MB = threshold
        mgr.put(b"k", seq=1, timestamp_ms=100, value=big_value)
        with mgr._write_lock:
            mgr.maybe_freeze()

        mgr.put(b"k", seq=2, timestamp_ms=200, value=b"new")
        result = mgr.get(b"k")
        assert result == (2, b"new")

    def test_newer_immutable_shadows_older(self, tmp_path: Path) -> None:
        cfg = _small_config(tmp_path)
        mgr = MemTableManager(cfg)

        big_value = b"x" * (1024 * 1024)
        mgr.put(b"k", seq=1, timestamp_ms=100, value=big_value)
        with mgr._write_lock:
            mgr.maybe_freeze()

        mgr.put(b"k", seq=2, timestamp_ms=200, value=b"y" * (1024 * 1024))
        with mgr._write_lock:
            mgr.maybe_freeze()

        result = mgr.get(b"k")
        assert result is not None
        assert result[0] == 2


class TestMemTableManagerFreeze:
    """Freeze trigger and backpressure."""

    def test_freeze_below_threshold(self, mgr: MemTableManager) -> None:
        mgr.put(b"small", seq=1, timestamp_ms=100, value=b"data")
        with mgr._write_lock:
            result = mgr.maybe_freeze()
        assert result is None
        assert mgr.queue_len() == 0

    def test_freeze_at_size_threshold(self, tmp_path: Path) -> None:
        cfg = _small_config(tmp_path)
        mgr = MemTableManager(cfg)

        big_value = b"x" * (1024 * 1024)
        mgr.put(b"k", seq=1, timestamp_ms=100, value=big_value)

        with mgr._write_lock:
            snapshot = mgr.maybe_freeze()

        assert snapshot is not None
        assert mgr.queue_len() == 1
        assert mgr.size_bytes == 0

    def test_freeze_at_entry_count_threshold(self, tmp_path: Path) -> None:
        cfg = LSMConfig.load(tmp_path / "data")
        cfg.set("max_memtable_entries", 3)  # very low threshold
        mgr = MemTableManager(cfg)

        mgr.put(b"a", seq=1, timestamp_ms=100, value=b"1")
        mgr.put(b"b", seq=2, timestamp_ms=100, value=b"2")
        mgr.put(b"c", seq=3, timestamp_ms=100, value=b"3")

        with mgr._write_lock:
            snapshot = mgr.maybe_freeze()

        assert snapshot is not None
        assert mgr.queue_len() == 1

    def test_new_writes_after_freeze(self, tmp_path: Path) -> None:
        cfg = _small_config(tmp_path)
        mgr = MemTableManager(cfg)

        big_value = b"x" * (1024 * 1024)
        mgr.put(b"old", seq=1, timestamp_ms=100, value=big_value)
        with mgr._write_lock:
            mgr.maybe_freeze()

        mgr.put(b"new", seq=2, timestamp_ms=200, value=b"fresh")
        assert mgr.get(b"new") == (2, b"fresh")
        assert mgr.get(b"old") is not None


class TestMemTableManagerQueueOps:
    """peek_oldest, pop_oldest, queue_len."""

    def test_peek_oldest(self, tmp_path: Path) -> None:
        cfg = _small_config(tmp_path)
        mgr = MemTableManager(cfg)

        big_value = b"x" * (1024 * 1024)
        mgr.put(b"k", seq=1, timestamp_ms=100, value=big_value)
        with mgr._write_lock:
            mgr.maybe_freeze()

        assert mgr.peek_oldest() is not None
        assert mgr.queue_len() == 1

    def test_pop_oldest(self, tmp_path: Path) -> None:
        cfg = _small_config(tmp_path)
        mgr = MemTableManager(cfg)

        big_value = b"x" * (1024 * 1024)
        mgr.put(b"k", seq=1, timestamp_ms=100, value=big_value)
        with mgr._write_lock:
            mgr.maybe_freeze()

        mgr.pop_oldest()
        assert mgr.queue_len() == 0

    def test_peek_empty(self, mgr: MemTableManager) -> None:
        assert mgr.peek_oldest() is None


class TestMemTableManagerBackpressure:
    """Backpressure when queue is full."""

    def test_pop_unblocks_freeze(self, tmp_path: Path) -> None:
        cfg = _small_config(tmp_path)
        cfg.set("immutable_queue_max_len", 2)  # small for fast test
        mgr = MemTableManager(cfg)
        big_value = b"x" * (1024 * 1024)

        for i in range(2):
            mgr.put(f"k{i}".encode(), seq=i + 1, timestamp_ms=100, value=big_value)
            with mgr._write_lock:
                mgr.maybe_freeze()

        assert mgr.queue_len() == 2

        freeze_done = threading.Event()
        mgr.put(b"overflow", seq=100, timestamp_ms=100, value=big_value)

        def do_freeze() -> None:
            with mgr._write_lock:
                mgr.maybe_freeze()
            freeze_done.set()

        t = threading.Thread(target=do_freeze)
        t.start()

        mgr.pop_oldest()
        freeze_done.wait(timeout=5.0)
        assert freeze_done.is_set()
        t.join()


class TestMemTableManagerRestore:
    """WAL replay into memtable."""

    def test_restore(self, mgr: MemTableManager) -> None:
        entries = [
            _entry(seq=1, key=b"a", value=b"1"),
            _entry(seq=2, key=b"b", value=b"2"),
            _entry(seq=3, key=b"c", value=b"3"),
        ]
        mgr.restore(entries)

        assert mgr.get(b"a") == (1, b"1")
        assert mgr.get(b"b") == (2, b"2")
        assert mgr.get(b"c") == (3, b"3")

    def test_restore_empty(self, mgr: MemTableManager) -> None:
        mgr.restore([])
        assert mgr.active_metadata.entry_count == 0


class TestMemTableManagerMetadata:
    """Metadata access."""

    def test_active_metadata(self, mgr: MemTableManager) -> None:
        mgr.put(b"k", seq=1, timestamp_ms=100, value=b"v")
        m = mgr.active_metadata
        assert m.entry_count == 1
        assert m.seq_first == 1

    def test_immutable_metadata(self, tmp_path: Path) -> None:
        cfg = _small_config(tmp_path)
        mgr = MemTableManager(cfg)

        big_value = b"x" * (1024 * 1024)
        mgr.put(b"k", seq=1, timestamp_ms=100, value=big_value)
        with mgr._write_lock:
            mgr.maybe_freeze()

        metas = mgr.immutable_metadata
        assert len(metas) == 1
        assert metas[0].entry_count == 1
