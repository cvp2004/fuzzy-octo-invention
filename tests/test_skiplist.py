"""Unit tests for SkipList — concurrent sorted data structure."""

from __future__ import annotations

import threading

import pytest

from app.memtable.skiplist import SkipList
from app.types import TOMBSTONE


class TestSkipListPutGet:
    """Basic insert and lookup."""

    def test_put_then_get(self) -> None:
        sl = SkipList()
        sl.put(b"hello", seq=1, timestamp_ms=100, value=b"world")
        result = sl.get(b"hello")
        assert result == (1, b"world")

    def test_get_missing_key(self) -> None:
        sl = SkipList()
        assert sl.get(b"nope") is None

    def test_get_returns_no_timestamp(self) -> None:
        sl = SkipList()
        sl.put(b"k", seq=1, timestamp_ms=999, value=b"v")
        result = sl.get(b"k")
        assert result is not None
        assert len(result) == 2  # (seq, value) only
        assert result == (1, b"v")

    def test_update_existing_key(self) -> None:
        sl = SkipList()
        sl.put(b"k", seq=1, timestamp_ms=100, value=b"old")
        sl.put(b"k", seq=2, timestamp_ms=200, value=b"new")
        result = sl.get(b"k")
        assert result == (2, b"new")

    def test_multiple_keys(self) -> None:
        sl = SkipList()
        sl.put(b"b", seq=1, timestamp_ms=100, value=b"2")
        sl.put(b"a", seq=2, timestamp_ms=100, value=b"1")
        sl.put(b"c", seq=3, timestamp_ms=100, value=b"3")
        assert sl.get(b"a") == (2, b"1")
        assert sl.get(b"b") == (1, b"2")
        assert sl.get(b"c") == (3, b"3")

    def test_empty_key_raises(self) -> None:
        sl = SkipList()
        with pytest.raises(Exception):  # SkipListKeyError
            sl.put(b"", seq=1, timestamp_ms=100, value=b"v")


class TestSkipListDelete:
    """Logical delete with tombstones."""

    def test_delete_existing_key(self) -> None:
        sl = SkipList()
        sl.put(b"k", seq=1, timestamp_ms=100, value=b"v")
        existed = sl.delete(b"k", seq=2, timestamp_ms=200)
        assert existed is True
        assert sl.get(b"k") is None

    def test_delete_nonexistent_key(self) -> None:
        sl = SkipList()
        existed = sl.delete(b"ghost", seq=1, timestamp_ms=100)
        assert existed is False
        # Tombstone should be inserted
        # The node is marked, so get() returns None
        # But a tombstone entry should be visible via iteration
        # (it's inserted via put(), not marked)
        items = list(sl)
        assert len(items) == 1
        assert items[0][0] == b"ghost"
        assert items[0][3] == TOMBSTONE

    def test_delete_makes_iter_skip(self) -> None:
        sl = SkipList()
        sl.put(b"a", seq=1, timestamp_ms=100, value=b"1")
        sl.put(b"b", seq=2, timestamp_ms=100, value=b"2")
        sl.delete(b"a", seq=3, timestamp_ms=200)
        keys = [k for k, _, _, _ in sl]
        assert b"a" not in keys
        assert b"b" in keys


class TestSkipListIteration:
    """Sorted iteration and snapshot."""

    def test_iter_sorted_order(self) -> None:
        sl = SkipList()
        sl.put(b"c", seq=1, timestamp_ms=100, value=b"3")
        sl.put(b"a", seq=2, timestamp_ms=100, value=b"1")
        sl.put(b"b", seq=3, timestamp_ms=100, value=b"2")

        keys = [k for k, _, _, _ in sl]
        assert keys == [b"a", b"b", b"c"]

    def test_iter_includes_timestamp(self) -> None:
        sl = SkipList()
        sl.put(b"k", seq=1, timestamp_ms=42, value=b"v")
        items = list(sl)
        assert len(items) == 1
        key, seq, ts, value = items[0]
        assert ts == 42  # timestamp_ms IS included in iteration

    def test_snapshot_returns_sorted_list(self) -> None:
        sl = SkipList()
        sl.put(b"b", seq=1, timestamp_ms=100, value=b"2")
        sl.put(b"a", seq=2, timestamp_ms=100, value=b"1")

        snap = sl.snapshot()
        assert isinstance(snap, list)
        assert [k for k, _, _, _ in snap] == [b"a", b"b"]

    def test_snapshot_not_affected_by_later_inserts(self) -> None:
        sl = SkipList()
        sl.put(b"a", seq=1, timestamp_ms=100, value=b"1")
        snap = sl.snapshot()

        sl.put(b"b", seq=2, timestamp_ms=100, value=b"2")
        assert len(snap) == 1  # snapshot is a copy


class TestSkipListSizeBytes:
    """Size tracking."""

    def test_size_after_insert(self) -> None:
        sl = SkipList()
        sl.put(b"key", seq=1, timestamp_ms=100, value=b"val")
        assert sl.size_bytes == len(b"key") + len(b"val")

    def test_size_after_update(self) -> None:
        sl = SkipList()
        sl.put(b"k", seq=1, timestamp_ms=100, value=b"short")
        sl.put(b"k", seq=2, timestamp_ms=200, value=b"longvalue")
        # size = key_len + new_value_len
        assert sl.size_bytes == len(b"k") + len(b"longvalue")

    def test_size_multiple_keys(self) -> None:
        sl = SkipList()
        sl.put(b"a", seq=1, timestamp_ms=100, value=b"1")
        sl.put(b"b", seq=2, timestamp_ms=100, value=b"2")
        assert sl.size_bytes == 4  # 1+1 + 1+1

    def test_count(self) -> None:
        sl = SkipList()
        sl.put(b"a", seq=1, timestamp_ms=100, value=b"1")
        sl.put(b"b", seq=2, timestamp_ms=100, value=b"2")
        assert sl.count == 2
        sl.delete(b"a", seq=3, timestamp_ms=200)
        assert sl.count == 1


class TestSkipListConcurrency:
    """Thread-safety tests."""

    def test_concurrent_inserts(self) -> None:
        sl = SkipList()
        errors: list[Exception] = []

        def insert_range(start: int, end: int) -> None:
            try:
                for i in range(start, end):
                    sl.put(f"key{i:04d}".encode(), seq=i, timestamp_ms=100, value=f"val{i}".encode())
            except Exception as e:
                errors.append(e)

        threads = [
            threading.Thread(target=insert_range, args=(0, 250)),
            threading.Thread(target=insert_range, args=(250, 500)),
            threading.Thread(target=insert_range, args=(500, 750)),
            threading.Thread(target=insert_range, args=(750, 1000)),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors, f"Errors during concurrent insert: {errors}"

        # All entries present
        items = list(sl)
        assert len(items) == 1000

        # Sorted order
        keys = [k for k, _, _, _ in items]
        assert keys == sorted(keys)

    def test_concurrent_reads_and_writes(self) -> None:
        sl = SkipList()
        stop = threading.Event()
        read_count = 0
        errors: list[Exception] = []

        def writer() -> None:
            try:
                for i in range(500):
                    sl.put(f"k{i}".encode(), seq=i, timestamp_ms=100, value=b"v")
            except Exception as e:
                errors.append(e)

        def reader() -> None:
            nonlocal read_count
            try:
                while not stop.is_set():
                    sl.get(b"k0")
                    read_count += 1
            except Exception as e:
                errors.append(e)

        writer_t = threading.Thread(target=writer)
        reader_t = threading.Thread(target=reader)
        reader_t.start()
        writer_t.start()
        writer_t.join()
        stop.set()
        reader_t.join()

        assert not errors
        assert read_count > 0
