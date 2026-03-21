"""Tests for app.cache.block — BlockCache."""

from __future__ import annotations

import threading

from app.cache.block import BlockCache


class TestBlockCache:
    def test_put_and_get(self) -> None:
        cache = BlockCache(maxsize=10)
        cache.put("file1", 0, b"block_data")
        assert cache.get("file1", 0) == b"block_data"

    def test_get_missing(self) -> None:
        cache = BlockCache(maxsize=10)
        assert cache.get("nope", 0) is None

    def test_invalidate(self) -> None:
        cache = BlockCache(maxsize=10)
        cache.put("file1", 0, b"a")
        cache.put("file1", 4096, b"b")
        cache.put("file2", 0, b"c")

        cache.invalidate("file1")

        assert cache.get("file1", 0) is None
        assert cache.get("file1", 4096) is None
        assert cache.get("file2", 0) == b"c"

    def test_lru_eviction(self) -> None:
        cache = BlockCache(maxsize=2)
        cache.put("f", 0, b"first")
        cache.put("f", 1, b"second")
        cache.put("f", 2, b"third")  # should evict "first"

        assert cache.get("f", 0) is None
        assert cache.get("f", 1) == b"second"
        assert cache.get("f", 2) == b"third"

    def test_thread_safety(self) -> None:
        cache = BlockCache(maxsize=1000)
        errors: list[str] = []

        def writer(tid: int) -> None:
            for i in range(100):
                cache.put(f"t{tid}", i, f"data_{tid}_{i}".encode())

        def reader(tid: int) -> None:
            for i in range(100):
                cache.get(f"t{tid}", i)

        threads = []
        for t in range(4):
            threads.append(threading.Thread(target=writer, args=(t,)))
            threads.append(threading.Thread(target=reader, args=(t,)))

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
