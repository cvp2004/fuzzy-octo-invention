"""Async tests for WALManager."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from app.engine.wal_manager import WALManager
from app.wal.writer import WALEntry

from tests.conftest import make_entry


@pytest.fixture()
def wal_manager(wal_path: Path) -> WALManager:
    """Return a WALManager backed by a temp WAL file."""
    return WALManager.open(wal_path)


class TestWALManagerAppend:
    """Tests for async append."""

    async def test_append_does_not_block(self, wal_manager: WALManager) -> None:
        entry = make_entry(seq=1)
        # Should complete quickly — times out if it blocks the event loop
        await asyncio.wait_for(wal_manager.append(entry), timeout=2.0)

    async def test_append_then_replay(self, wal_manager: WALManager) -> None:
        await wal_manager.append(make_entry(seq=1, key=b"k1", value=b"v1"))
        await wal_manager.append(make_entry(seq=2, key=b"k2", value=b"v2"))

        entries = wal_manager.replay()
        assert len(entries) == 2
        assert entries[0].seq == 1
        assert entries[1].seq == 2

    async def test_concurrent_appends(self, wal_manager: WALManager) -> None:
        tasks = [
            wal_manager.append(make_entry(seq=i, key=f"k{i}".encode()))
            for i in range(20)
        ]
        await asyncio.gather(*tasks)

        entries = wal_manager.replay()
        assert len(entries) == 20
        # All seqs present (order may vary in WAL, but replay sorts)
        assert {e.seq for e in entries} == set(range(20))


class TestWALManagerTruncate:
    """Tests for async truncate_before."""

    async def test_truncate(self, wal_manager: WALManager) -> None:
        for i in range(1, 6):
            await wal_manager.append(make_entry(seq=i))

        await wal_manager.truncate_before(3)

        entries = wal_manager.replay()
        assert [e.seq for e in entries] == [4, 5]


class TestWALManagerClose:
    """Tests for async close."""

    async def test_close(self, wal_manager: WALManager) -> None:
        await wal_manager.append(make_entry(seq=1))
        await wal_manager.close()
        # No assertion needed — should complete without error
