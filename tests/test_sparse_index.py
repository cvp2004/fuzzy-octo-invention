"""Tests for app.index.sparse — SparseIndex."""

from __future__ import annotations

from app.index.sparse import SparseIndex


class TestSparseIndex:
    def test_add_and_floor(self) -> None:
        idx = SparseIndex()
        idx.add(b"aaa", 0)
        idx.add(b"mmm", 4096)
        idx.add(b"zzz", 8192)

        assert idx.floor_offset(b"aaa") == 0
        assert idx.floor_offset(b"bbb") == 0
        assert idx.floor_offset(b"mmm") == 4096
        assert idx.floor_offset(b"nnn") == 4096
        assert idx.floor_offset(b"zzz") == 8192

    def test_floor_before_first(self) -> None:
        idx = SparseIndex()
        idx.add(b"mmm", 100)
        assert idx.floor_offset(b"aaa") is None

    def test_ceil_offset(self) -> None:
        idx = SparseIndex()
        idx.add(b"aaa", 0)
        idx.add(b"mmm", 4096)
        idx.add(b"zzz", 8192)

        assert idx.ceil_offset(b"aaa") == 0
        assert idx.ceil_offset(b"bbb") == 4096
        assert idx.ceil_offset(b"zzz") == 8192

    def test_ceil_after_last(self) -> None:
        idx = SparseIndex()
        idx.add(b"aaa", 0)
        assert idx.ceil_offset(b"zzz") is None

    def test_serialization_roundtrip(self) -> None:
        idx = SparseIndex()
        idx.add(b"alpha", 0)
        idx.add(b"beta", 1024)
        idx.add(b"gamma", 2048)

        data = idx.to_bytes()
        idx2 = SparseIndex.from_bytes(data)

        assert idx2.floor_offset(b"alpha") == 0
        assert idx2.floor_offset(b"beta") == 1024
        assert idx2.floor_offset(b"gamma") == 2048
        assert len(idx2) == 3

    def test_empty_index(self) -> None:
        idx = SparseIndex()
        assert idx.floor_offset(b"any") is None
        assert idx.ceil_offset(b"any") is None
        assert len(idx) == 0
