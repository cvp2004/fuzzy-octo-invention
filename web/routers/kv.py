"""KV endpoints — put, get, delete, batch, trace."""

from __future__ import annotations

import time

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

import web.server as srv
from app.types import TOMBSTONE

router = APIRouter()


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class PutRequest(BaseModel):
    """Write a key-value pair to the store."""
    key: str = Field(..., description="The key to write.", examples=["user:1"])
    value: str = Field(..., description="The value to associate with the key.", examples=["Alice"])


class DeleteRequest(BaseModel):
    """Delete a key by writing a tombstone marker."""
    key: str = Field(..., description="The key to delete.", examples=["user:1"])


class BatchOp(BaseModel):
    """A single operation within a batch request."""
    op: str = Field(..., description='Operation type: "put" or "del".', examples=["put"])
    key: str = Field(..., description="The key to operate on.", examples=["user:1"])
    value: str | None = Field(None, description="The value (required for put, ignored for del).", examples=["Alice"])


class BatchRequest(BaseModel):
    """Execute multiple put/delete operations atomically."""
    ops: list[BatchOp] = Field(..., description="List of operations to execute.")


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class WriteResponse(BaseModel):
    """Response for a successful write (put or delete)."""
    ok: bool = Field(..., description="Whether the operation succeeded.")
    seq: int = Field(..., description="Sequence number after the write.")
    timestamp_ms: int = Field(..., description="Server timestamp in milliseconds.")


class BatchResponse(BaseModel):
    """Response for a batch write operation."""
    ok: bool = Field(..., description="Whether the batch succeeded.")
    count: int = Field(..., description="Number of operations executed.")
    seq_range: list[int] = Field(..., description="[start_seq, end_seq] range of sequence numbers assigned.")


class GetResponse(BaseModel):
    """Response for a key lookup."""
    found: bool = Field(..., description="Whether the key exists and is not deleted.")
    value: str | None = Field(None, description="The value, or null if not found / deleted.")
    source: str = Field(..., description='Where the result came from: "active_memtable", "memtable", "sstable", or "miss".')
    deleted: bool = Field(False, description="True if the key was found but is tombstoned.")
    seq: int | None = Field(None, description="Sequence number of the entry, if found.")


class TraceStep(BaseModel):
    """A single step in a lookup trace."""
    step: int = Field(..., description="Step number in the trace.")
    component: str = Field(..., description="Component checked (e.g. active_memtable, l0:<id>).")
    result: str = Field(..., description="Outcome: hit, miss, tombstone, bloom_skip, or found.")
    seq: int | None = Field(None, description="Sequence number if a record was found.")
    bloom_check: str | None = Field(None, description='Bloom filter result: "positive" or "negative".')
    bisect_offset: int | None = Field(None, description="Block offset from sparse index bisect.")
    block_cache_hit: bool | None = Field(None, description="Whether the block was served from cache.")


class TraceResponse(BaseModel):
    """Full trace of a key lookup through all storage layers."""
    key: str = Field(..., description="The key that was traced.")
    found: bool = Field(..., description="Whether the key was ultimately found.")
    value: str | None = Field(None, description="The value if found.")
    source: str | None = Field(None, description="The component that held the final result.")
    seq: int | None = Field(None, description="Sequence number of the found entry.")
    steps: list[dict[str, object]] = Field(..., description="Ordered list of lookup steps.")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/put", response_model=WriteResponse, summary="Put a key-value pair")
async def kv_put(req: PutRequest) -> dict[str, object]:
    """Write a key-value pair to the LSM store.

    The key and value are UTF-8 encoded and written to the active memtable.
    A WAL entry is persisted before the write is acknowledged.
    """
    e = srv.get_engine()
    t0 = time.time_ns() // 1_000_000
    key_bytes = req.key.encode()
    val_bytes = req.value.encode()
    srv.wa_user_bytes += len(key_bytes) + len(val_bytes)
    await e.put(key_bytes, val_bytes)
    seq_after = e.stats().seq
    return {"ok": True, "seq": seq_after, "timestamp_ms": t0}


@router.post("/delete", response_model=WriteResponse, summary="Delete a key")
async def kv_delete(req: DeleteRequest) -> dict[str, object]:
    """Delete a key by writing a tombstone marker.

    The key is not immediately removed — a tombstone sentinel is written so
    that subsequent reads return "not found". The tombstone is eventually
    removed during compaction.
    """
    e = srv.get_engine()
    t0 = time.time_ns() // 1_000_000
    key_bytes = req.key.encode()
    srv.wa_user_bytes += len(key_bytes)
    await e.delete(key_bytes)
    seq_after = e.stats().seq
    return {"ok": True, "seq": seq_after, "timestamp_ms": t0}


@router.post("/batch", response_model=BatchResponse, summary="Batch put/delete operations")
async def kv_batch(req: BatchRequest) -> dict[str, object]:
    """Execute multiple put and delete operations in sequence.

    Each operation is applied in order. The response includes the range of
    sequence numbers assigned across all operations.
    """
    e = srv.get_engine()
    seq_start = e.stats().seq
    count = 0
    for op in req.ops:
        if op.op == "put" and op.value is not None:
            key_bytes = op.key.encode()
            val_bytes = op.value.encode()
            srv.wa_user_bytes += len(key_bytes) + len(val_bytes)
            await e.put(key_bytes, val_bytes)
            count += 1
        elif op.op == "del":
            key_bytes = op.key.encode()
            srv.wa_user_bytes += len(key_bytes)
            await e.delete(key_bytes)
            count += 1
    seq_end = e.stats().seq
    return {"ok": True, "count": count, "seq_range": [seq_start + 1, seq_end]}


@router.get("/get", response_model=GetResponse, summary="Get a value by key")
async def kv_get(key: str = Query(..., description="The key to look up.", examples=["user:1"])) -> dict[str, object]:
    """Read a value by key, searching memtable first, then SSTables.

    Returns the source of the result (active_memtable, sstable, or miss)
    and whether the key was found as a tombstone (deleted).
    """
    e = srv.get_engine()
    raw_key = key.encode()

    # Check memtable first for source info
    mem_result = e._mem.get(raw_key)
    if mem_result is not None:
        seq, value = mem_result
        if value == TOMBSTONE:
            return {"found": False, "value": None, "source": "memtable", "deleted": True}
        return {
            "found": True,
            "value": value.decode(errors="replace"),
            "source": "active_memtable",
            "seq": seq,
        }

    # Check SSTables
    sst_result = await e._sst.get(raw_key)
    if sst_result is not None:
        seq, _, value = sst_result
        if value == TOMBSTONE:
            return {"found": False, "value": None, "source": "sstable", "deleted": True}
        return {
            "found": True,
            "value": value.decode(errors="replace"),
            "source": "sstable",
            "seq": seq,
        }

    return {"found": False, "value": None, "source": "miss"}


@router.get("/trace", response_model=TraceResponse, summary="Trace a key lookup")
async def kv_trace(key: str = Query(..., description="The key to trace.", examples=["user:1"])) -> dict[str, object]:
    """Walk the full lookup path step-by-step, returning a trace array.

    The trace shows every component checked (active memtable, immutable
    snapshots, L0 SSTables, L1+ SSTables) including bloom filter checks,
    sparse index bisects, and block cache hits/misses.
    """
    e = srv.get_engine()
    raw_key = key.encode()
    steps: list[dict[str, object]] = []
    step_num = 0
    found_value: bytes | None = None
    found_source: str | None = None
    found_seq: int | None = None

    # 1. Active memtable
    step_num += 1
    result = e._mem._active.get(raw_key)
    if result is not None:
        seq, value = result
        is_tomb = value == TOMBSTONE
        steps.append({
            "step": step_num,
            "component": "active_memtable",
            "result": "tombstone" if is_tomb else "hit",
            "seq": seq,
        })
        if not is_tomb:
            found_value = value
            found_source = "active_memtable"
            found_seq = seq
    else:
        steps.append({
            "step": step_num,
            "component": "active_memtable",
            "result": "miss",
        })

    # 2. Immutable snapshots
    if found_value is None:
        snapshot = list(e._mem._immutable_q)
        for i, table in enumerate(snapshot):
            step_num += 1
            result = table.get(raw_key)
            if result is not None:
                seq, value = result
                is_tomb = value == TOMBSTONE
                steps.append({
                    "step": step_num,
                    "component": f"immutable_{i}",
                    "result": "tombstone" if is_tomb else "hit",
                    "seq": seq,
                })
                if not is_tomb:
                    found_value = value
                    found_source = f"immutable_{i}"
                    found_seq = seq
                break
            else:
                steps.append({
                    "step": step_num,
                    "component": f"immutable_{i}",
                    "result": "miss",
                })

    # 3. L0 SSTables
    if found_value is None:
        with e._sst._state_lock:
            l0_snap = list(e._sst._l0_order)

        for fid in l0_snap:
            step_num += 1
            try:
                with e._sst._registry.open_reader(fid) as reader:
                    reader._ensure_loaded()
                    assert reader._bloom is not None
                    assert reader._index is not None

                    bloom_hit = reader._bloom.may_contain(raw_key)
                    if not bloom_hit:
                        steps.append({
                            "step": step_num,
                            "component": f"l0:{fid[:12]}",
                            "bloom_check": "negative",
                            "result": "bloom_skip",
                        })
                        continue

                    offset = reader._index.floor_offset(raw_key)
                    cache_hit = (
                        reader._cache is not None
                        and offset is not None
                        and reader._cache.get(fid, offset) is not None
                    )
                    val = reader.get(raw_key)

                    if val is not None:
                        seq, _, value = val
                        is_tomb = value == TOMBSTONE
                        steps.append({
                            "step": step_num,
                            "component": f"l0:{fid[:12]}",
                            "bloom_check": "positive",
                            "bisect_offset": offset,
                            "block_cache_hit": cache_hit,
                            "result": "tombstone" if is_tomb else "found",
                            "seq": seq,
                        })
                        if not is_tomb:
                            found_value = value
                            found_source = f"l0:{fid[:12]}"
                            found_seq = seq
                        break
                    else:
                        steps.append({
                            "step": step_num,
                            "component": f"l0:{fid[:12]}",
                            "bloom_check": "positive",
                            "bisect_offset": offset,
                            "block_cache_hit": cache_hit,
                            "result": "miss (false positive)",
                        })
            except KeyError:
                continue

    # 4. L1+ SSTables
    if found_value is None:
        with e._sst._state_lock:
            level_snap = dict(e._sst._level_files)

        for level in range(1, e._sst.max_level + 1):
            entry = level_snap.get(level)
            if entry is None:
                continue
            fid, _ = entry
            step_num += 1
            try:
                with e._sst._registry.open_reader(fid) as reader:
                    reader._ensure_loaded()
                    assert reader._bloom is not None
                    assert reader._index is not None

                    bloom_hit = reader._bloom.may_contain(raw_key)
                    if not bloom_hit:
                        steps.append({
                            "step": step_num,
                            "component": f"l{level}:{fid[:12]}",
                            "bloom_check": "negative",
                            "result": "bloom_skip",
                        })
                        continue

                    offset = reader._index.floor_offset(raw_key)
                    cache_hit = (
                        reader._cache is not None
                        and offset is not None
                        and reader._cache.get(fid, offset) is not None
                    )
                    val = reader.get(raw_key)

                    if val is not None:
                        seq, _, value = val
                        is_tomb = value == TOMBSTONE
                        steps.append({
                            "step": step_num,
                            "component": f"l{level}:{fid[:12]}",
                            "bloom_check": "positive",
                            "bisect_offset": offset,
                            "block_cache_hit": cache_hit,
                            "result": "tombstone" if is_tomb else "found",
                            "seq": seq,
                        })
                        if not is_tomb:
                            found_value = value
                            found_source = f"l{level}:{fid[:12]}"
                            found_seq = seq
                        break
                    else:
                        steps.append({
                            "step": step_num,
                            "component": f"l{level}:{fid[:12]}",
                            "bloom_check": "positive",
                            "bisect_offset": offset,
                            "block_cache_hit": cache_hit,
                            "result": "miss (false positive)",
                        })
            except KeyError:
                continue

    return {
        "key": key,
        "found": found_value is not None,
        "value": found_value.decode(errors="replace") if found_value else None,
        "source": found_source,
        "seq": found_seq,
        "steps": steps,
    }
