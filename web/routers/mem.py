"""MemTable inspection endpoints."""

from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel, Field

import web.server as srv

router = APIRouter()


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class FlushResponse(BaseModel):
    """Response for a memtable flush operation."""
    ok: bool = Field(..., description="Whether the operation succeeded.")
    flushed: bool = Field(..., description="True if data was actually flushed (false if memtable was empty).")
    snapshot_id: str | None = Field(None, description="ID of the flushed snapshot, if a flush occurred.")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("", summary="List all memtables")
async def mem_list() -> dict[str, object]:
    """List the active memtable and all immutable snapshots awaiting flush.

    Returns metadata (table ID, entry count, size) for each memtable.
    """
    e = srv.get_engine()
    return e.show_mem()


@router.post("/flush", response_model=FlushResponse, summary="Force flush memtable")
async def mem_flush() -> dict[str, object]:
    """Force-flush the active memtable to an SSTable on disk.

    The active memtable is frozen, moved to the immutable queue, and
    a new empty memtable is created. The flush pipeline will write
    the frozen snapshot as a Level-0 SSTable.
    """
    e = srv.get_engine()
    flushed = await e.flush()
    result: dict[str, object] = {"ok": True, "flushed": flushed}
    if flushed:
        meta = e._mem.active_metadata
        result["snapshot_id"] = meta.table_id
    return result


@router.get("/{table_id}", summary="Get memtable detail")
async def mem_detail(table_id: str) -> dict[str, object]:
    """Return all entries in a specific memtable (active or immutable) by its ID."""
    e = srv.get_engine()
    return e.show_mem(table_id)
