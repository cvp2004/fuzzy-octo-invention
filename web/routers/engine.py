"""Engine lifecycle endpoints."""

from __future__ import annotations

import shutil
import time

from fastapi import APIRouter
from pydantic import BaseModel, Field

import web.server as srv

router = APIRouter()


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class EngineStatusResponse(BaseModel):
    """Current engine status."""
    open: bool = Field(..., description="Whether the engine is currently open.")
    data_root: str | None = Field(None, description="Filesystem path to the data directory.")
    log_port: int = Field(..., description="TCP port for the structured log server.")
    uptime_s: float = Field(..., description="Seconds since the engine was opened.")


class EngineOkResponse(BaseModel):
    """Acknowledgement for engine lifecycle operations."""
    ok: bool = Field(..., description="Whether the operation succeeded.")
    data_root: str | None = Field(None, description="Filesystem path to the data directory (when applicable).")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/status", response_model=EngineStatusResponse, summary="Get engine status")
async def engine_status() -> dict[str, object]:
    """Return the current engine status including uptime and data directory path."""
    if srv._engine is None:
        return {"open": False, "data_root": None, "log_port": 0, "uptime_s": 0}
    e = srv.get_engine()
    uptime = round(time.time() - srv._engine_opened_at, 1)
    return {
        "open": True,
        "data_root": str(e.data_root),
        "log_port": e.log_port,
        "uptime_s": uptime,
    }


@router.post("/open", response_model=EngineOkResponse, summary="Open the engine")
async def engine_open() -> dict[str, object]:
    """Open (initialize) the LSM engine. No-op if already open."""
    if srv._engine is not None:
        return {"ok": True, "data_root": str(srv._engine.data_root)}
    from app.engine import LSMEngine
    srv._engine = await LSMEngine.open()
    srv._engine_opened_at = time.time()
    return {"ok": True, "data_root": str(srv._engine.data_root)}


@router.post("/close", response_model=EngineOkResponse, summary="Close the engine")
async def engine_close() -> dict[str, object]:
    """Gracefully close the engine, flushing pending writes."""
    if srv._engine is None:
        return {"ok": True}
    await srv._engine.close()
    srv._engine = None
    return {"ok": True}


@router.post("/reset", response_model=EngineOkResponse, summary="Reset the engine")
async def engine_reset() -> dict[str, object]:
    """Close engine, delete data directory, and reopen with a fresh state."""
    from app.engine import LSMEngine

    # Resolve data_root before closing
    data_root = srv._engine.data_root if srv._engine is not None else None

    # Close if open
    if srv._engine is not None:
        await srv._engine.close()
        srv._engine = None

    # Delete data directory
    if data_root is not None and data_root.exists():
        shutil.rmtree(data_root)

    # Clear stats history
    srv.stats_history.clear()
    srv.wa_user_bytes = 0

    # Reopen with fresh state
    srv._engine = await LSMEngine.open()
    srv._engine_opened_at = time.time()
    return {"ok": True, "data_root": str(srv._engine.data_root)}
