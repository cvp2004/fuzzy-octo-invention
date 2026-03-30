"""Compaction endpoints."""

from __future__ import annotations

import json

from fastapi import APIRouter
from pydantic import BaseModel, Field

import web.server as srv

router = APIRouter()


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class CompactionJob(BaseModel):
    """An active compaction job."""
    src: str = Field(..., description="Source level or file ID.")
    dst: str = Field(..., description="Destination level or file ID.")
    task_id: str = Field(..., description="Async task identifier.")
    started_at: str | None = Field(None, description="ISO timestamp when the job started.")


class CompactionStatusResponse(BaseModel):
    """Current compaction status."""
    active_jobs: list[CompactionJob] = Field(..., description="Currently running compaction jobs.")
    active_levels: list[int] = Field(..., description="Levels with active compaction.")
    last_completed: str | None = Field(None, description="Timestamp of the last completed compaction.")


class CompactionTriggerResponse(BaseModel):
    """Response from a manual compaction trigger."""
    ok: bool = Field(..., description="Whether the operation succeeded.")
    triggered: bool = Field(..., description="Whether compaction was actually triggered (L0 count >= threshold).")
    reason: str = Field(..., description="Human-readable reason (e.g. L0 count vs threshold).")


class CompactionHistoryResponse(BaseModel):
    """Compaction event log."""
    events: list[dict[str, object]] = Field(..., description="Chronological list of compaction events from compaction.log.")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/status", response_model=CompactionStatusResponse, summary="Compaction status")
async def compaction_status() -> dict[str, object]:
    """Return the current status of compaction jobs and active levels."""
    e = srv.get_engine()
    cm = e._compaction
    if cm is None:
        return {"active_jobs": [], "active_levels": [], "last_completed": None}

    active_jobs = []
    for (src, dst), task in cm.active_jobs.items():
        active_jobs.append({
            "src": src,
            "dst": dst,
            "task_id": task.get_name() if hasattr(task, "get_name") else str(id(task)),
            "started_at": None,
        })

    return {
        "active_jobs": active_jobs,
        "active_levels": sorted(cm._active_levels),
        "last_completed": None,
    }


@router.post("/trigger", response_model=CompactionTriggerResponse, summary="Trigger compaction")
async def compaction_trigger() -> dict[str, object]:
    """Manually trigger a compaction check.

    Compaction runs if the L0 SSTable count meets or exceeds the configured threshold.
    """
    e = srv.get_engine()
    cm = e._compaction
    if cm is None:
        return {"ok": False, "triggered": False, "reason": "No compaction manager"}

    threshold = int(e.config.l0_compaction_threshold)
    l0_count = e._sst.l0_count
    await cm.check_and_compact()
    return {
        "ok": True,
        "triggered": l0_count >= threshold,
        "reason": f"L0 count: {l0_count}/{threshold}",
    }


@router.get("/history", response_model=CompactionHistoryResponse, summary="Compaction history")
async def compaction_history() -> dict[str, object]:
    """Return the full compaction event log from compaction.log."""
    e = srv.get_engine()
    log_path = e.data_root / "compaction.log"
    events: list[dict[str, object]] = []
    if log_path.exists():
        try:
            for line in log_path.read_text(encoding="utf-8").strip().splitlines():
                if line.strip():
                    events.append(json.loads(line))
        except (json.JSONDecodeError, OSError):
            pass
    return {"events": events}
