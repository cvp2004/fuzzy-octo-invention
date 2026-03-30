"""Stats and WAL endpoints."""

from __future__ import annotations

import json

from fastapi import APIRouter
from pydantic import BaseModel, Field

import web.server as srv

router = APIRouter()


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class StatsSnapshot(BaseModel):
    """Live engine statistics snapshot."""
    key_count: int = Field(..., description="Number of keys in the active memtable.")
    seq: int = Field(..., description="Current global sequence number.")
    wal_entry_count: int = Field(..., description="Number of entries in the Write-Ahead Log.")
    active_table_id: str = Field(..., description="UUID of the active memtable.")
    active_size_bytes: int = Field(..., description="Byte size of the active memtable.")
    immutable_queue_len: int = Field(..., description="Number of frozen memtable snapshots awaiting flush.")
    l0_sstable_count: int = Field(..., description="Number of Level-0 SSTables on disk.")
    compaction_active: bool = Field(..., description="Whether a compaction job is currently running.")


class StatsSample(BaseModel):
    """A single time-series sample of engine stats."""
    ts: float = Field(..., description="Unix timestamp of the sample.")
    seq: int = Field(..., description="Sequence number at sample time.")
    l0_count: int = Field(..., description="L0 SSTable count at sample time.")
    mem_bytes: int = Field(..., description="Active memtable size in bytes.")
    key_count: int = Field(..., description="Key count at sample time.")


class StatsHistoryResponse(BaseModel):
    """Ring buffer of stats samples (max 300, sampled every 1s)."""
    samples: list[dict[str, object]] = Field(..., description="Time-ordered stats samples.")


class WriteAmpResponse(BaseModel):
    """Write amplification analysis."""
    user_bytes_written: int = Field(..., description="Cumulative bytes written by user operations (key + value).")
    disk_bytes_current_sstables: int = Field(..., description="Total bytes of all current SSTables on disk.")
    disk_bytes_compaction_historical: int = Field(..., description="Bytes written by compactions whose output was later replaced.")
    disk_bytes_wal: int = Field(..., description="Current WAL file size in bytes.")
    disk_bytes_total: int = Field(..., description="Sum of SSTable, historical compaction, and WAL bytes.")
    write_amplification: float = Field(..., description="Ratio of total disk bytes to user bytes (disk / user).")


class WalInfoResponse(BaseModel):
    """Write-Ahead Log information."""
    entry_count: int = Field(..., description="Number of WAL entries.")
    size_bytes: int = Field(..., description="WAL file size in bytes.")
    last_seq: int = Field(..., description="Sequence number of the last WAL entry.")
    path: str = Field(..., description="Filesystem path to the WAL file.")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.get("/stats", response_model=StatsSnapshot, summary="Engine stats snapshot")
async def stats_snapshot() -> dict[str, object]:
    """Return a point-in-time snapshot of engine statistics."""
    e = srv.get_engine()
    s = e.stats()
    cm = e._compaction
    return {
        "key_count": s.key_count,
        "seq": s.seq,
        "wal_entry_count": s.wal_entry_count,
        "active_table_id": s.active_table_id,
        "active_size_bytes": s.active_size_bytes,
        "immutable_queue_len": s.immutable_queue_len,
        "l0_sstable_count": s.l0_sstable_count,
        "compaction_active": len(cm.active_jobs) > 0 if cm else False,
    }


@router.get("/stats/history", response_model=StatsHistoryResponse, summary="Stats time-series history")
async def stats_history() -> dict[str, object]:
    """Return the ring buffer of stats samples (up to 300, sampled every 1 second)."""
    return {"samples": list(srv.stats_history)}


@router.get("/stats/write-amp", response_model=WriteAmpResponse, summary="Write amplification analysis")
async def write_amplification() -> dict[str, object]:
    """Compute write amplification = total disk bytes written / user bytes written.

    Disk bytes include current SSTables, historical compaction output that
    has since been replaced, and the WAL file.
    """
    e = srv.get_engine()
    user_bytes = srv.wa_user_bytes

    # Current SSTable sizes on disk
    disk_listing = e.show_disk()
    current_disk_bytes = 0
    for level_name in ("L0", "L1", "L2", "L3"):
        for table in (disk_listing.get(level_name) or []):
            current_disk_bytes += table.get("size_bytes", 0)  # type: ignore[union-attr]

    # Historical compaction output bytes from compaction.log
    compaction_historical_bytes = 0
    current_file_ids: set[str] = set()
    for level_name in ("L0", "L1", "L2", "L3"):
        for table in (disk_listing.get(level_name) or []):
            current_file_ids.add(table.get("file_id", ""))  # type: ignore[union-attr]

    log_path = e.data_root / "compaction.log"
    if log_path.exists():
        try:
            for line in log_path.read_text(encoding="utf-8").strip().splitlines():
                if not line.strip():
                    continue
                entry = json.loads(line)
                if entry.get("event") == "committed":
                    out_bytes = entry.get("output_bytes", 0)
                    out_fid = entry.get("output", "")
                    if out_fid not in current_file_ids:
                        compaction_historical_bytes += out_bytes
        except (json.JSONDecodeError, OSError):
            pass

    disk_bytes = current_disk_bytes + compaction_historical_bytes

    # WAL bytes
    wal_path = e._wal._wal.path
    wal_bytes = wal_path.stat().st_size if wal_path.exists() else 0

    total_disk_bytes = disk_bytes + wal_bytes

    if user_bytes == 0:
        ratio = 0.0
    else:
        ratio = round(total_disk_bytes / user_bytes, 2)

    return {
        "user_bytes_written": user_bytes,
        "disk_bytes_current_sstables": current_disk_bytes,
        "disk_bytes_compaction_historical": compaction_historical_bytes,
        "disk_bytes_wal": wal_bytes,
        "disk_bytes_total": total_disk_bytes,
        "write_amplification": ratio,
    }


@router.get("/wal", response_model=WalInfoResponse, summary="WAL info")
async def wal_info() -> dict[str, object]:
    """Return Write-Ahead Log metadata: entry count, size, last sequence number, and path."""
    e = srv.get_engine()
    wal_path = e._wal._wal.path
    entries = e._wal.replay()
    size = wal_path.stat().st_size if wal_path.exists() else 0
    last_seq = entries[-1].seq if entries else 0
    return {
        "entry_count": len(entries),
        "size_bytes": size,
        "last_seq": last_seq,
        "path": str(wal_path),
    }
