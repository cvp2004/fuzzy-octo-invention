"""FastAPI server — wraps the LSMEngine for the web dashboard."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator

from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.engine import LSMEngine
from web.routers import kv, mem, disk, compaction, stats, config_routes, engine, terminal
from web.ws import logs

# ---------------------------------------------------------------------------
# Global engine + stats history
# ---------------------------------------------------------------------------

_engine: LSMEngine | None = None
_engine_opened_at: float = 0.0
stats_history: deque[dict[str, object]] = deque(maxlen=300)

# Write-amplification tracking
wa_user_bytes: int = 0  # cumulative user-written bytes (key+value per put/del)


def get_engine() -> LSMEngine:
    """Return the live engine instance. Raises if not open."""
    if _engine is None:
        raise RuntimeError("Engine is not open")
    return _engine


async def _collect_stats() -> None:
    """Background task: sample engine stats every 1s into ring buffer."""
    while True:
        if _engine is not None:
            try:
                s = _engine.stats()
                stats_history.append({
                    "ts": time.time(),
                    "seq": s.seq,
                    "l0_count": s.l0_sstable_count,
                    "mem_bytes": s.active_size_bytes,
                    "key_count": s.key_count,
                })
            except Exception:
                pass
        await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    global _engine, _engine_opened_at
    _engine = await LSMEngine.open()
    _engine_opened_at = time.time()
    collector = asyncio.create_task(_collect_stats())
    yield
    collector.cancel()
    if _engine is not None:
        await _engine.close()
        _engine = None


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="kiwidb Explorer",
    description=(
        "REST API for an educational Log-Structured Merge Tree (LSM-Tree) "
        "key-value store. Provides endpoints for key-value operations, engine "
        "lifecycle management, memtable and SSTable inspection, compaction "
        "control, configuration, real-time statistics, and an interactive terminal."
    ),
    version="1.0.0",
    docs_url="/swagger",
    redoc_url="/redoc",
    openapi_tags=[
        {
            "name": "kv",
            "description": "Key-value CRUD operations — put, get, delete, batch writes, and lookup tracing.",
        },
        {
            "name": "engine",
            "description": "Engine lifecycle — open, close, reset, and status checks.",
        },
        {
            "name": "mem",
            "description": "MemTable inspection — list active and immutable memtables, view entries, force flush.",
        },
        {
            "name": "disk",
            "description": "SSTable inspection — list SSTables by level, view entries, metadata, bloom filters, and sparse indexes.",
        },
        {
            "name": "compaction",
            "description": "Compaction control — view active jobs, trigger compaction, and browse compaction history.",
        },
        {
            "name": "stats",
            "description": "Engine statistics — live snapshot, time-series history, write amplification analysis, and WAL info.",
        },
        {
            "name": "config",
            "description": "Runtime configuration — view, update, and inspect the config schema.",
        },
        {
            "name": "terminal",
            "description": "Interactive REPL — execute kiwidb commands (put, get, del, flush, etc.) and get text output.",
        },
    ],
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount routers
app.include_router(engine.router, prefix="/api/v1/engine", tags=["engine"])
app.include_router(kv.router, prefix="/api/v1/kv", tags=["kv"])
app.include_router(mem.router, prefix="/api/v1/mem", tags=["mem"])
app.include_router(disk.router, prefix="/api/v1/disk", tags=["disk"])
app.include_router(compaction.router, prefix="/api/v1/compaction", tags=["compaction"])
app.include_router(stats.router, prefix="/api/v1", tags=["stats"])
app.include_router(config_routes.router, prefix="/api/v1/config", tags=["config"])
app.include_router(terminal.router, prefix="/api/v1/terminal", tags=["terminal"])

# WebSocket
app.include_router(logs.router)

# ---------------------------------------------------------------------------
# Serve MkDocs documentation from site/ at /docs
# ---------------------------------------------------------------------------

_docs_dir = Path(__file__).resolve().parent.parent / "site"

if _docs_dir.exists():
    app.mount("/docs", StaticFiles(directory=_docs_dir, html=True), name="docs")

# ---------------------------------------------------------------------------
# Serve React SPA from frontend/dist (must be AFTER API routers)
# ---------------------------------------------------------------------------

_frontend_dir = Path(__file__).resolve().parent.parent / "frontend" / "dist"

if _frontend_dir.exists():
    app.mount("/assets", StaticFiles(directory=_frontend_dir / "assets"), name="assets")

    @app.get("/{full_path:path}")
    async def _serve_spa(full_path: str) -> FileResponse:
        """Serve index.html for all non-API routes (React Router handles client-side routing)."""
        file_path = _frontend_dir / full_path
        if file_path.is_file():
            return FileResponse(file_path)
        return FileResponse(_frontend_dir / "index.html")
