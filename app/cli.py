# app/cli.py — Unified CLI entry point for lsm-kv
from __future__ import annotations

import argparse
import asyncio
import subprocess
import sys
from pathlib import Path


def main() -> None:
    """CLI dispatcher: lsm-kv {repl,api,web}."""
    parser = argparse.ArgumentParser(
        prog="lsm-kv",
        description="Production-grade LSM-tree Key-Value Store",
    )
    sub = parser.add_subparsers(dest="command")

    # ── repl ───────────────────────────────────────────────────────────────
    sub.add_parser("repl", help="Start the interactive REPL")

    # ── api ────────────────────────────────────────────────────────────────
    api_p = sub.add_parser("api", help="Start the REST API server (no frontend)")
    api_p.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    api_p.add_argument("--port", type=int, default=8081, help="Bind port (default: 8081)")
    api_p.add_argument("--reload", action="store_true", help="Enable auto-reload")

    # ── web ────────────────────────────────────────────────────────────────
    web_p = sub.add_parser("web", help="Build frontend and start the web explorer")
    web_p.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    web_p.add_argument("--port", type=int, default=8081, help="Bind port (default: 8081)")
    web_p.add_argument("--reload", action="store_true", help="Enable auto-reload")
    web_p.add_argument(
        "--skip-build", action="store_true", help="Skip frontend build (use existing dist/)"
    )

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(1)

    if args.command == "repl":
        _run_repl()
    elif args.command == "api":
        _run_api(args.host, args.port, args.reload)
    elif args.command == "web":
        _run_web(args.host, args.port, args.reload, args.skip_build)


def _run_repl() -> None:
    """Start the interactive REPL (delegates to main.main)."""
    # main.py lives at the project root, not inside a package.
    # Import it by path so the CLI works when installed as a package.
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "main", Path(__file__).resolve().parent.parent / "main.py"
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    asyncio.run(mod.main())


def _run_api(host: str, port: int, reload: bool) -> None:
    """Start only the FastAPI server."""
    import uvicorn

    uvicorn.run("web.server:app", host=host, port=port, reload=reload)


def _run_web(host: str, port: int, reload: bool, skip_build: bool) -> None:
    """Build the frontend (unless skipped) then start the full web dashboard."""
    frontend_dir = Path(__file__).resolve().parent.parent / "frontend"

    if not skip_build:
        print("Building frontend...")
        result = subprocess.run(
            ["npm", "--prefix", str(frontend_dir), "run", "build"],
            check=False,
        )
        if result.returncode != 0:
            print("Frontend build failed.", file=sys.stderr)
            sys.exit(1)
        print("Frontend build complete.")

    import uvicorn

    uvicorn.run("web.server:app", host=host, port=port, reload=reload)


if __name__ == "__main__":
    main()
