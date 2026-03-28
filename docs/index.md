# lsm-kv

A production-grade **Log-Structured Merge Tree** key-value store written in Python 3.12+.

## Features

- **Async-first API** — `put`, `get`, `delete`, `flush`, `close` are all async
- **Write-ahead log** — crash recovery via WAL replay
- **Concurrent skip-list memtable** — fine-grained locking for concurrent writes
- **SSTable persistence** — bloom filters, sparse indexes, memory-mapped reads
- **Tiered compaction** — L0 → L1 → L2 → L3 with subprocess-based merging
- **Runtime configuration** — update thresholds without restarting

## Quick Start

```bash
pip install .        # or: uv pip install -e .
lsm-kv repl         # interactive REPL
lsm-kv api          # REST API server on :8081
lsm-kv web          # full web dashboard with React frontend
```

### As a Library

```python
from app.engine import LSMEngine

engine = await LSMEngine.open("./data")

await engine.put(b"hello", b"world")
value = await engine.get(b"hello")    # b"world"
await engine.delete(b"hello")

await engine.close()
```

## Documentation

- **[Getting Started](getting-started.md)** — installation and first steps
- **[API Reference](api/engine.md)** — public classes and methods
- **[Internals](internals/memtable.md)** — how each subsystem works
- **[Design Docs](design/architecture.md)** — architecture and specifications
