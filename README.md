# lsm-kv

A production-grade **Log-Structured Merge Tree** key-value store written in Python 3.12+.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    LSMEngine                         │
│  (async open/close, put/get/delete, recovery)       │
├──────────┬──────────┬───────────┬───────────────────┤
│ WAL      │ MemTable │ SSTable   │ Flush             │
│ Manager  │ Manager  │ Manager   │ Pipeline          │
├──────────┼──────────┼───────────┼───────────────────┤
│ WALWriter│ Active   │ Writer    │ FlushSlot chain   │
│ (msgpack)│ MemTable │ Reader    │ (parallel write,  │
│          │ Immutable│ Registry  │  serial commit)   │
│          │ MemTable │ BlockCache│                   │
│          │ SkipList │ BloomFilter                   │
│          │          │ SparseIndex                   │
├──────────┴──────────┴───────────┴───────────────────┤
│  Common: CRC32, Encoding, Errors, Types, Config     │
│  Observability: structlog + TCP broadcast            │
└─────────────────────────────────────────────────────┘
```

## Quick Start

```bash
uv sync              # install dependencies
uv run poe start     # start the REPL
uv run poe check     # mypy + basedpyright + ruff + bandit
uv run poe test      # run all tests
```

## REPL Commands

| Command | Description |
|---------|-------------|
| `put <key> <value>` | Write a key-value pair |
| `get <key>` | Read a value |
| `del <key>` | Tombstone a key |
| `flush` | Force-flush memtable to SSTable |
| `mem` | List all memtables |
| `mem <id>` | Show entries of a specific memtable |
| `disk` | List all SSTables by level |
| `disk <id>` | Show entries of a specific SSTable |
| `stats` | Show engine statistics |
| `config` | Show current config |
| `config set <k> <v>` | Update a config parameter |

## Key Features

- Atomic writes: WAL-first durability, memtable visibility under lock
- Concurrent SkipList with per-node fine-grained locking
- Parallel flush pipeline with event-chain ordered commits
- mmh3-backed Bloom filters with configurable FPR
- Bisect-based sparse block index for SSTable lookups
- mmap-based SSTable reads for zero-copy access
- Recovery via SSTable scan + WAL replay on startup
- UUIDv7 time-ordered SSTable file IDs
- Persistent manifest for L0 ordering across restarts
- Structured logging with file + TCP broadcast
- Runtime-mutable, disk-persisted configuration

## Design Docs

See `docs/` for architecture documents covering SSTable format, flush pipeline concurrency, async I/O strategy, and MemTable design.
