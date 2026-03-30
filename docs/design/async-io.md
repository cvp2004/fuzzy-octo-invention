# Async I/O Integration

This document explains how async I/O is applied throughout the kiwi-db engine — which operations are async, which stay sync, and why. Every operation is classified before choosing its execution model: I/O-bound operations get async treatment, pure computation stays synchronous.

## Classification Framework

| Class | Signature | Rule | Examples |
|-------|-----------|------|----------|
| I/O-bound | `async def` + `await` | Disk write, fsync, mmap open | WAL append, SSTable finish |
| Sync I/O (blocking stdlib) | `def` + `asyncio.to_thread()` | `mmap`, `open()`, `os.fsync` | WAL sync_append, compaction subprocess |
| CPU-bound | `def`, offload if >100ms | CRC32, bloom filter build | Compaction merge |
| Pure logic | `def`, call directly | Key comparison, bisect, flag checks | MemTable get, bloom may_contain |

## Component Breakdown

### WAL (`app/wal/`, `app/engine/wal_manager.py`)

- `append()` — async, delegates to `to_thread(_sync_append)` so the event loop stays free during fsync
- `replay()` — sync, runs once at startup before any async workers
- `truncate_before()` — async via `to_thread`, rewrites the WAL file

WAL fsync is the dominant write-path latency (0.5–2ms). Making it async lets the engine accept the next `put()` while the OS flushes.

### MemTable (`app/memtable/`)

All operations are **sync**. The skip list is entirely in-memory — point lookups are microseconds, iteration is O(n). Per the async guidelines: never make a function async unless it contains at least one `await` on I/O.

### SSTable Writer (`app/sstable/writer.py`)

- `put()` — sync (buffers in memory, no I/O per record)
- `finish()` — async (L0): bloom filter and index written concurrently via `asyncio.gather`
- `finish_sync()` — sync (L1+): runs in compaction subprocess where blocking is fine

### SSTable Reader (`app/sstable/reader.py`)

- `open()` — async factory: mmaps `data.bin`, lazily defers bloom/index loading
- `get()` — sync: bloom check + bisect + mmap scan are pure memory operations after OS page cache is warm
- Bloom and index loaded lazily on first `get()`, cached in `BlockCache`

### SSTable Manager Read Path

L0 reads fan out concurrently — all L0 files must be checked (overlapping ranges). This is the key async optimization on the read path: instead of checking 4 files sequentially (~4ms), all are checked in parallel (~1ms regardless of count).

L1+ reads are sequential (one file per level, non-overlapping ranges).

### Flush Pipeline

Async daemon task. Writes are parallel (bounded by semaphore), commits are serialized (event chain). See [Flush Pipeline](flush-pipeline.md) for details.

### Compaction

CPU-bound merge runs in a `ProcessPoolExecutor` subprocess, dispatched via `asyncio.to_thread(pool.submit(...).result)`. The event loop stays free during the potentially long merge operation.

## What Stays Sync and Why

| Component | Reason |
|-----------|--------|
| `SkipList.put()` / `get()` | In-memory, microseconds |
| `ImmutableMemTable.get()` | Dict lookup |
| CRC32 computation | <1ms per record |
| `BloomFilter.may_contain()` | Bit array read |
| `SparseIndex.floor_offset()` | List bisect |

These are all pure computation with no I/O. Wrapping them in async would add overhead without benefit.

## Lock Ordering

The critical ordering rule: `_mem.write_lock` → `_wal_lock`. The write lock is acquired first in `engine.put()`, then the WAL lock is acquired inside `sync_append()`. Reversing this order causes deadlock.

No threading lock is ever held when an asyncio primitive is awaited. The async primitives (flush semaphore, commit events) are only awaited in async context where no threading lock is held.
