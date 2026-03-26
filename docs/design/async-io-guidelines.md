# Async I/O Coding Guidelines

Generic coding rules for implementing async I/O in Python, extracted from patterns used throughout the lsm-kv engine. These guidelines apply to any Python project that mixes async event loops with blocking stdlib I/O.

## Foundational Principle

Async I/O exists to manage wait time on external systems (disk, network), not to speed up computation. If a function does no I/O, it should not be async.

## Decision Flowchart

```
Is the operation I/O-bound?
  ├─ Yes → Does the stdlib call block?
  │          ├─ Yes → Use asyncio.to_thread(blocking_call)
  │          └─ No  → Use native async (aiofiles, etc.)
  └─ No  → Is it CPU-bound and >100ms?
             ├─ Yes → Offload to ProcessPoolExecutor
             └─ No  → Call directly (sync)
```

## Core Rules

1. **Never block the event loop** — `os.fsync()`, `mmap.mmap()`, `open()` must be wrapped in `asyncio.to_thread()` or run in a thread pool
2. **Never make a function async without an `await`** — async is for I/O management, not decoration
3. **Use `asyncio.gather()` for concurrent fan-out** — e.g., checking multiple L0 SSTables in parallel
4. **Use `asyncio.Semaphore` to bound concurrency** — prevent resource exhaustion from unbounded parallelism
5. **Bridge sync→async carefully** — use `loop.call_soon_threadsafe()` to signal async events from sync threads
6. **CPU-bound work goes to subprocesses** — `ProcessPoolExecutor` bypasses the GIL for true parallelism
7. **Cleanup never raises** — wrap each resource release in its own try/except
8. **Prefer `to_thread` over raw threading** — lets asyncio manage the thread lifecycle

## Patterns Used in lsm-kv

### Sync I/O Offload
```python
async def append(self, entry: WALEntry) -> None:
    await asyncio.to_thread(self._sync_append, entry)
```

### Concurrent Fan-Out
```python
tasks = [asyncio.create_task(self._reader_get(fid, key)) for fid in l0_files]
results = await asyncio.gather(*tasks, return_exceptions=True)
```

### Bounded Parallelism
```python
self._semaphore = asyncio.Semaphore(max_workers)
async with self._semaphore:
    meta, reader = await self._write_sstable(slot)
```

### Sync→Async Bridge
```python
# From sync thread (MemTableManager.maybe_freeze):
loop.call_soon_threadsafe(async_event.set)
```

### Subprocess Dispatch
```python
new_meta = await asyncio.to_thread(self._run_in_subprocess, task)
```

## Error Handling

- `asyncio.gather(*tasks, return_exceptions=True)` — collect all results including exceptions, don't abort on first failure
- Always set events/signals in `finally` blocks to prevent chain deadlocks
- Log errors from background tasks — don't let them disappear silently

## Testing

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "function"
```

Use `await asyncio.wait_for(coroutine, timeout=...)` to detect accidental blocking.
