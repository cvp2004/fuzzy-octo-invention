# MemTable Manager

The `MemTableManager` is the single coordination point for all in-memory write-side state. It owns the active memtable, the immutable snapshot queue, the write lock, and the backpressure mechanism. It has no knowledge of SSTables, WAL, or disk — those responsibilities belong to other managers.

## Responsibilities

1. **Active table management** — delegates `put()` and `get()` to the current `ActiveMemTable`
2. **Freeze orchestration** — checks thresholds, freezes the active table, pushes snapshots to the immutable queue
3. **Immutable queue management** — FIFO deque of frozen snapshots awaiting flush
4. **Backpressure** — blocks writers when the queue is full, preventing unbounded memory growth
5. **Recovery** — replays WAL entries into a fresh active table on startup
6. **Flush signaling** — notifies the flush pipeline when new snapshots are available

## Immutable Queue

The queue is a `collections.deque` with newest snapshots inserted at the left (front) and oldest popped from the right (back). Maximum depth is controlled by `immutable_queue_max_len` (default: 4).

```
deque: [snap3(newest)]  [snap2]  [snap1]  [snap0(oldest)]
        ← appendleft                         pop() →
```

Queue operations:
- `peek_oldest()` — returns the rightmost snapshot without removing it
- `peek_at_depth(n)` — returns the snapshot at position n from oldest (used by flush pipeline to assign parallel slots)
- `pop_oldest()` — removes the rightmost snapshot after successful flush commit
- `snapshot_queue()` — returns a copy of the queue (oldest-first) for safe iteration

## Freeze Logic

### Threshold-Based Freeze (`maybe_freeze()`)

Called under `_write_lock` after every `put()`. Checks whether the active memtable has crossed its threshold:

- **Dev mode**: `active.metadata.entry_count >= max_memtable_entries`
- **Prod mode**: `active.size_bytes >= max_memtable_bytes`

If the threshold is reached and the queue is full, backpressure kicks in: the caller blocks on `_queue_not_full.wait()` with a configurable timeout (`backpressure_timeout`, default 60s). If the timeout expires, `FreezeBackpressureTimeout` is raised.

### Force Freeze (`force_freeze()`)

Bypasses threshold checks entirely. Used by the engine's manual `flush()` command. Returns `None` if the active memtable is empty (nothing to freeze).

## Write Lock and Lock Ordering

The `_write_lock` is a `threading.RLock` that protects the atomic write path:

```
engine.put()
    with _mem.write_lock:          ← acquire first
        seq = _seq.next()
        _wal.sync_append(entry)    ← acquires _wal_lock inside
        _mem.put(key, seq, ...)
        _mem.maybe_freeze()
```

**Lock ordering rule (BUG-18)**: `_mem.write_lock` must always be acquired before `_wal_lock`. Never reverse this order — it causes deadlock.

## Flush Notification

When a snapshot is added to the queue, two things happen:

1. `_flush_event.set()` — a `threading.Event` for legacy compatibility
2. `_flush_notify()` — an optional callback registered by `FlushPipeline` that calls `loop.call_soon_threadsafe()` to bridge from the sync write thread to the async event loop

This bridging mechanism (BUG-14 fix) ensures the flush pipeline wakes up immediately when new work is available, without polling.

## Recovery

`restore(entries)` replays a list of `WALEntry` objects into the active memtable. Called once at startup, single-threaded, before any workers start. No locks needed. Raises `MemTableRestoreError` if any entry fails to apply.

## Introspection

`show_mem(table_id)` provides two modes:
- **Listing mode** (no `table_id`): returns metadata summaries of active table and all immutable snapshots
- **Detail mode** (with `table_id`): returns full entry list for a specific table
