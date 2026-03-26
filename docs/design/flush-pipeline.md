# Flush Pipeline

The `FlushPipeline` is an async daemon task that drains frozen memtable snapshots from the immutable queue to SSTables on disk. It is the bridge between the in-memory write path and persistent storage — without it, the immutable queue would fill up and block writers via backpressure.

## Role in the System

```
MemTableManager                    FlushPipeline                    SSTableManager
    │                                  │                                │
    ├─ maybe_freeze()                  │                                │
    │   push snapshot to queue         │                                │
    │   signal flush_notify ──────────►│                                │
    │                                  ├─ _dispatch_all()               │
    │                                  │   for each snapshot:           │
    │                                  │     Phase 1: write SSTable ───►│ flush()
    │                                  │     Phase 2: commit ──────────►│ commit()
    │                                  │     pop_oldest() ◄─────────────│
    │                                  │     truncate WAL               │
    │                                  │     trigger compaction check   │
```

## Two-Phase Design

### Phase 1: Write (Parallel)

Multiple snapshots can be written to disk simultaneously, bounded by an `asyncio.Semaphore` (default: 2 workers). Each write produces an independent SSTable in its own directory — no coordination needed.

The `SSTableWriter` iterates the immutable snapshot's sorted entries, builds a bloom filter and sparse index, and writes all four files (`data.bin`, `index.bin`, `filter.bin`, `meta.json`).

### Phase 2: Commit (Serialized)

Commits must happen in oldest-first order to maintain the immutable queue's FIFO invariant. Each slot waits for the previous slot's commit to complete before proceeding.

The commit sequence:
1. Register the new SSTable reader in `SSTableManager`
2. Pop the oldest snapshot from the immutable queue
3. Truncate WAL entries up to this snapshot's `seq_max`
4. Trigger compaction check (non-blocking async task)

## Event Chain

Ordering is enforced via an `asyncio.Event` chain threaded through `FlushSlot` objects at dispatch time:

- `slot.prev_committed` — set when the previous slot finishes committing
- `slot.my_committed` — set when this slot finishes committing (or fails)

The first slot in a batch has `prev_committed` pre-set (no predecessor). Each subsequent slot waits on its predecessor's event.

## Wakeup Mechanism

The pipeline sleeps until work is available. When `MemTableManager.maybe_freeze()` pushes a new snapshot, it calls the `_flush_notify` callback. This callback uses `loop.call_soon_threadsafe()` to bridge from the sync write thread to the async event loop, waking the pipeline immediately without polling.

## Failure Handling

If a write fails (Phase 1):
- `batch_abort` event is set — all downstream slots in the batch skip their commits
- `my_committed` is set in the `finally` block — prevents event chain deadlock
- The snapshot stays in the queue and will be retried on the next dispatch cycle
- WAL is not truncated — data remains recoverable

If a commit fails (Phase 2):
- Same chain-unblocking behavior
- This indicates a programming error (rare) and should be investigated

## Configuration

| Config key | Default | Effect |
|------------|---------|--------|
| `flush_max_workers` | 2 | Maximum concurrent SSTable writes |
