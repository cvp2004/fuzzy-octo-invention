# Compaction Design

Compaction is the process of merging SSTables across levels to reduce read amplification, reclaim disk space, and garbage-collect deletion tombstones. Without compaction, every read would need to scan an ever-growing number of L0 files.

## Why Compaction Matters

| Problem | Without compaction | With compaction |
|---------|-------------------|-----------------|
| Read amplification | Grows with every flush (scan all L0 files) | Bounded by level count (3–4 levels) |
| Disk space | Dead keys and tombstones accumulate forever | Garbage-collected during merge |
| Bloom filter effectiveness | Each L0 file has its own filter | Merged filter covers more keys |

## Level Model

```
L0:  [file_a] [file_b] [file_c] ... (up to 10 files, overlapping key ranges)
      ↓ compaction (merge all L0 + existing L1)
L1:  [single_merged_file]            (non-overlapping, fully deduplicated)
      ↓ compaction (when L1 exceeds threshold)
L2:  [single_merged_file]
      ↓
L3:  [single_merged_file]            (oldest data, largest file)
```

- **L0**: Multiple files with overlapping key ranges. Flushed directly from memtable.
- **L1–L3**: Exactly one file per level. Produced by merging the level above with the existing file at this level.

## Trigger Mechanism

Compaction is **flush-triggered, not daemon-driven**. After every successful flush commit, the flush pipeline calls `CompactionManager.check_and_compact()`. This avoids idle polling and ensures compaction happens promptly when needed.

### Thresholds

| Transition | Trigger | Config |
|------------|---------|--------|
| L0 → L1 | File count >= threshold | `l0_compaction_threshold` (default: 10) |
| L1 → L2 | Dev: entry count, Prod: byte size | Cascading: `10^(N+1) x base` |
| L2 → L3 | Dev: entry count, Prod: byte size | Cascading: `10^(N+1) x base` |

Dev base = `max_memtable_entries`, Prod base = `max_memtable_bytes`.

## Merge Algorithm

The `KWayMergeIterator` merges N sorted input iterators (one per input SSTable) into a single globally sorted stream:

1. **Deduplication**: For equal keys, only the record with the highest sequence number is kept
2. **Tombstone GC**: Tombstones with `seq < seq_cutoff` are dropped (the deletion has propagated to all levels)
3. **Tombstone preservation**: Tombstones above the cutoff are kept (needed to shadow values in deeper levels)

The merge output is written to a new SSTable via `SSTableWriter.finish_sync()` (synchronous, since it runs in a subprocess).

## Task-Based Execution

Each compaction run is represented by a `CompactionTask` — a frozen dataclass with only primitive fields that can safely cross the subprocess boundary. The full task lifecycle (trigger → build → dispatch → merge → commit → re-trigger) and how multiple tasks run as parallel async jobs is documented in detail in [Compaction Manager](compaction-manager.md).

### Subprocess Merge

Compaction merges run in a `ProcessPoolExecutor` subprocess to achieve true parallelism, bypassing CPython's GIL. The subprocess has its own memory space and file handles with no shared state. The dispatch is non-blocking — `asyncio.to_thread(pool.submit(...).result)` keeps the event loop free during the merge.

## Compaction Commit

After the subprocess produces a new SSTable, the commit is atomic:

1. Register new reader (destination level becomes readable)
2. Write manifest (durable)
3. Mark old files for deletion (deferred by ref-count)
4. Update in-memory state
5. Evict stale cache blocks

Write locks on source and destination levels prevent reads from seeing inconsistent state.
