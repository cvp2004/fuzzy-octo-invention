# SSTable Manager

The `SSTableManager` owns all on-disk read-side state: the block cache, SSTable registry, L0 file ordering, per-level files (L1+), and the persistent manifest. It provides the interface for flush commits, point lookups across all levels, and compaction commits.

## Responsibilities

1. **Manifest management** — persistent JSON tracking L0 order and per-level file IDs
2. **Reader lifecycle** — opening, registering, and closing `SSTableReader` instances via `SSTableRegistry`
3. **Flush commits** — writing new L0 SSTables and updating the manifest
4. **Read path** — scanning L0 (all files) then L1+ (one per level) for point lookups
5. **Compaction commits** — atomically swapping old files for new merged output
6. **Cache ownership** — manages the shared `BlockCache` for data blocks, indexes, and bloom filters

## Manifest

The manifest (`manifest.json`) persists the SSTable layout across restarts:

```json
{
  "l0_order": ["019abc...", "019abd...", ...],
  "levels": {
    "1": "019abe...",
    "2": "019abf..."
  }
}
```

- `l0_order` is newest-first — the first entry is the most recently flushed SSTable
- `levels` maps level number to the single file ID at that level
- Written atomically via temp file + `os.replace()`

On startup, the manifest is reconciled with what actually exists on disk. Missing files are logged and skipped. Orphan files (on disk but not in manifest) are adopted.

## Startup (`load()`)

1. Discover all L0 SSTable directories with a valid `meta.json`
2. Load manifest and reconcile L0 ordering (manifest order + orphans)
3. Open all L0 readers and register them
4. Load L1+ files from manifest, open readers
5. Scan for orphan level files not in manifest
6. Persist reconciled manifest
7. Clean up stale level directories

## Flush Commit

When the flush pipeline writes a new L0 SSTable:

```python
sst_manager.commit(file_id, reader, sst_dir)
```

1. Register the reader in `SSTableRegistry`
2. Insert `file_id` at the front of `_l0_order` (newest-first)
3. Update `_max_seq` if this SSTable's `seq_max` is higher
4. Persist manifest to disk

## Read Path

`get(key)` scans all levels, returning the result with the highest sequence number:

### L0 Scan
Under a read lock, iterate all L0 files. Each file may contain the key (overlapping ranges). The result with the highest `seq` wins.

### L1+ Scan
One file per level. Check sequentially from L1 upward. A single bloom + index bisect + block scan per level.

Read locks (`AsyncRWLock`) on each level prevent compaction commits from swapping files mid-scan.

## Compaction Commit

The atomic 5-step commit when compaction produces a new merged SSTable:

1. **Register new reader** — destination level becomes readable
2. **Write manifest** — durable record of the new layout
3. **Mark old files for deletion** — deferred by ref-count in registry
4. **Update in-memory state** — remove consumed L0 files, update level mapping
5. **Evict stale cache blocks** — remove cached data for deleted SSTables

Write locks on both source and destination levels are held during this sequence (acquired in ascending level order to prevent deadlock).

After the locks are released, old SSTable directories are deleted from disk.

## Per-Level Locking

Each level has its own `AsyncRWLock`:

- **Read lock**: held during `get()` scans — multiple readers can scan simultaneously
- **Write lock**: held during compaction commits — exclusive access to swap files

Writer-preference policy prevents reader starvation of compaction.

## Block Cache

The manager owns a `BlockCache` instance shared across all readers. Three-tier LRU with independent eviction:

- **Data blocks** (offset >= 0): lowest retention, evicted first
- **Sparse indexes** (sentinel offset -2): medium retention
- **Bloom filters** (sentinel offset -1): highest retention, evicted last

Configured via `cache_data_entry_limit`, `cache_index_entry_limit`, `cache_bloom_entry_limit`.
