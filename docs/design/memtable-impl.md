# MemTable Layer

The memtable layer is the in-memory write buffer of the LSM engine. All writes land here first before being flushed to SSTables on disk. It consists of two components: the `ActiveMemTable` (mutable, accepts writes) and `ImmutableMemTable` (frozen, read-only, awaiting flush).

## Why a Skip List?

The active memtable is backed by a concurrent skip list rather than a hash map or B-tree:

| Requirement | Skip List | Hash Map | B-Tree |
|-------------|-----------|----------|--------|
| Sorted iteration (for SSTable flush) | O(n) | Requires sort | O(n) |
| Point lookups | O(log n) | O(1) avg | O(log n) |
| Concurrent writes | Fine-grained per-node locks | Global lock | Complex rebalancing |
| Lock-free reads | Yes (GIL + boolean flags) | Yes | No |

The skip list allows **writers to lock only the predecessor nodes** at the insertion boundary, while **readers acquire no locks at all** — they rely on `fully_linked` and `marked` boolean flags being atomic under CPython's GIL.

Key parameters: maximum height of 16 levels, geometric distribution with P=0.5, and a safety bound of 64 retries before raising `SkipListInsertError`.

## ActiveMemTable

The `ActiveMemTable` wraps a `SkipList` and adds:

- **`table_id`**: UUID4 hex assigned at creation, used to track the table through the audit chain
- **`put(key, seq, timestamp_ms, value)`**: Delegates to the skip list. Updates entry count and seq range metadata.
- **`get(key)`**: Lock-free O(log n) lookup. Returns `(seq, value)` or `None`.
- **`freeze()`**: Materializes the skip list into a sorted list. Raises `SnapshotEmptyError` if empty. The returned data becomes the basis for an `ImmutableMemTable`.

Implements the `MemTable` ABC contract.

## ImmutableMemTable

Created when an `ActiveMemTable` is frozen. It is permanently sealed — any attribute assignment after construction raises `ImmutableTableAccessError`.

- **O(1) point lookups**: An internal `dict[Key, int]` maps keys to positions in the sorted data list
- **Sorted iteration**: `items()` yields entries in ascending key order for the flush path
- **Metadata**: Tracks `snapshot_id`, entry count, tombstone count, seq range, and freeze timestamp

Note: `ImmutableMemTable` does not implement the `MemTable` ABC because it is read-only (no `put()`, no `freeze()`).

## Freeze Triggers

The freeze threshold depends on the environment mode:

| Mode | Trigger | Config key | Default |
|------|---------|------------|---------|
| **Dev** | Entry count | `max_memtable_entries` | 10 |
| **Prod** | Byte size | `max_memtable_size_mb` | 64 MB |

Dev mode uses a low entry count for predictable, rapid flushing during testing. Prod mode uses byte-based thresholds that reflect real memory budgets.

## Metadata Dataclasses

Both tables expose frozen metadata snapshots safe to pass across threads:

- **`ActiveMemTableMeta`**: `table_id`, `size_bytes`, `entry_count`, `created_at`, `seq_first`, `seq_last`
- **`ImmutableMemTableMeta`**: `snapshot_id`, `source_table_id`, `size_bytes`, `entry_count`, `tombstone_count`, `frozen_at`, `seq_min`, `seq_max`
