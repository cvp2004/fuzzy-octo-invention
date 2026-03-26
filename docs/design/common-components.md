# Common Components

The `app/common/` package and related utility modules provide shared infrastructure with zero knowledge of LSM semantics. These are general-purpose primitives used across all layers of the engine.

## CRC32 Helper (`app/common/crc.py`)

Four functions for data integrity verification:

- `compute(data)` — returns CRC32 as unsigned 32-bit integer
- `pack(crc)` — encodes CRC as 4-byte big-endian bytes
- `unpack(data, offset)` — reads CRC from bytes at given offset
- `verify(data, expected_crc)` — computes and compares

Used by WAL entries, SSTable records, bloom filter serialization, and sparse index serialization.

## Binary Encoding (`app/common/encoding.py`)

Record encoding for SSTable data blocks:

- `encode_record(key, seq, timestamp_ms, value)` — produces the binary frame with header + body + CRC
- `decode_from(memoryview, offset)` — returns a `DecodedRecord` namedtuple with `key`, `seq`, `timestamp_ms`, `value`, `next_offset`
- `iter_block(memoryview, start, end)` — yields all records in a block range
- `encode_index_entry(key, offset)` / `decode_index_entries(data)` — sparse index wire format

## Bloom Filter (`app/bloom/filter.py`)

An mmh3-backed probabilistic membership test. Used by `SSTableReader` to skip disk reads for keys that are definitely not present.

- Bit count and hash count are computed optimally from `n` (expected items) and `fpr` (false positive rate)
- `n` is derived from actual data: `len(snapshot)` for flush, sum of input record counts for compaction
- `fpr` is configurable per environment: `bloom_fpr_dev` (0.05) and `bloom_fpr_prod` (0.01)
- Serialized with a 16-byte header + bit array + 4-byte CRC footer
- Implements the `Serializable` ABC and satisfies `BloomFilterProtocol` structurally

## Sparse Index (`app/index/sparse.py`)

Maps block first-keys to byte offsets for binary search within SSTable data files.

- `add(first_key, offset)` — append entry (keys must be ascending)
- `floor_offset(key)` — rightmost entry with `first_key <= key` (for point lookups)
- `ceil_offset(key)` — leftmost entry with `first_key >= key` (for range scans)
- `next_offset_after(offset)` — find the next block boundary
- Serialized with CRC footer, implements `Serializable` ABC

## Block Cache (`app/cache/block.py`)

Three-tier thread-safe LRU cache backed by `cachetools.LRUCache`:

| Tier | Sentinel offset | Retention | Default capacity |
|------|----------------|-----------|-----------------|
| Bloom filters | -1 | Highest | 64 entries |
| Sparse indexes | -2 | Medium | 64 entries |
| Data blocks | >= 0 | Lowest | 256 entries |

Each `get()`/`put()` call is automatically routed to the correct tier based on the offset value. All operations are protected by a single `threading.Lock`.

## SSTable Registry (`app/sstable/registry.py`)

Ref-counted lifecycle management for open `SSTableReader` instances:

- `register(file_id, reader)` — add a reader with refcount 0
- `open_reader(file_id)` — context manager that increments refcount on enter, decrements on exit
- `mark_for_deletion(file_id)` — schedule cleanup; actual close happens when refcount hits 0
- `close_all()` — close idle readers, defer in-use ones

Prevents readers from being closed while a `get()` scan is using them.

## AsyncRWLock (`app/common/rwlock.py`)

Async readers-writer lock with writer preference:

- Multiple readers can hold the lock simultaneously
- A writer gets exclusive access — blocks new readers and waits for existing ones
- Writer preference: once a writer is waiting, new readers block (prevents writer starvation)

Used by `SSTableManager` for per-level locking during compaction commits.

## KWayMergeIterator (`app/common/merge_iterator.py`)

Heap-based merge of N sorted iterators into one globally sorted stream:

- Deduplication: for equal keys, highest sequence number wins
- Tombstone GC: entries with `seq < seq_cutoff` are garbage-collected
- Tombstone filtering: optionally omit tombstones from output
- Uses a min-heap where records sort by key ascending, then seq descending for equal keys

Used by the compaction worker to merge multiple SSTables.
