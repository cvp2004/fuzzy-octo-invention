# Read & Write Flow

This document traces the exact path of data through the lsm-kv engine for both write and read operations. Every lock acquisition, every conditional branch, and every data transformation is documented.

---

## Write Flow

### Overview

All writes (put, delete) follow the same atomic path under a single lock. The key invariant is **durability before visibility** — the WAL is fsynced before the memtable is updated.

```mermaid
flowchart TD
    A["engine.put(key, value)"] --> B["_check_closed()"]
    B --> C["Acquire _mem.write_lock"]

    subgraph ATOMIC["Atomic Block (under write_lock)"]
        direction TB
        D["1. seq = SeqGenerator.next()"] --> E["2. WALManager.sync_append(entry)"]
        E --> F["3. MemTableManager.put(key, seq, ts, value)"]
        F --> G["4. MemTableManager.maybe_freeze()"]
    end

    C --> D
    G --> H{Threshold reached?}
    H -- No --> I["Release write_lock<br/>Return"]
    H -- Yes --> J{Queue full?}
    J -- No --> K["Freeze & Enqueue"]
    J -- Yes --> L["Backpressure wait<br/>(up to 60s)"]
    L --> M{Space freed?}
    M -- Yes --> K
    M -- Timeout --> N["Raise FreezeBackpressureTimeout"]
    K --> O["Signal FlushPipeline"]
    O --> I

```

### Step 1: Sequence Generation

`SeqGenerator.next()` atomically increments and returns a monotonically increasing integer under its own `threading.Lock`. This seq number is the MVCC version — higher seq means newer data.

### Step 2: WAL Durability

```mermaid
flowchart TD
    A["WALManager.sync_append()"] --> B["Acquire _wal_lock"]
    B --> C["WALWriter.append(entry)"]
    C --> D["msgpack.packb(seq, ts, op, key, value)"]
    D --> E["CRC32 of payload"]
    E --> F["Frame: 4B len | payload | 4B CRC"]
    F --> G["fd.write(frame)"]
    G --> H["fd.flush()"]
    H --> I["os.fsync(fd)"]
    I --> J["Release _wal_lock"]
```

**Lock ordering constraint**: `_mem.write_lock` is always acquired before `_wal_lock`. Reversing this order would cause deadlock, since the write path holds `write_lock` while calling `sync_append()` which acquires `_wal_lock` internally.

### Step 3: MemTable Insert

The write delegates through `MemTableManager` → `ActiveMemTable` → `SkipList`:

```mermaid
flowchart TD
    A["SkipList.put(key, seq, ts, value)"] --> B["_find(key)<br/><i>lock-free traversal</i>"]
    B --> C{Key exists?}

    C -- Yes --> D["Update Path"]
    C -- No --> E["Insert Path"]

    subgraph UPDATE["Update (per-node lock)"]
        direction TB
        D --> D1["Acquire node.lock"]
        D1 --> D2["Update seq, timestamp, value"]
        D2 --> D3["Update _size_lock"]
        D3 --> D4["Release node.lock"]
    end

    subgraph INSERT["Insert (predecessor locks)"]
        direction TB
        E --> E1["_lock_nodes(predecessors)<br/><i>sorted by id() to prevent deadlock</i>"]
        E1 --> E2{Predecessors still valid?}
        E2 -- No --> E3["Release locks, retry"]
        E2 -- Yes --> E4["Link new node at all levels"]
        E4 --> E5["fully_linked = True<br/><i>visibility barrier</i>"]
        E5 --> E6["Update _size_lock + _count_lock"]
    end

    E3 --> B

```

### Step 4: Conditional Freeze

`maybe_freeze()` checks whether the active memtable has crossed its threshold:

| Mode | Condition | Default |
|------|-----------|---------|
| Dev | `entry_count >= max_memtable_entries` | 10 entries |
| Prod | `size_bytes >= max_memtable_bytes` | 64 MB |

```mermaid
flowchart TD
    A["maybe_freeze()"] --> B{Dev or Prod?}
    B -- Dev --> C{"entry_count >= max_memtable_entries?"}
    B -- Prod --> D{"size_bytes >= max_memtable_bytes?"}
    C -- No --> Z["Return None"]
    D -- No --> Z
    C -- Yes --> E["Freeze triggered"]
    D -- Yes --> E

    E --> F{Queue full?}
    F -- Yes --> G["_queue_not_full.wait(60s)"]
    G --> F
    F -- No --> H["data = SkipList.snapshot()<br/><i>lock-free materialization</i>"]
    H --> I["ImmutableMemTable(snapshot_id, data)<br/><i>sealed after construction</i>"]
    I --> J["queue.appendleft(snapshot)"]
    J --> K["self._active = ActiveMemTable()"]
    K --> L["_flush_event.set()"]
    L --> M["_flush_notify()<br/><i>loop.call_soon_threadsafe</i>"]

```

### Delete vs Put

`delete(key)` follows the identical path but writes `OpType.DELETE` and `value=TOMBSTONE` (sentinel `b"\x00__tomb__\x00"`). The tombstone propagates through flush and compaction. Readers check for tombstones and return `None`.

### Manual Flush

`engine.flush()` calls `force_freeze()`, which bypasses the threshold check and freezes unconditionally (unless the memtable is empty). The same backpressure and signaling logic applies.

### Write Path Lock Hierarchy

```mermaid
graph TD
    A["_mem.write_lock<br/><b>RLock</b><br/><i>held for entire atomic block</i>"]
    A --> B["_seq._lock<br/><b>Lock</b><br/><i>microseconds</i>"]
    A --> C["_wal._wal_lock<br/><b>Lock</b><br/><i>~1ms (fsync)</i>"]
    A --> D["SkipList internal locks"]
    D --> E["node.lock<br/><i>per-node, update path</i>"]
    D --> F["_lock_nodes<br/><i>multi-node, insert path</i>"]
    D --> G["_size_lock + _count_lock<br/><i>microseconds</i>"]

```

### Write Path Error Conditions

| Error | Condition | Recovery |
|-------|-----------|----------|
| `EngineClosed` | Engine has been closed | Caller must reopen |
| `OSError` | WAL fsync failure | Write fails atomically (memtable not updated) |
| `SkipListKeyError` | Empty key | Caller error |
| `SkipListInsertError` | 64 concurrent retries exhausted | Extremely rare contention |
| `SnapshotEmptyError` | Freeze called on empty table | Should never happen (threshold > 0) |
| `FreezeBackpressureTimeout` | Queue full for 60 seconds | Flush pipeline may be stuck |

---

## Read Flow

### Overview

Reads scan from newest to oldest data sources. The first match with the highest sequence number wins. No write locks are acquired — reads are non-blocking.

```mermaid
flowchart TD
    A["engine.get(key)"] --> B["_check_closed()"]
    B --> C["MemTableManager.get(key)"]

    subgraph MEM["In-Memory (lock-free)"]
        direction TB
        C --> D["Active MemTable<br/><i>O(log n) skip list</i>"]
        D --> E{Found?}
        E -- Yes --> F{TOMBSTONE?}
        F -- Yes --> RNULL1["Return None"]
        F -- No --> RVAL1["Return value"]
        E -- No --> G["Immutable Queue<br/><i>newest-first, O(1) per table</i>"]
        G --> H{Found in any snapshot?}
        H -- Yes --> I{TOMBSTONE?}
        I -- Yes --> RNULL2["Return None"]
        I -- No --> RVAL2["Return value"]
    end

    H -- No --> J["SSTableManager.get(key)"]

    subgraph SST["On-Disk (async read locks)"]
        direction TB
        J --> K["L0 Scan<br/><i>all files, read_lock(0)</i>"]
        K --> L["L1 Scan<br/><i>one file, read_lock(1)</i>"]
        L --> M2["L2 Scan<br/><i>one file, read_lock(2)</i>"]
        M2 --> N["L3 Scan<br/><i>one file, read_lock(3)</i>"]
    end

    N --> O{Best result found?}
    O -- Yes --> P{TOMBSTONE?}
    P -- Yes --> RNULL3["Return None"]
    P -- No --> RVAL3["Return value"]
    O -- No --> RNULL4["Return None"]

```

### Step 1: Active MemTable Lookup

`SkipList.get(key)` performs a lock-free top-down traversal starting from the highest occupied level. At level 0, it checks whether the successor node matches the key and is visible (`fully_linked=True`, `marked=False`). These boolean reads are atomic under CPython's GIL.

Returns `(seq, value)` or `None`. Does not return `timestamp_ms` — only `seq` is needed for MVCC ordering.

### Step 2: Immutable Queue Scan

The queue is scanned **newest-first** (deque index 0 is newest). Each `ImmutableMemTable.get(key)` is an O(1) dict lookup — the internal `_index` dict maps keys directly to their position in the sorted data list.

**Concurrent safety**: A `list()` copy of the deque is taken before iteration to prevent concurrent modification if `pop_oldest()` runs during the scan. The copy is O(n) but n <= 4 (queue max).

### Step 3: L0 SSTable Scan

All L0 files must be checked because their key ranges overlap. The scan holds a read lock on level 0 (via `AsyncRWLock`) to prevent compaction from swapping files mid-scan.

For each file, `SSTableReader.get(key)` executes this pipeline:

```mermaid
flowchart TD
    A["SSTableReader.get(key)"] --> B{mmap is None?}
    B -- Yes --> Z1["Return None<br/><i>empty SSTable</i>"]
    B -- No --> C["_ensure_loaded()<br/><i>lazy load bloom + index</i>"]

    subgraph LAZY["Lazy Load (first access only)"]
        direction TB
        C --> C1{Bloom in cache?}
        C1 -- Yes --> C2["Deserialize from cache"]
        C1 -- No --> C3["Read filter.bin → cache → deserialize"]
        C2 --> C4{Index in cache?}
        C3 --> C4
        C4 -- Yes --> C5["Deserialize from cache"]
        C4 -- No --> C6["Read index.bin → cache → deserialize"]
    end

    C5 --> D
    C6 --> D

    D["BloomFilter.may_contain(key)"] --> E{All hash bits set?}
    E -- No --> Z2["Return None<br/><i>definite miss</i>"]
    E -- Yes --> F["SparseIndex.floor_offset(key)<br/><i>bisect_right</i>"]

    F --> G{Block found?}
    G -- No --> Z3["Return None<br/><i>key before first block</i>"]
    G -- Yes --> H{Block in cache?}

    H -- Yes --> I["Scan cached block"]
    H -- No --> J["mmap read → populate cache"]
    J --> I

    I --> K["iter_block(): scan records"]
    K --> L{rec.key == key?}
    L -- Yes --> M["Candidate: keep if highest seq"]
    L -- No --> N{rec.key > key?}
    N -- Yes --> O["Break<br/><i>sorted, past target</i>"]
    N -- No --> K
    M --> K
    O --> P["Return best match or None"]

```

#### Bloom Filter Check

`BloomFilter.may_contain(key)` hashes the key with multiple mmh3 seeds and checks the bit array. If **any** bit is unset, the key is definitely absent — skip this SSTable. False negatives are impossible; false positives are controlled by `bloom_fpr` config (5% dev, 1% prod).

#### Sparse Index Bisect

`SparseIndex.floor_offset(key)` uses `bisect_right` to find the block whose first key is <= the search key. Returns the byte offset into `data.bin` where the candidate block starts, or `None` if the key is before the first block.

#### Block Cache + mmap Scan

The identified block is checked in the block cache first. On a cache miss, the block is read via mmap (zero-copy memoryview) and populated into the cache. Records within the block are scanned sequentially via `iter_block()`. Since records are sorted by key, the scan stops early when `rec.key > key`.

#### MVCC: Highest Seq Wins

Across all L0 files, the result with the highest `seq` is kept. This ensures the most recent write wins even when the same key appears in multiple L0 SSTables.

### Step 4: L1+ SSTable Scan

Each level above L0 has at most one SSTable. The scan proceeds sequentially from L1 upward, with a read lock per level. The same `SSTableReader.get()` flow (bloom → index → block) applies.

### Step 5: Tombstone Resolution

After finding a result from any source, the engine checks if the value is the `TOMBSTONE` sentinel. If so, the key has been deleted — return `None`.

### Read Path Lock Summary

```mermaid
graph TD
    A["_state_lock<br/><b>Lock</b><br/>snapshot L0 + levels"] --> B["level_lock 0<br/><b>AsyncRWLock</b><br/>L0 scan"]
    B --> C["level_lock 1<br/><b>AsyncRWLock</b><br/>L1 scan"]
    C --> D["level_lock 2<br/><b>AsyncRWLock</b><br/>L2 scan"]
    D --> E["level_lock 3<br/><b>AsyncRWLock</b><br/>L3 scan"]
```

No write locks are ever acquired during reads. The memtable scan is entirely lock-free. The `AsyncRWLock` read locks allow multiple concurrent readers while blocking compaction commits.

### Read Path Data Types

| Source | Returns | Type |
|--------|---------|------|
| SkipList.get() | `(seq, value)` | `tuple[int, bytes] \| None` |
| ImmutableMemTable.get() | `(seq, value)` | `tuple[int, bytes] \| None` |
| SSTableReader.get() | `(seq, timestamp_ms, value)` | `tuple[int, int, bytes] \| None` |
| LSMEngine.get() | `value` | `bytes \| None` |

---

## Interaction Between Read and Write

Reads and writes can happen concurrently without blocking each other:

```mermaid
sequenceDiagram
    participant W as Writer Thread
    participant SL as SkipList
    participant R as Reader Thread
    participant SST as SSTableManager

    W->>W: acquire _mem.write_lock
    W->>W: seq = SeqGenerator.next()
    W->>W: WAL.sync_append(entry)

    R->>SL: get(key) — lock-free
    Note over R,SL: No lock needed<br/>GIL-atomic flag checks

    W->>SL: put(key, seq, value)
    SL->>SL: fully_linked = True

    R->>R: scan immutable queue
    Note over R: O(1) dict lookup<br/>per snapshot

    W->>W: maybe_freeze()
    W->>W: SkipList.snapshot()
    W->>W: queue.appendleft(snapshot)
    W->>W: signal FlushPipeline
    W->>W: release write_lock

    R->>SST: get(key) — read_lock
    Note over R,SST: bloom → index → block<br/>read lock allows concurrent readers
```

**Key concurrency properties:**

- **Writers hold `_mem.write_lock`** — serializes writes but does not block readers
- **Readers are lock-free in memtable** — skip list traversal checks GIL-atomic flags
- **Readers hold AsyncRWLock read locks** — multiple readers proceed simultaneously
- **Compaction holds write locks** — blocks new readers only during the brief commit phase (< 5ms)
- **Flush pipeline is async** — runs on the event loop, does not hold write locks
