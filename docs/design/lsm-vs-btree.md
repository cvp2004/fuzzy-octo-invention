# LSM Trees vs B+ Trees

This document explains what a Log-Structured Merge Tree (LSM Tree) is, how it compares to the traditional B+ Tree used in most relational databases, and why lsm-kv chose an LSM architecture.

---

## What is a B+ Tree?

A B+ Tree is a balanced, disk-oriented tree structure where:

- All data lives in **leaf nodes** (sorted by key)
- Internal nodes store only keys and child pointers for navigation
- Every insert, update, or delete **modifies the tree in place** — finding the correct leaf, updating it, and potentially splitting or merging nodes to maintain balance

B+ Trees are the foundation of most relational databases (PostgreSQL, MySQL/InnoDB, SQLite) and are optimized for **read-heavy workloads** where data is queried far more often than it is written.

### How a B+ Tree write works

```
1. Traverse tree from root to target leaf         → O(log_B N) random reads
2. Modify the leaf page in place                   → 1 random write
3. If leaf overflows → split + update parent       → 1–2 more random writes
4. Fsync the modified pages                        → random I/O
```

Each write requires **random I/O** — seeking to the correct page on disk, modifying it, and fsyncing. On HDDs this is ~10ms per seek. On SSDs it's faster but still involves write amplification from page rewrites.

### How a B+ Tree read works

```
1. Traverse tree from root to target leaf         → O(log_B N) page reads
2. Binary search within the leaf page             → O(log B) comparisons
```

Reads are efficient because the tree is always sorted and balanced. With a branching factor of ~100, even a billion keys need only 4–5 page reads.

---

## What is an LSM Tree?

A Log-Structured Merge Tree takes the opposite approach:

- Writes go to an **in-memory buffer** (memtable) first — no disk I/O on the write path
- When the buffer fills up, it's **flushed as a sorted file** (SSTable) to disk — purely sequential I/O
- Background **compaction** merges sorted files to keep read performance bounded
- Reads check the memtable first, then search through sorted files on disk

LSM Trees are the foundation of most write-heavy systems (Cassandra, RocksDB, LevelDB, HBase, ScyllaDB) and are optimized for **write-heavy workloads** where throughput matters more than single-read latency.

### How an LSM Tree write works

```
1. Append to WAL (sequential write + fsync)        → O(1) sequential I/O
2. Insert into in-memory memtable                   → O(log N) in-memory
3. Done — no disk page modification                 → no random I/O
```

Every write is **sequential I/O** — appending to the WAL and inserting into memory. No seeking, no page splitting, no in-place modification. This is fundamentally cheaper than B+ Tree writes.

### How an LSM Tree read works

```
1. Check memtable (in-memory)                      → O(log N)
2. Check each SSTable on disk:
   a. Bloom filter test                            → O(k) hash checks
   b. Sparse index bisect                          → O(log B) comparisons
   c. Block read + scan                            → 1 sequential read
3. Return result with highest sequence number
```

Reads are more expensive than B+ Trees because multiple sources must be checked. The cost depends on the number of levels and SSTables. Bloom filters mitigate this by eliminating most negative lookups.

---

## Side-by-Side Comparison

| Dimension | B+ Tree | LSM Tree | Winner |
|-----------|---------|----------|--------|
| **Write latency** | O(log_B N) random I/O | O(1) sequential I/O | LSM |
| **Write throughput** | Bounded by random IOPS | Bounded by sequential bandwidth | LSM |
| **Write amplification** | 1× (in-place) + page splits | L/B to T·L/B (compaction rewrites) | B+ Tree |
| **Read latency (point)** | O(log_B N) — predictable | O(L · φ) to O(T · L · φ) — depends on levels + bloom FPR | B+ Tree |
| **Read amplification** | 1× — single tree traversal | L to T·L — check each level | B+ Tree |
| **Range scan** | O(k) — leaf pages are linked | O(k · L) — merge across levels | B+ Tree |
| **Space amplification** | ~1× (in-place updates) | 1/T to T× (depends on compaction strategy) | B+ Tree |
| **Sequential disk access** | Mostly random | Mostly sequential | LSM |
| **SSD friendliness** | Random writes wear SSD faster | Sequential writes extend SSD life | LSM |
| **Concurrent writes** | Page-level locking, splits block | Memtable-level locking, no disk contention | LSM |

---

## The RUM Conjecture

The Read-Update-Memory (RUM) conjecture states that any data structure can optimize at most two of three dimensions:

- **R**ead cost
- **U**pdate (write) cost
- **M**emory (space) overhead

You cannot minimize all three simultaneously. Every design makes a tradeoff:

| Structure | Optimizes | Sacrifices |
|-----------|-----------|------------|
| B+ Tree | Read + Memory | Write (random I/O per update) |
| LSM Tree (leveling) | Read + Memory | Write (compaction rewrites) |
| LSM Tree (tiering) | Write + Memory | Read (multiple runs per level) |
| **LSM Tree (lsm-kv hybrid)** | **Write + Read (balanced)** | **Memory (bounded L0 overlap)** |

lsm-kv's hybrid approach (tiering at L0, leveling at L1+) trades a small, bounded read penalty at L0 for dramatically cheaper writes — the best tradeoff for write-heavy workloads.

---

## Why LSM for Write-Heavy Workloads

### 1. Sequential I/O is 10–100× cheaper than random I/O

On an HDD, a sequential write of 64KB takes ~0.1ms. A random write of 4KB takes ~10ms. The LSM write path is entirely sequential — WAL append + SSTable flush. The B+ Tree write path requires random page modifications.

On SSDs, the gap is smaller (~3–10×) but still significant at scale. Additionally, random small writes to SSDs cause write amplification at the flash translation layer (FTL), reducing SSD lifespan.

### 2. Write path has no disk contention

B+ Tree writes modify shared pages — concurrent writers must coordinate via page-level locks, and page splits can cascade. LSM writes go to an in-memory memtable. Multiple threads can write concurrently with fine-grained locking (per-node in lsm-kv's skip list). The disk is only involved during background flush, which is non-blocking.

### 3. Writes never block on compaction

In lsm-kv, compaction runs in a background subprocess. The write path (WAL + memtable) is completely independent of compaction. In a B+ Tree, there's no concept of background reorganization — every write directly modifies the tree.

### 4. Write amplification is deferred and amortized

A B+ Tree write touches 1–3 pages immediately (leaf + potential splits). An LSM write touches 0 pages immediately (just WAL + memory). The compaction cost is paid later, in the background, and amortized across many writes.

---

## Why B+ Trees Win for Read-Heavy Workloads

### 1. Single traversal, predictable latency

A B+ Tree read follows one path from root to leaf — `O(log_B N)` pages, always. An LSM read may check memtable + multiple SSTables, with latency varying based on where the key lives.

### 2. No bloom filter false positives

B+ Tree reads are deterministic — the key is either in the leaf or it isn't. LSM reads rely on bloom filters that have a false positive rate (1–5%). Each false positive triggers an unnecessary block read.

### 3. Range scans are a linked-list traversal

B+ Tree leaf nodes are linked. A range scan reads consecutive pages — perfect sequential I/O. An LSM range scan must merge results from multiple levels, each potentially requiring seeks to different SSTables.

---

## Where lsm-kv Sits on the Spectrum

lsm-kv is designed for **write-heavy workloads with point reads** — the sweet spot for LSM Trees. Its specific optimizations push further toward write performance:

| Feature | Effect |
|---------|--------|
| L0 tiering (multiple files) | Flush is O(N) — no merge-on-write |
| Parallel flush pipeline | 2× disk throughput on NVMe |
| Subprocess compaction | GIL bypass — writes never blocked |
| Lock-free skip list reads | Concurrent writes don't block reads |
| Bloom filters (per-SSTable) | Point reads skip irrelevant files |
| Three-tier block cache | Hot metadata stays resident |
| Environment-aware bloom FPR | Dev speed vs prod accuracy |

For workloads that are primarily read-heavy with infrequent writes, a B+ Tree (PostgreSQL, SQLite) would be more appropriate. For workloads with high write throughput, time-series data, log ingestion, or append-heavy patterns, the LSM architecture provides fundamentally better performance.
