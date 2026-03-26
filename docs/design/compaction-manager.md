# Compaction Manager

The `CompactionManager` orchestrates when and how SSTables are merged across levels. Unlike a traditional daemon that polls on a timer, it is **flush-triggered** — called after every successful flush commit by the flush pipeline. This ensures compaction happens promptly when L0 fills up, without wasting cycles polling when the engine is idle.

## Role in the System

```
FlushPipeline                     CompactionManager                SSTableManager
    │                                  │                                │
    ├─ _commit_slot() done             │                                │
    │   trigger check ────────────────►│                                │
    │                                  ├─ _find_eligible_jobs()         │
    │                                  │   L0 count >= threshold?       │
    │                                  │   L1 size >= cascading limit?  │
    │                                  │                                │
    │                                  ├─ _try_reserve(src, dst)        │
    │                                  │   lock levels to prevent       │
    │                                  │   conflicting jobs             │
    │                                  │                                │
    │                                  ├─ _build_task()                 │
    │                                  │   snapshot SSTableManager ────►│ compaction_snapshot()
    │                                  │   state into CompactionTask    │
    │                                  │                                │
    │                                  ├─ _run_in_subprocess()          │
    │                                  │   ProcessPoolExecutor          │
    │                                  │   run_compaction(task)         │
    │                                  │                                │
    │                                  ├─ commit result ───────────────►│ commit_compaction_async()
    │                                  │                                │
    │                                  └─ re-trigger check              │
    │                                     (cascading compactions)       │
```

## Task Lifecycle — End to End

A compaction task flows through five stages from trigger to completion. Multiple tasks can be in-flight simultaneously as independent `asyncio.Task` objects, as long as their levels don't conflict.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        check_and_compact()                              │
│  (called by FlushPipeline after every commit, or after a job finishes) │
└────────────────────────┬────────────────────────────────────────────────┘
                         │
                         ▼
            ┌────────────────────────┐
            │ 1. DISCOVER            │  _find_eligible_jobs()
            │    Which level pairs   │  → [(0,1), (1,2)] based on thresholds
            │    need compaction?    │
            └────────────┬───────────┘
                         │ for each (src, dst):
                         ▼
            ┌────────────────────────┐
            │ 2. RESERVE             │  _try_reserve(src, dst)
            │    Lock both levels    │  → False if either level already active
            │    to prevent conflict │    (skip this job, try next)
            └────────────┬───────────┘
                         │ if reserved:
                         ▼
            ┌────────────────────────┐
            │ 3. BUILD TASK          │  _build_task(src, dst)
            │    Snapshot SSTable    │  → CompactionTask (frozen dataclass)
            │    state into a task   │    with input files, output path,
            │    object              │    bloom_fpr, seq_cutoff
            └────────────┬───────────┘
                         │
                         ▼
            ┌────────────────────────┐
            │ 4. DISPATCH            │  asyncio.create_task(_run_job(src, dst))
            │    Launch as async     │  → stored in _active_jobs[(src, dst)]
            │    task (non-blocking) │    runs independently on the event loop
            └────────────┬───────────┘
                         │ inside _run_job:
                         ▼
            ┌────────────────────────────────────────────────────┐
            │ 5. EXECUTE (three phases inside _run_one_compaction)│
            │                                                     │
            │  Phase 1: SUBPROCESS MERGE                          │
            │    _run_in_subprocess(task)                          │
            │    → ProcessPoolExecutor runs run_compaction()       │
            │    → returns SSTableMeta                             │
            │                                                     │
            │  Phase 2: OPEN READER                               │
            │    SSTableReader.open(output_dir)                    │
            │    → reader ready for the registry                  │
            │                                                     │
            │  Phase 3: ATOMIC COMMIT                             │
            │    SSTableManager.commit_compaction_async()          │
            │    → register reader, write manifest, mark old      │
            │      files for deletion, evict cache                │
            └────────────────────────┬────────────────────────────┘
                                     │ finally:
                                     ▼
            ┌────────────────────────┐
            │ 6. CLEANUP + RE-TRIGGER│  _release(src, dst)
            │    Release levels      │  _active_jobs.pop((src, dst))
            │    Re-check thresholds │  check_and_compact() ← cascading
            └────────────────────────┘
```

## Parallel Job Management

The manager tracks all in-flight jobs in `_active_jobs: dict[tuple[Level, Level], asyncio.Task]`. Multiple non-conflicting jobs can run concurrently as independent async tasks on the event loop.

### The Conflict Rule

A job `(src, dst)` is **blocked** if either `src` or `dst` is already in `_active_levels`. The check is a simple set-membership test under `_reservation_lock`:

```python
if src in self._active_levels or dst in self._active_levels:
    return False  # skip this job
self._active_levels.add(src)
self._active_levels.add(dst)
return True  # reserved
```

When a job reserves `(src=0, dst=1)`, both levels 0 and 1 are added to the set. Any other job that touches level 0 or level 1 will be blocked until this job releases them.

### Conflict Matrix

With 4 levels (L0, L1, L2, L3), these are all possible compaction pairs and their conflict relationships:

| Running job | Blocked jobs | Can run in parallel |
|-------------|-------------|---------------------|
| L0→L1 (reserves L0, L1) | L0→L1 (same levels), L1→L2 (shares L1) | L2→L3 (no shared levels) |
| L1→L2 (reserves L1, L2) | L0→L1 (shares L1), L1→L2 (same levels), L2→L3 (shares L2) | — (no non-adjacent pair left) |
| L2→L3 (reserves L2, L3) | L1→L2 (shares L2), L2→L3 (same levels) | L0→L1 (no shared levels) |

**Key insight**: Only jobs with **no shared levels** can run in parallel. Adjacent-level compactions always conflict because the destination of one is the source of the other.

In practice with `max_levels = 3`, the only parallel combination is **L0→L1 alongside L2→L3**.

### Same-Level Protection

A second L0→L1 job cannot start while one is already running — both `src=0` and `dst=1` are in `_active_levels`. This prevents two merges from racing to produce competing L1 files.

### Adjacent-Level Protection

An L1→L2 job cannot start while L0→L1 is running — they share level 1. Without this protection:
- L0→L1 reads L1's current file as a merge input
- L1→L2 concurrently replaces L1's file with a new merge output
- L0→L1 commits with a stale L1 reference → data loss

The reservation mechanism prevents this by ensuring no two jobs share a level.

### Timeline Examples

**Non-conflicting (parallel)**:
```
Time ──────────────────────────────────────────────────────────►

L0→L1:  ──[reserve 0,1]──[merge]──[commit]──[release 0,1]──►
L2→L3:  ──[reserve 2,3]──[merge]──[commit]──[release 2,3]──►
         ↑ both start simultaneously — no shared levels
```

**Adjacent conflict (serialized via re-trigger)**:
```
Time ──────────────────────────────────────────────────────────►

L0→L1:  ──[reserve 0,1]──[merge]──[commit]──[release 0,1]──►
                                                    │
L1→L2:  (skip — L1 reserved)                       └─ re-trigger
L1→L2:                             ──[reserve 1,2]──[merge]──[commit]──►
```

**Same-level duplicate (blocked)**:
```
L0→L1:  ──[reserve 0,1]──[merge]──[commit]──[release 0,1]──►
L0→L1:  (skip — L0 and L1 both reserved, never starts)
         └─ will be discovered by re-trigger after first job completes
```

### Re-Trigger Catches Deferred Work

When a job completes, `_run_job()` always calls `check_and_compact()` again. This re-evaluates all thresholds with the updated state:
- If L0→L1 just finished but L0 accumulated new files during the merge, a new L0→L1 is triggered
- If L0→L1 made L1 large enough to trigger L1→L2, that job starts now (L1 is no longer reserved)
- Cascading continues until no thresholds are exceeded

## CompactionTask Dataclass

The `CompactionTask` is a frozen dataclass that captures everything needed to execute a merge in an isolated subprocess. All fields are primitive types (str, int, list, dict, float) — no live objects, file handles, or locks.

```python
@dataclass(frozen=True)
class CompactionTask:
    task_id: str                    # unique ID for logging/tracking
    input_file_ids: list[FileID]    # SSTable files to merge
    input_dirs: dict[FileID, str]   # file_id → directory path
    output_file_id: FileID          # ID for the new merged SSTable
    output_dir: str                 # where to write the output
    output_level: Level             # destination level (1, 2, or 3)
    seq_cutoff: SeqNum              # tombstones below this seq are GC'd
    bloom_fpr: float = 0.01         # false positive rate from config
```

The task is built by `_build_task()` which snapshots `SSTableManager` state at that moment. The subprocess receives this frozen snapshot and operates independently — it opens its own file handles, builds its own bloom filter, and returns only the `SSTableMeta` result.

## Job Discovery

`_find_eligible_jobs()` returns a list of `(src_level, dst_level)` pairs ordered by priority:

### L0 → L1
Triggered when L0 file count reaches `l0_compaction_threshold` (default: 10). This is the most common compaction — it fires after enough flushes accumulate.

### L1 → L2, L2 → L3 (Cascading)
Triggered when a level's size exceeds a cascading threshold:

- **Dev mode** (entry-count based): `threshold = 10^(N+1) * max_memtable_entries`
- **Prod mode** (byte-size based): `threshold = 10^(N+1) * max_memtable_bytes`

For example, with `max_memtable_entries = 10` in dev: L1 triggers at 100 entries, L2 at 1000 entries.

## Level Reservation

Before launching a job, the manager atomically reserves both source and destination levels via `_try_reserve(src, dst)`. This prevents conflicting jobs — e.g., two concurrent L0→L1 compactions, or an L0→L1 running alongside an L1→L2 that would read from L1 while it's being written.

Reservations are held for the duration of the job and released in `_release()` when the job completes (or fails).

## Task Construction

`_build_task()` snapshots the current SSTableManager state and assembles a `CompactionTask`:

- **Input files**: All L0 files (for L0→L1) or the single source-level file (for L1+). The existing destination-level file is also included for a full-replacement merge.
- **Bloom FPR**: Read from `config.bloom_fpr` (environment-aware: 0.05 dev, 0.01 prod)
- **Seq cutoff**: The minimum sequence number at the destination level, used for tombstone garbage collection
- **Output path**: A new directory under `data_root/sstable/L{dst}/{new_file_id}/`

## Subprocess Dispatch

The `CompactionTask` is sent to a `ProcessPoolExecutor` subprocess via `asyncio.to_thread()`. Inside the subprocess, `run_compaction()`:

1. Opens all input SSTables (no shared cache or registry — isolated)
2. Creates a `KWayMergeIterator` to merge all inputs
3. Writes the merged output via `SSTableWriter.finish_sync()`
4. Returns `SSTableMeta` to the parent process

The bloom filter for the output is sized to the actual total input record count — not a config guess.

## Commit

After the subprocess completes, the result is committed atomically via `SSTableManager.commit_compaction_async()`:

1. Register the new reader
2. Write the updated manifest
3. Mark old files for deletion (deferred by ref-count)
4. Update in-memory level state
5. Evict stale cache entries

Write locks on both source and destination levels are held during this sequence.

## Re-Trigger

After a compaction job completes, `check_and_compact()` is called again. This handles cascading compactions — e.g., an L0→L1 merge that makes L1 exceed its threshold, triggering an L1→L2 merge.

## Observability

Every compaction event is appended to `data_root/compaction.log` as a JSON line:

```json
{"ts": "2026-03-26T12:00:00", "event": "started", "task_id": "...", "src": 0, "dst": 1, "inputs": [...], "output": "..."}
{"ts": "2026-03-26T12:00:01", "event": "committed", "task_id": "...", "output_records": 500, "output_bytes": 32768}
```

Active jobs are exposed via the `active_jobs` property for monitoring.
