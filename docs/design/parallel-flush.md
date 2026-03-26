# Parallel Flush Correctness

This document proves the correctness of the parallel flush design — specifically, that data is never unreachable and that commit ordering is preserved despite concurrent writes.

## The Core Invariant

At every point in time, every durable key must be findable by `get()` — either in the immutable queue or in the SSTable registry. Never in neither.

This means:

| Phase | Parallel? | Reason |
|-------|-----------|--------|
| SSTable disk write | Yes | Each writes to a separate directory |
| Registry register + queue pop + WAL truncate | **No — strictly ordered** | Determines read visibility and WAL safety |

**Writes are embarrassingly parallel. Commits must be serialized in oldest-first order.**

## Event Chain Mechanism

Each flush slot carries two `asyncio.Event` objects:

```
slot0.prev_committed = pre_set_event       ← no predecessor, can commit immediately
slot0.my_committed   = Event_A

slot1.prev_committed = Event_A             ← must wait for slot0
slot1.my_committed   = Event_B

slot2.prev_committed = Event_B             ← must wait for slot1
slot2.my_committed   = Event_C
```

Each worker blocks on `prev_committed.wait()` only after its disk write is complete. The wait does not delay the write — only the commit.

## Crash Safety

### Crash during Phase 1 (write)
- Snapshot still in immutable queue
- Incomplete SSTable directory has no `meta.json` — ignored on recovery
- WAL intact — recovery replays all entries

### Crash after register, before pop
- SSTable registered and readable
- Snapshot still in queue (redundant but safe)
- WAL replay deduplicates by sequence number

### Crash after pop, before WAL truncate
- SSTable has all data
- WAL entries still present — replayed and deduplicated on recovery

### Crash after WAL truncate
- Clean state: SSTable has data, WAL trimmed

## Out-of-Order Completion

When a newer snapshot finishes writing before an older one:

```
snap0 (oldest):  ──[write: 400ms]─────────────────[commit A]──►
snap1 (newer):   ──[write: 200ms]──[wait: 200ms]──────────────[commit B]──►
```

snap1 finishes writing 200ms early. It blocks on `Event_A.wait()` until snap0 commits. Total elapsed: 400ms + commit overhead. No ordering violation.

## State Machine (2 Workers)

```
Queue at dispatch:  [snap1(newer)]  [snap0(oldest)]

slot0: prev=pre_set,  my=Event_A
slot1: prev=Event_A,  my=Event_B

Worker A picks slot0:
  Phase 1: write snap0 to disk
  Phase 2: prev already set → commit immediately
           register L0a → pop snap0 → truncate WAL
           set Event_A

Worker B picks slot1:
  Phase 1: write snap1 to disk (concurrent with A)
  Phase 2: wait for Event_A
           register L0b → pop snap1 → truncate WAL
           set Event_B
```

After both complete: queue empty, both SSTables registered, WAL truncated, Event_B set for the next batch.

## Failure Propagation

If any slot fails during Phase 1 (write):

1. `batch_abort` event is set — all downstream slots skip their commits
2. `my_committed` is set in the `finally` block — prevents chain deadlock
3. The failed snapshot stays in the queue — retried on the next dispatch cycle
4. WAL is not truncated — data remains recoverable
