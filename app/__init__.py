"""lsm-kv — a Log-Structured Merge Tree key-value store.

Provides a durable, crash-safe, async-first storage engine with:

- Write-ahead log (WAL) for crash recovery
- Concurrent skip-list memtable with freeze-to-immutable pipeline
- SSTable persistence with bloom filters and sparse indexes
- Tiered compaction (L0 → L1 → L2 → L3)
- Runtime-mutable configuration

Quick start::

    from app.engine import LSMEngine

    engine = await LSMEngine.open("./data")
    await engine.put(b"key", b"value")
    value = await engine.get(b"key")
    await engine.close()
"""