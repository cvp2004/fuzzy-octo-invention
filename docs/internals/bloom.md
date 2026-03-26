# Bloom Filter

Probabilistic membership test for fast negative lookups.

## Configuration

The bloom filter's false positive rate is controlled per environment via `config.json`:

| Config key | Default | Effect |
|------------|---------|--------|
| `bloom_fpr_dev` | `0.05` (5%) | Smaller filters, faster builds — suited for small dev datasets |
| `bloom_fpr_prod` | `0.01` (1%) | Fewer false disk reads — suited for production workloads |

Access the active value via the convenience property:

```python
engine.config.bloom_fpr  # 0.05 in dev, 0.01 in prod
```

The expected item count (`bloom_n`) is **not configurable** — it is derived from the actual data:

- **Flush path**: `len(snapshot)` — exact entry count of the immutable memtable
- **Compaction path**: sum of `reader.meta.record_count` across all input SSTables

This ensures the bloom filter is optimally sized for every SSTable.

## BloomFilter

::: app.bloom.filter.BloomFilter
