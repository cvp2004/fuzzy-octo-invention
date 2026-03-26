# Configuration

Runtime-mutable, disk-persisted engine configuration.

## LSMConfig

::: app.engine.config.LSMConfig

## Bloom Filter Settings

The bloom filter's false positive rate is environment-aware:

| Key | Dev default | Prod default | Description |
|-----|-------------|--------------|-------------|
| `bloom_fpr_dev` | `0.05` | — | FPR used when `env = "dev"` |
| `bloom_fpr_prod` | — | `0.01` | FPR used when `env = "prod"` |

The convenience property `config.bloom_fpr` auto-selects based on the current `env`.

The expected item count is derived from the actual data — not configurable:

- **Flush**: exact snapshot size (`len(snapshot)`)
- **Compaction**: total input record count

Update at runtime:

```python
engine.update_config("bloom_fpr_prod", 0.001)  # tighter FPR
```

## ConfigError

::: app.engine.config.ConfigError
