# Getting Started

## Installation

```bash
uv sync
```

## Usage

### REPL Mode

```bash
uv run python main.py
```

Available commands: `put`, `get`, `del`, `flush`, `mem`, `disk`, `stats`, `config`, `help`.

### As a Library

```python
from app.engine import LSMEngine

async def main():
    engine = await LSMEngine.open("./data")

    # Write
    await engine.put(b"user:1", b"Alice")

    # Read
    value = await engine.get(b"user:1")  # b"Alice"

    # Delete
    await engine.delete(b"user:1")

    # Force flush memtable to SSTable
    await engine.flush()

    # Inspect
    print(engine.stats())

    # Shutdown
    await engine.close()
```

### Configuration

```python
engine = await LSMEngine.open("./data")

# Read config
print(engine.config.max_memtable_entries)

# Update at runtime
old, new = engine.update_config("max_memtable_entries", 500_000)
```

See [`LSMConfig`](api/config.md) for all available settings.
