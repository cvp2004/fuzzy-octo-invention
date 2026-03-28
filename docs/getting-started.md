# Getting Started

## Installation

### From Source (development)

```bash
uv sync                # install dependencies into virtual environment
uv pip install -e .    # install lsm-kv CLI in editable mode
```

### From Wheel (production)

```bash
uv build                                        # build the package
pip install dist/lsm_kv-0.1.0-py3-none-any.whl  # install anywhere
```

## Usage

The `lsm-kv` CLI provides three modes — REPL, API, and Web — each started with a separate subcommand:

```bash
lsm-kv --help
```

### REPL Mode

Start an interactive shell to read/write keys, inspect memtables and SSTables, and trace lookups:

```bash
lsm-kv repl
# or without installing the CLI:
uv run python main.py
```

Available commands: `put`, `get`, `del`, `flush`, `mem`, `disk`, `stats`, `config`, `trace`, `help`.

### API Mode

Start only the FastAPI REST server (no frontend):

```bash
lsm-kv api                         # default: 0.0.0.0:8081
lsm-kv api --port 9000 --reload    # custom port with auto-reload
```

API endpoints are served under `/api/v1/` — KV operations, memtable/SSTable inspection, compaction control, stats, config, and a terminal interface.

### Web Explorer

Build the React frontend and start the full web dashboard:

```bash
lsm-kv web                   # builds frontend then starts server on :8081
lsm-kv web --skip-build      # reuse existing frontend/dist/
lsm-kv web --port 3000       # custom port
```

Open `http://localhost:8081` for the interactive dashboard with KV explorer, memtable viewer, SSTable browser, compaction monitor, live log streaming, and web terminal.

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
