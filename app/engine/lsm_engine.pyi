"""Type stub for app.engine.lsm_engine — public API surface."""

from dataclasses import dataclass
from pathlib import Path

from app.common.abc import StorageEngine
from app.engine.config import LSMConfig
from app.types import Key, SeqNum, Value

@dataclass(frozen=True)
class EngineStats:
    key_count: int
    seq: SeqNum
    wal_entry_count: int
    data_root: str
    active_table_id: str
    active_size_bytes: int
    immutable_queue_len: int
    immutable_snapshots: list[str]
    l0_sstable_count: int

class LSMEngine(StorageEngine):
    @property
    def data_root(self) -> Path: ...
    @property
    def log_port(self) -> int: ...
    @property
    def config(self) -> LSMConfig: ...

    @classmethod
    async def open(
        cls,
        data_root: str | Path = "./data",
        config_path: Path | None = None,
    ) -> LSMEngine: ...

    async def put(self, key: Key, value: Value) -> None: ...
    async def get(self, key: Key) -> Value | None: ...
    async def delete(self, key: Key) -> None: ...
    async def flush(self) -> bool: ...
    async def close(self) -> None: ...

    def update_config(
        self, key: str, value: int | float | str,
    ) -> tuple[int | float | str, int | float | str]: ...
    def stats(self) -> EngineStats: ...
    def show_mem(self, table_id: str | None = None) -> dict[str, object]: ...
    def show_disk(self, file_id: str | None = None) -> dict[str, object]: ...
