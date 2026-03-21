"""Unit tests for LSMConfig."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.engine.config import ConfigError, LSMConfig


class TestConfigDefaults:
    """Default values and initial load."""

    def test_defaults_created(self, tmp_path: Path) -> None:
        cfg = LSMConfig.load(tmp_path)
        assert cfg.max_memtable_size_mb == 64
        assert cfg.max_memtable_entries == 10
        assert cfg.immutable_queue_max_len == 4
        assert cfg.backpressure_timeout == 60.0
        assert cfg.l0_compaction_threshold == 10
        assert cfg.compaction_check_interval == 0.5
        assert cfg.block_size == 4096

    def test_config_file_written(self, tmp_path: Path) -> None:
        LSMConfig.load(tmp_path)
        path = tmp_path / "config.json"
        assert path.exists()
        data = json.loads(path.read_text())
        assert data["max_memtable_size_mb"] == 64

    def test_max_memtable_bytes_conversion(self, tmp_path: Path) -> None:
        cfg = LSMConfig.load(tmp_path)
        assert cfg.max_memtable_bytes == 64 * 1024 * 1024


class TestConfigLoad:
    """Loading from existing file."""

    def test_load_existing(self, tmp_path: Path) -> None:
        path = tmp_path / "config.json"
        path.write_text(json.dumps({"max_memtable_size_mb": 128}))

        cfg = LSMConfig.load(tmp_path)
        assert cfg.max_memtable_size_mb == 128
        # Other fields should be defaults
        assert cfg.block_size == 4096

    def test_load_corrupt_uses_defaults(self, tmp_path: Path) -> None:
        path = tmp_path / "config.json"
        path.write_text("not json!!!")

        cfg = LSMConfig.load(tmp_path)
        assert cfg.max_memtable_size_mb == 64  # fell back to defaults

    def test_custom_path(self, tmp_path: Path) -> None:
        custom = tmp_path / "custom" / "my.json"
        cfg = LSMConfig.load(tmp_path, config_path=custom)
        assert custom.exists()
        assert cfg.max_memtable_size_mb == 64


class TestConfigSet:
    """Runtime updates."""

    def test_set_valid(self, tmp_path: Path) -> None:
        cfg = LSMConfig.load(tmp_path)
        old, new = cfg.set("max_memtable_size_mb", 128)
        assert old == 64
        assert new == 128
        assert cfg.max_memtable_size_mb == 128

    def test_set_persists_to_disk(self, tmp_path: Path) -> None:
        cfg = LSMConfig.load(tmp_path)
        cfg.set("block_size", 8192)

        # Reload from disk
        cfg2 = LSMConfig.load(tmp_path)
        assert cfg2.block_size == 8192

    def test_set_unknown_key_raises(self, tmp_path: Path) -> None:
        cfg = LSMConfig.load(tmp_path)
        with pytest.raises(ConfigError, match="Unknown config key"):
            cfg.set("nonexistent", 42)

    def test_set_negative_raises(self, tmp_path: Path) -> None:
        cfg = LSMConfig.load(tmp_path)
        with pytest.raises(ConfigError, match="must be positive"):
            cfg.set("max_memtable_size_mb", -1)

    def test_set_zero_raises(self, tmp_path: Path) -> None:
        cfg = LSMConfig.load(tmp_path)
        with pytest.raises(ConfigError, match="must be positive"):
            cfg.set("max_memtable_size_mb", 0)

    def test_set_float_for_int_field(self, tmp_path: Path) -> None:
        cfg = LSMConfig.load(tmp_path)
        cfg.set("max_memtable_size_mb", 32.5)
        # Should be cast to int (32)
        assert cfg.max_memtable_size_mb == 32

    def test_set_int_for_float_field(self, tmp_path: Path) -> None:
        cfg = LSMConfig.load(tmp_path)
        cfg.set("backpressure_timeout", 60)
        assert cfg.backpressure_timeout == 60.0


class TestConfigSerialization:
    """to_dict and to_json."""

    def test_to_dict(self, tmp_path: Path) -> None:
        cfg = LSMConfig.load(tmp_path)
        d = cfg.to_dict()
        assert isinstance(d, dict)
        assert "max_memtable_size_mb" in d

    def test_to_json(self, tmp_path: Path) -> None:
        cfg = LSMConfig.load(tmp_path)
        j = cfg.to_json()
        parsed = json.loads(j)
        assert parsed["max_memtable_size_mb"] == 64

    def test_unknown_attr_raises(self, tmp_path: Path) -> None:
        cfg = LSMConfig.load(tmp_path)
        with pytest.raises(AttributeError, match="Unknown config key"):
            _ = cfg.nonexistent_field  # type: ignore[attr-defined]
