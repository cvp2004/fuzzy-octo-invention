"""Shared test fixtures for lsm-kv."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from app.engine.config import LSMConfig
from app.observability.logging import reset_logging


@pytest.fixture(autouse=True)
def _disable_tcp_logging_in_tests(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Disable TCP log server in tests to avoid port conflicts."""
    monkeypatch.setenv("LSM_LOG_PORT", "0")
    yield
    reset_logging()


@pytest.fixture()
def test_config(tmp_path: Path) -> LSMConfig:
    """Return an LSMConfig backed by a temp directory."""
    return LSMConfig.load(tmp_path / "data")
