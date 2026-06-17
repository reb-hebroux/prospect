"""Shared pytest fixtures and helpers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from src.config_loader import load_config


def make_isolated_config(
    tmp_path: Path,
    *,
    bronze: bool = True,
    silver: bool = False,
    gold: bool = False,
    dq_reports: bool = False,
    state: bool = False,
) -> dict[str, Any]:
    """Build a config dict with output paths redirected to a temp directory."""
    config = load_config()
    if bronze:
        config["paths"]["bronze"] = str(tmp_path / "bronze")
    if silver:
        config["paths"]["silver"] = str(tmp_path / "silver")
    if gold:
        config["paths"]["gold"] = str(tmp_path / "gold")
    if dq_reports:
        config["paths"]["dq_reports"] = str(tmp_path / "reports" / "dq")
    if state:
        config["incremental"]["state_file"] = str(tmp_path / "state" / "pipeline_state.json")
        config["incremental"]["cumulative_bronze"] = str(tmp_path / "bronze" / "cumulative")
    return config


@pytest.fixture
def full_pipeline_config(tmp_path: Path) -> dict[str, Any]:
    """Isolated config with all medallion output paths and checkpoint state."""
    return make_isolated_config(
        tmp_path,
        bronze=True,
        silver=True,
        gold=True,
        dq_reports=True,
        state=True,
    )