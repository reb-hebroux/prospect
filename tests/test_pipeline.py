"""Tests for pipeline CLI entry points and end-to-end orchestration."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.pipeline.runner import main, reset_pipeline_state, run_incremental_pipeline

from tests.conftest import make_isolated_config


def test_pipeline_main_reset(monkeypatch):
    monkeypatch.setenv("PIPELINE_RESET", "true")
    monkeypatch.delenv("PIPELINE_FORCE", raising=False)
    assert main() == 0


def test_pipeline_main_runs_incremental(monkeypatch, tmp_path: Path):
    config = make_isolated_config(
        tmp_path,
        bronze=True,
        silver=True,
        gold=True,
        dq_reports=True,
        state=True,
    )
    state_path = Path(config["incremental"]["state_file"])
    monkeypatch.setenv("PIPELINE_RESET", "false")
    monkeypatch.setattr(
        "src.pipeline.runner.load_config",
        lambda *args, **kwargs: config,
    )

    assert main() == 0
    assert state_path.exists()


def test_incremental_pipeline_with_spark_engine_fails_fast(tmp_path: Path):
    config = make_isolated_config(
        tmp_path,
        bronze=True,
        silver=True,
        gold=True,
        dq_reports=True,
        state=True,
    )
    config["runtime"]["engine"] = "spark"

    with pytest.raises(NotImplementedError, match="ENGINE=spark is reserved"):
        run_incremental_pipeline(config=config)


def test_reset_pipeline_state_is_idempotent(tmp_path: Path):
    config = make_isolated_config(tmp_path, state=True)
    state_path = Path(config["incremental"]["state_file"])
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text("{}", encoding="utf-8")

    reset_pipeline_state(config)
    assert not state_path.exists()

    assert reset_pipeline_state(config) is None