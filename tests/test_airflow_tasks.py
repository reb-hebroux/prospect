"""Tests for Airflow task wrappers (no Airflow runtime required)."""

from __future__ import annotations

from pathlib import Path

import pytest

from src.pipeline.airflow_tasks import (
    finalize_snapshot_state,
    get_next_snapshot_name,
    resolve_snapshot_for_dag,
    run_bronze_step,
    run_dq_step,
    run_gold_step,
    run_silver_step,
)


@pytest.fixture
def pipeline_config(tmp_path: Path):
    from src.config_loader import load_config

    config = load_config()
    config["paths"]["bronze"] = str(tmp_path / "bronze")
    config["paths"]["silver"] = str(tmp_path / "silver")
    config["paths"]["gold"] = str(tmp_path / "gold")
    config["paths"]["dq_reports"] = str(tmp_path / "reports" / "dq")
    config["incremental"]["state_file"] = str(tmp_path / "state" / "pipeline_state.json")
    config["incremental"]["cumulative_bronze"] = str(tmp_path / "bronze" / "cumulative")
    return config


def test_get_next_snapshot_name_returns_day1(pipeline_config):
    assert get_next_snapshot_name(pipeline_config) == "data_day1"


def test_resolve_snapshot_for_dag_honours_env(pipeline_config, monkeypatch):
    monkeypatch.setenv("DATA_SNAPSHOT", "data_day2")
    assert resolve_snapshot_for_dag(pipeline_config) == "data_day2"


def test_airflow_step_wrappers_run_end_to_end(pipeline_config):
    snapshot = "data_day1"

    bronze_result = run_bronze_step(snapshot, config=pipeline_config)
    assert bronze_result["snapshot"] == snapshot
    assert "bronze" in bronze_result
    assert "cumulative" in bronze_result

    dq_result = run_dq_step(snapshot, config=pipeline_config)
    assert "dq" in dq_result

    silver_result = run_silver_step(snapshot, config=pipeline_config)
    assert "silver" in silver_result

    gold_result = run_gold_step(config=pipeline_config)
    assert "gold" in gold_result

    state_result = finalize_snapshot_state(snapshot, bronze_result, config=pipeline_config)
    assert state_result["snapshot"] == snapshot
    assert Path(state_result["state_path"]).exists()

    assert get_next_snapshot_name(pipeline_config) == "data_day2"