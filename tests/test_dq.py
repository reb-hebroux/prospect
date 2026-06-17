"""Tests for data quality checks and reporting."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from src.config_loader import load_config
from src.dq.checks import (
    check_duplicates,
    check_nulls,
    check_referential_integrity,
    check_valid_ranges,
)
from src.dq.exceptions import DataQualityError
from src.dq.runner import run_checks, run_dq_checks
from src.ingestion.bronze import run_bronze_ingest


@pytest.fixture
def pipeline_config(tmp_path: Path):
    config = load_config()
    config["paths"]["bronze"] = str(tmp_path / "bronze")
    config["paths"]["dq_reports"] = str(tmp_path / "reports" / "dq")
    return config


def test_check_nulls_detects_missing_values():
    tables = {
        "appearances": pd.DataFrame(
            {
                "appearance_id": [1, 2],
                "player_id": [1001, None],
                "game_id": [2001, 2002],
            }
        )
    }
    results = check_nulls(tables, [{"table": "appearances", "columns": ["player_id", "game_id"]}])
    assert len(results) == 1
    assert results[0].passed is False
    assert results[0].violation_count == 1


def test_check_referential_integrity_detects_orphans():
    tables = {
        "appearances": pd.DataFrame({"player_id": [1001, 9999]}),
        "players": pd.DataFrame({"player_id": [1001]}),
    }
    rules = [
        {
            "child_table": "appearances",
            "child_column": "player_id",
            "parent_table": "players",
            "parent_column": "player_id",
        }
    ]
    results = check_referential_integrity(tables, rules)
    assert results[0].passed is False
    assert results[0].violation_count == 1
    assert 9999 in results[0].details["orphan_keys_sample"]


def test_check_valid_ranges_detects_negative_minutes():
    tables = {
        "appearances": pd.DataFrame(
            {"minutes_played": [90, -5], "goals": [1, 0]}
        )
    }
    results = check_valid_ranges(
        tables,
        [{"table": "appearances", "column": "minutes_played", "min": 0}],
    )
    assert results[0].passed is False
    assert results[0].violation_count == 1


def test_check_duplicates_detects_repeated_keys():
    tables = {
        "appearances": pd.DataFrame(
            {"appearance_id": [3001, 3001, 3002], "player_id": [1001, 1001, 1002]}
        )
    }
    results = check_duplicates(
        tables,
        [{"table": "appearances", "columns": ["appearance_id"]}],
    )
    assert results[0].passed is False
    assert results[0].violation_count == 2


def test_run_dq_checks_passes_day1(pipeline_config):
    run_bronze_ingest(config=pipeline_config, snapshot="data_day1")
    report = run_dq_checks(config=pipeline_config, snapshot="data_day1")

    assert report["layer"] == "dq"
    assert report["passed"] is True
    assert report["summary"]["failed_checks"] == 0
    assert report["summary"]["total_checks"] >= 5

    report_path = Path(report["report_path"])
    assert report_path.exists()
    latest = Path(pipeline_config["paths"]["dq_reports"]) / "dq_report_data_day1_latest.json"
    assert latest.exists()

    parsed = json.loads(report_path.read_text(encoding="utf-8"))
    check_types = {c["check_type"] for c in parsed["checks"]}
    assert "null_check" in check_types
    assert "referential_integrity" in check_types
    assert "valid_range" in check_types
    assert "duplicate_check" in check_types
    assert "schema_drift" in check_types


def test_run_dq_checks_fails_on_invalid_data(pipeline_config):
    run_bronze_ingest(config=pipeline_config, snapshot="data_day1")

    bronze_dir = Path(pipeline_config["paths"]["bronze"]) / "data_day1"
    appearances = pd.read_parquet(bronze_dir / "appearances.parquet")
    appearances.loc[0, "goals"] = -1
    appearances.to_parquet(bronze_dir / "appearances.parquet", index=False)

    with pytest.raises(DataQualityError):
        run_dq_checks(config=pipeline_config, snapshot="data_day1")

    report = run_dq_checks(
        config=pipeline_config,
        snapshot="data_day1",
        fail_on_error=False,
    )
    assert report["passed"] is False
    failed = [c for c in report["checks"] if not c["passed"]]
    assert any(c["check_type"] == "valid_range" for c in failed)


def test_run_checks_respects_config_without_bonus(pipeline_config):
    tables = {
        "appearances": pd.DataFrame(
            {
                "appearance_id": [1],
                "player_id": [1001],
                "game_id": [2001],
                "minutes_played": [90],
                "goals": [1],
            }
        ),
        "players": pd.DataFrame({"player_id": [1001]}),
    }
    dq_config = load_config()["data_quality"].copy()
    dq_config.pop("bonus", None)

    results = run_checks(tables, dq_config, manifest=None)
    assert len(results) == 4
    assert all(r.passed for r in results)