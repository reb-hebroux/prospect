"""Tests for incremental and idempotent pipeline behaviour."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.config_loader import load_config, snapshot_path
from src.ingestion.bronze import run_bronze_ingest
from src.ingestion.cumulative import cumulative_bronze_dir, merge_snapshot_to_cumulative
from src.load.incremental import compute_source_hash, upsert_dataframe
from src.load.parquet_writer import read_parquet
from src.load.state import (
    PipelineState,
    load_state,
    mark_snapshot_processed,
    should_process_snapshot,
)
from src.pipeline.runner import (
    reset_pipeline_state,
    run_incremental_pipeline,
    run_snapshot_pipeline,
)


@pytest.fixture
def pipeline_config(tmp_path: Path):
    config = load_config()
    config["paths"]["bronze"] = str(tmp_path / "bronze")
    config["paths"]["silver"] = str(tmp_path / "silver")
    config["paths"]["gold"] = str(tmp_path / "gold")
    config["paths"]["dq_reports"] = str(tmp_path / "reports" / "dq")
    config["incremental"]["state_file"] = str(tmp_path / "state" / "pipeline_state.json")
    config["incremental"]["cumulative_bronze"] = str(tmp_path / "bronze" / "cumulative")
    return config


def test_upsert_dataframe_is_idempotent():
    existing = pd.DataFrame({"game_id": [2001, 2002], "home_club_goals": [2, 1]})
    incoming = pd.DataFrame({"game_id": [2002, 2003], "home_club_goals": [1, 3]})
    first = upsert_dataframe(existing, incoming, ["game_id"])
    second = upsert_dataframe(first, incoming, ["game_id"])
    pd.testing.assert_frame_equal(first, second)


def test_should_process_snapshot_skips_unchanged_hash():
    state = PipelineState()
    mark_snapshot_processed(
        state,
        "data_day1",
        content_hash="abc123",
        game_date_max="2024-08-25",
        merge_stats={},
    )
    assert should_process_snapshot(state, "data_day1", "abc123") is False
    assert should_process_snapshot(state, "data_day1", "changed") is True
    assert should_process_snapshot(state, "data_day1", "abc123", force=True) is True


def test_merge_snapshot_to_cumulative_tracks_new_games(pipeline_config):
    run_bronze_ingest(config=pipeline_config, snapshot="data_day1")
    day1 = merge_snapshot_to_cumulative(pipeline_config, "data_day1")
    assert day1["tables"]["games"]["cumulative_rows"] == 4

    run_bronze_ingest(config=pipeline_config, snapshot="data_day2")
    day2 = merge_snapshot_to_cumulative(
        pipeline_config,
        "data_day2",
        watermark_date=day1["game_date_max"],
    )
    assert day2["tables"]["games"]["cumulative_rows"] == 7
    assert day2["tables"]["games"]["rows_after_watermark"] == 3
    assert day2["tables"]["games"]["new_keys"] == 3

    cumulative_games = read_parquet(cumulative_bronze_dir(pipeline_config) / "games.parquet")
    assert len(cumulative_games) == 7


def test_run_incremental_pipeline_processes_snapshots_in_order(pipeline_config):
    summary = run_incremental_pipeline(config=pipeline_config)

    assert summary["processed_snapshots"] == ["data_day1", "data_day2"]
    assert summary["skipped_snapshots"] == []
    assert summary["watermarks"]["games_date"] == "2026-03-22"
    assert summary["watermarks"]["last_snapshot"] == "data_day2"

    silver_games = read_parquet(Path(pipeline_config["paths"]["silver"]) / "fact_games.parquet")
    assert len(silver_games) == 7

    state = load_state(pipeline_config)
    assert set(state.processed_snapshots) == {"data_day1", "data_day2"}


def test_incremental_rerun_skips_processed_snapshots(pipeline_config):
    first = run_incremental_pipeline(config=pipeline_config)
    second = run_incremental_pipeline(config=pipeline_config)

    assert first["processed_snapshots"] == ["data_day1", "data_day2"]
    assert second["processed_snapshots"] == []
    assert second["skipped_snapshots"] == ["data_day1", "data_day2"]


def test_forced_rerun_is_idempotent_for_silver_outputs(pipeline_config):
    run_incremental_pipeline(config=pipeline_config)
    silver_dir = Path(pipeline_config["paths"]["silver"])
    before = {
        name: read_parquet(silver_dir / f"{name}.parquet")
        for name in [
            "fact_games",
            "fact_appearances",
            "dim_players",
            "dim_player_valuations",
        ]
    }

    run_incremental_pipeline(config=pipeline_config, force=True)

    after = {
        name: read_parquet(silver_dir / f"{name}.parquet")
        for name in before
    }

    for name, df in before.items():
        assert len(df) == len(after[name])
        assert set(df.columns) == set(after[name].columns)

    palmer = after["dim_players"]
    palmer_rows = palmer[palmer["player_id"] == 1003]
    assert len(palmer_rows) == 2


def test_reset_pipeline_state(pipeline_config):
    run_incremental_pipeline(config=pipeline_config)
    state_path = Path(pipeline_config["incremental"]["state_file"])
    assert state_path.exists()

    reset_pipeline_state(pipeline_config)
    assert not state_path.exists()


def test_compute_source_hash_stable(pipeline_config):
    source = snapshot_path(pipeline_config, "data_day1")
    tables = pipeline_config["tables"]["source"]
    assert compute_source_hash(source, tables) == compute_source_hash(source, tables)


def test_run_snapshot_pipeline_updates_state(pipeline_config):
    run_bronze_ingest(config=pipeline_config, snapshot="data_day1")
    result = run_snapshot_pipeline(pipeline_config, "data_day1")

    assert result["processed"] is True
    assert Path(result["state_path"]).exists()
    state = load_state(pipeline_config)
    assert "data_day1" in state.processed_snapshots