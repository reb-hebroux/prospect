"""Tests for bronze layer ingest."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from src.config_loader import get_project_root, load_bronze_schema, load_config, snapshot_path
from src.ingestion.bronze import ingest_table, run_bronze_ingest
from src.ingestion.schema import SchemaValidationError, validate_and_prepare
from src.load.parquet_writer import read_parquet

PROJECT_ROOT = get_project_root()


@pytest.fixture
def bronze_config(tmp_path: Path):
    """Config pointing bronze output to a temp directory."""
    config = load_config()
    config["paths"]["bronze"] = str(tmp_path / "bronze")
    return config


def test_validate_adds_optional_is_deleted():
    schema = load_bronze_schema()
    df = pd.read_csv(snapshot_path(load_config(), "data_day1") / "players.csv")
    assert "is_deleted" not in df.columns

    prepared, report = validate_and_prepare(df, "players", schema["players"])
    assert "is_deleted" in prepared.columns
    assert "is_deleted" in report.optional_added
    assert prepared["is_deleted"].tolist() == [0, 0, 0, 0, 0]


def test_validate_rejects_missing_required_column():
    schema = load_bronze_schema()
    df = pd.DataFrame({"player_id": [1], "name": ["Test"]})
    with pytest.raises(SchemaValidationError, match="missing required columns"):
        validate_and_prepare(df, "players", schema["players"])


def test_validate_drops_extra_columns():
    schema = load_bronze_schema()
    df = pd.read_csv(snapshot_path(load_config(), "data_day1") / "players.csv")
    df["unexpected_col"] = "x"

    prepared, report = validate_and_prepare(df, "players", schema["players"])
    assert "unexpected_col" not in prepared.columns
    assert "unexpected_col" in report.extra_columns


def test_ingest_table_writes_parquet(tmp_path: Path):
    schema = load_bronze_schema()
    source = snapshot_path(load_config(), "data_day1")
    output = tmp_path / "bronze" / "data_day1"

    result = ingest_table("games", source, output, schema["games"])
    parquet_path = Path(result["output"])

    assert parquet_path.exists()
    assert result["row_count"] == 4
    df = read_parquet(parquet_path)
    assert list(df.columns) == schema["games"]
    assert pd.api.types.is_datetime64_any_dtype(df["date"])


def test_run_bronze_ingest_day1(bronze_config):
    manifest = run_bronze_ingest(config=bronze_config, snapshot="data_day1")

    assert manifest["layer"] == "bronze"
    assert manifest["snapshot"] == "data_day1"
    assert len(manifest["tables"]) == 6

    bronze_dir = Path(bronze_config["paths"]["bronze"]) / "data_day1"
    assert (bronze_dir / "_ingest_manifest.json").exists()
    assert (bronze_dir / "players.parquet").exists()
    assert (bronze_dir / "games.parquet").exists()

    players = read_parquet(bronze_dir / "players.parquet")
    assert len(players) == 5
    assert "is_deleted" in players.columns


def test_run_bronze_ingest_day2_more_rows(bronze_config):
    run_bronze_ingest(config=bronze_config, snapshot="data_day1")
    run_bronze_ingest(config=bronze_config, snapshot="data_day2")

    bronze_root = Path(bronze_config["paths"]["bronze"])
    day1_games = len(read_parquet(bronze_root / "data_day1" / "games.parquet"))
    day2_games = len(read_parquet(bronze_root / "data_day2" / "games.parquet"))
    assert day2_games > day1_games

    day2_players = read_parquet(bronze_root / "data_day2" / "players.parquet")
    palmer = day2_players.loc[day2_players["player_id"] == 1003, "current_club_id"].iloc[0]
    assert palmer == 103
    assert day2_players.loc[day2_players["player_id"] == 1005, "is_deleted"].iloc[0] == 1


def test_manifest_json_serializable(bronze_config):
    manifest = run_bronze_ingest(config=bronze_config, snapshot="data_day1")
    # round-trip test
    text = json.dumps(manifest)
    parsed = json.loads(text)
    assert parsed["snapshot"] == "data_day1"