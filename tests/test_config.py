"""Tests for project scaffold — config loading and snapshot layout."""

from pathlib import Path

import pandas as pd
import pytest

from src.config_loader import (
    get_project_root,
    load_bronze_schema,
    load_config,
    snapshot_path,
)

PROJECT_ROOT = get_project_root()
TABLES = [
    "players",
    "games",
    "appearances",
    "clubs",
    "competitions",
    "player_valuations",
]


def test_project_root_exists():
    assert (PROJECT_ROOT / "config" / "config.yaml").exists()


def test_load_config_defaults():
    config = load_config()
    assert config["project"]["name"] == "football-analytics-pipeline"
    assert config["runtime"]["engine"] in ("pandas", "spark")
    assert config["paths"]["bronze"] == "data/bronze"


def test_snapshot_paths():
    config = load_config()
    day1 = snapshot_path(config, "data_day1")
    day2 = snapshot_path(config, "data_day2")
    assert day1.name == "data_day1"
    assert day2.name == "data_day2"
    assert day1.exists() and day2.exists()


@pytest.mark.parametrize("snapshot", ["data_day1", "data_day2"])
@pytest.mark.parametrize("table", TABLES)
def test_snapshot_has_all_tables(snapshot: str, table: str):
    config = load_config()
    csv_path = snapshot_path(config, snapshot) / f"{table}.csv"
    assert csv_path.exists(), f"Missing {csv_path}"


def test_bronze_schema_covers_all_tables():
    schema = load_bronze_schema()
    for table in TABLES:
        assert table in schema
        assert "player_id" in schema["players"] or table != "players"


def test_day2_has_incremental_deltas():
    config = load_config()
    day1_games = pd.read_csv(snapshot_path(config, "data_day1") / "games.csv")
    day2_games = pd.read_csv(snapshot_path(config, "data_day2") / "games.csv")
    assert len(day2_games) > len(day1_games)

    day1_players = pd.read_csv(snapshot_path(config, "data_day1") / "players.csv")
    day2_players = pd.read_csv(snapshot_path(config, "data_day2") / "players.csv")
    palmer_day1 = day1_players.loc[day1_players["player_id"] == 1003, "current_club_id"].iloc[0]
    palmer_day2 = day2_players.loc[day2_players["player_id"] == 1003, "current_club_id"].iloc[0]
    assert palmer_day1 != palmer_day2
    assert "is_deleted" in day2_players.columns
    assert day2_players.loc[day2_players["player_id"] == 1005, "is_deleted"].iloc[0] == 1