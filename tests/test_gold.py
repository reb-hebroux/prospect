"""Tests for gold layer aggregates."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from src.config_loader import load_config
from src.ingestion.bronze import run_bronze_ingest
from src.load.parquet_writer import read_parquet
from src.transform.aggregates import (
    build_gold_club_performance,
    build_gold_player_performance,
    build_gold_player_valuation_trend,
)
from src.transform.gold import run_gold_aggregation
from src.transform.silver import run_silver_transform


@pytest.fixture
def pipeline_config(tmp_path: Path):
    config = load_config()
    config["paths"]["bronze"] = str(tmp_path / "bronze")
    config["paths"]["silver"] = str(tmp_path / "silver")
    config["paths"]["gold"] = str(tmp_path / "gold")
    return config


def test_build_gold_player_performance_season_grain():
    fact_appearances = pd.DataFrame(
        {
            "appearance_id": [1, 2, 3],
            "game_id": [2001, 2002, 2005],
            "player_id": [1001, 1001, 1001],
            "date_sk": [20240817, 20240817, 20260315],
            "goals": [1, 1, 2],
            "assists": [0, 0, 1],
            "minutes_played": [90, 90, 90],
        }
    )
    dim_date = pd.DataFrame(
        {
            "date_sk": [20240817, 20260315],
            "full_date": pd.to_datetime(["2024-08-17", "2026-03-15"]),
            "season": [2024, 2025],
        }
    )
    dim_players = pd.DataFrame(
        {
            "player_id": [1001],
            "name": ["Bukayo Saka"],
            "position_category": ["Attack"],
            "current_club_id": [101],
            "current_club_name": ["Arsenal FC"],
            "is_current": [True],
        }
    )

    result = build_gold_player_performance(fact_appearances, dim_date, dim_players)
    saka_2024 = result[(result["player_id"] == 1001) & (result["season"] == 2024)].iloc[0]
    saka_2025 = result[(result["player_id"] == 1001) & (result["season"] == 2025)].iloc[0]

    assert saka_2024["matches_played"] == 2
    assert saka_2024["total_goals"] == 2
    assert saka_2025["matches_played"] == 1
    assert saka_2025["total_goals"] == 2


def test_build_gold_club_performance_home_and_away():
    fact_games = pd.DataFrame(
        {
            "game_id": [2001, 2002, 2004],
            "season_derived": [2024, 2024, 2024],
            "home_club_id": [101, 103, 101],
            "away_club_id": [102, 101, 103],
            "home_club_name": ["Arsenal FC", "Liverpool FC", "Arsenal FC"],
            "away_club_name": ["Chelsea FC", "Arsenal FC", "Liverpool FC"],
            "home_club_goals": [2, 1, 1],
            "away_club_goals": [1, 2, 1],
            "home_result": ["W", "L", "D"],
            "away_result": ["L", "W", "D"],
        }
    )
    dim_clubs = pd.DataFrame(
        {
            "club_id": [101, 102, 103],
            "name": ["Arsenal FC", "Chelsea FC", "Liverpool FC"],
        }
    )

    result = build_gold_club_performance(fact_games, dim_clubs)
    arsenal = result[(result["club_id"] == 101) & (result["season"] == 2024)].iloc[0]

    assert arsenal["matches_played"] == 3
    assert arsenal["wins"] == 2
    assert arsenal["draws"] == 1
    assert arsenal["losses"] == 0
    assert arsenal["goals_scored"] == 5
    assert arsenal["goals_conceded"] == 3


def test_build_gold_player_valuation_trend_rolling_average():
    valuations = pd.DataFrame(
        {
            "player_id": [1004, 1004, 1004],
            "valuation_date": pd.to_datetime(["2024-08-01", "2024-09-01", "2026-03-01"]),
            "market_value_in_eur": [110_000_000, 112_000_000, 125_000_000],
            "current_club_id": [103, 103, 103],
            "current_club_name": ["Liverpool FC"] * 3,
            "is_current": [False, False, True],
        }
    )

    result = build_gold_player_valuation_trend(valuations, rolling_window=3)
    salah = result[result["player_id"] == 1004]

    assert len(salah) == 3
    assert salah.iloc[-1]["market_value"] == 125_000_000
    assert salah.iloc[-1]["rolling_average_market_value"] == 115_666_667.0


def test_run_gold_aggregation_day1(pipeline_config):
    run_bronze_ingest(config=pipeline_config, snapshot="data_day1")
    run_silver_transform(config=pipeline_config, snapshot="data_day1")
    manifest = run_gold_aggregation(config=pipeline_config)

    assert manifest["layer"] == "gold"
    assert len(manifest["tables"]) == 3

    gold_dir = Path(pipeline_config["paths"]["gold"])
    assert (gold_dir / "_aggregate_manifest.json").exists()
    assert (gold_dir / "gold_player_performance.parquet").exists()
    assert (gold_dir / "gold_club_performance.parquet").exists()
    assert (gold_dir / "gold_player_valuation_trend.parquet").exists()

    player_perf = read_parquet(gold_dir / "gold_player_performance.parquet")
    assert len(player_perf) >= 5
    assert "matches_played" in player_perf.columns

    club_perf = read_parquet(gold_dir / "gold_club_performance.parquet")
    assert "wins" in club_perf.columns
    assert "goals_scored" in club_perf.columns


def test_run_gold_after_day2_excludes_deleted_player(pipeline_config):
    run_bronze_ingest(config=pipeline_config, snapshot="data_day1")
    run_silver_transform(config=pipeline_config, snapshot="data_day1")
    run_bronze_ingest(config=pipeline_config, snapshot="data_day2")
    run_silver_transform(config=pipeline_config, snapshot="data_day2")
    run_gold_aggregation(config=pipeline_config)

    gold_dir = Path(pipeline_config["paths"]["gold"])
    player_perf = read_parquet(gold_dir / "gold_player_performance.parquet")

    # van Dijk still has historical appearances but is not a current player
    assert 1005 in player_perf["player_id"].tolist()
    assert player_perf.loc[player_perf["player_id"] == 1005, "name"].isna().all()

    palmer = player_perf[player_perf["player_id"] == 1003].iloc[0]
    assert palmer["current_club_id"] == 103

    games_after_day2 = read_parquet(Path(pipeline_config["paths"]["silver"]) / "fact_games.parquet")
    club_perf = read_parquet(gold_dir / "gold_club_performance.parquet")
    arsenal_2025 = club_perf[(club_perf["club_id"] == 101) & (club_perf["season"] == 2025)]
    assert len(arsenal_2025) == 1
    assert len(games_after_day2) == 7


def test_gold_manifest_json_serializable(pipeline_config):
    run_bronze_ingest(config=pipeline_config, snapshot="data_day1")
    run_silver_transform(config=pipeline_config, snapshot="data_day1")
    manifest = run_gold_aggregation(config=pipeline_config)
    parsed = json.loads(json.dumps(manifest))
    assert parsed["layer"] == "gold"