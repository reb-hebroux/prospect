"""Tests for silver layer transforms."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from src.config_loader import load_config
from src.ingestion.bronze import run_bronze_ingest
from src.load.parquet_writer import read_parquet
from src.transform.silver import run_silver_transform


@pytest.fixture
def pipeline_config(tmp_path: Path):
    """Config with isolated bronze and silver output directories."""
    config = load_config()
    config["paths"]["bronze"] = str(tmp_path / "bronze")
    config["paths"]["silver"] = str(tmp_path / "silver")
    return config


def test_run_silver_transform_day1(pipeline_config):
    run_bronze_ingest(config=pipeline_config, snapshot="data_day1")
    manifest = run_silver_transform(config=pipeline_config, snapshot="data_day1")

    assert manifest["layer"] == "silver"
    assert manifest["snapshot"] == "data_day1"
    assert len(manifest["tables"]) == 7

    silver_dir = Path(pipeline_config["paths"]["silver"])
    assert (silver_dir / "_transform_manifest.json").exists()
    assert (silver_dir / "dim_players.parquet").exists()
    assert (silver_dir / "fact_games.parquet").exists()

    players = read_parquet(silver_dir / "dim_players.parquet")
    assert players["is_current"].sum() == 5
    assert "position_category" in players.columns
    assert "country_of_birth_iso" in players.columns

    games = read_parquet(silver_dir / "fact_games.parquet")
    assert len(games) == 4
    assert set(games["home_result"].dropna().unique()).issubset({"W", "L", "D"})
    assert games.loc[games["game_id"] == 2001, "season_derived"].iloc[0] == 2024


def test_silver_scd_day2_palmer_transfer(pipeline_config):
    run_bronze_ingest(config=pipeline_config, snapshot="data_day1")
    run_silver_transform(config=pipeline_config, snapshot="data_day1")

    run_bronze_ingest(config=pipeline_config, snapshot="data_day2")
    run_silver_transform(config=pipeline_config, snapshot="data_day2")

    silver_dir = Path(pipeline_config["paths"]["silver"])
    players = read_parquet(silver_dir / "dim_players.parquet")
    palmer = players[players["player_id"] == 1003].sort_values("effective_date")

    assert len(palmer) == 2
    assert palmer.iloc[0]["current_club_id"] == 102
    assert palmer.iloc[1]["current_club_id"] == 103
    assert bool(palmer.iloc[1]["is_current"])
    assert palmer.iloc[1]["effective_date"] == pd.Timestamp("2026-03-01")

    current = players[players["is_current"]]
    assert 1005 not in current["player_id"].tolist()

    games = read_parquet(silver_dir / "fact_games.parquet")
    assert len(games) == 7

    appearances = read_parquet(silver_dir / "fact_appearances.parquet")
    assert len(appearances) == 15
    assert "player_sk" in appearances.columns


def test_silver_manifest_json_serializable(pipeline_config):
    run_bronze_ingest(config=pipeline_config, snapshot="data_day1")
    manifest = run_silver_transform(config=pipeline_config, snapshot="data_day1")
    text = json.dumps(manifest)
    parsed = json.loads(text)
    assert parsed["snapshot"] == "data_day1"