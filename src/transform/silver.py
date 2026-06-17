"""Silver layer — star schema transforms with SCD Type 2."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.config_loader import load_config, resolve_path
from src.load.parquet_writer import read_parquet, write_parquet
from src.transform.cleaning import snapshot_as_of_date
from src.transform.dimensions import (
    build_dim_clubs,
    build_dim_competitions,
    build_dim_date,
    build_dim_player_valuations,
    build_dim_players,
    merge_reference_dimension,
    player_effective_dates,
)
from src.transform.facts import build_fact_appearances, build_fact_games, merge_facts

logger = logging.getLogger(__name__)

SILVER_TABLES = [
    "dim_date",
    "dim_competitions",
    "dim_clubs",
    "dim_players",
    "dim_player_valuations",
    "fact_games",
    "fact_appearances",
]


def bronze_snapshot_dir(config: dict[str, Any], snapshot: str) -> Path:
    bronze_root = resolve_path(config, "bronze")
    return bronze_root / snapshot


def silver_output_dir(config: dict[str, Any]) -> Path:
    return resolve_path(config, "silver")


def load_bronze_tables(bronze_dir: Path, tables: list[str]) -> dict[str, pd.DataFrame]:
    """Read all bronze parquet tables for a snapshot."""
    loaded: dict[str, pd.DataFrame] = {}
    for table in tables:
        path = bronze_dir / f"{table}.parquet"
        if not path.exists():
            raise FileNotFoundError(f"Bronze table not found: {path}")
        loaded[table] = read_parquet(path)
        logger.info("Loaded bronze %s — %d rows", table, len(loaded[table]))
    return loaded


def load_existing_silver(silver_dir: Path) -> dict[str, pd.DataFrame]:
    """Load cumulative silver tables when present."""
    existing: dict[str, pd.DataFrame] = {}
    if not silver_dir.exists():
        return existing
    for name in SILVER_TABLES:
        path = silver_dir / f"{name}.parquet"
        if path.exists():
            existing[name] = read_parquet(path)
    return existing


def run_silver_transform(
    config: dict[str, Any] | None = None,
    snapshot: str | None = None,
) -> dict[str, Any]:
    """Build or merge silver star-schema tables from a bronze snapshot."""
    config = config or load_config()
    snap_name = snapshot or config["runtime"]["data_snapshot"]
    bronze_dir = bronze_snapshot_dir(config, snap_name)
    silver_dir = silver_output_dir(config)

    source_tables = config["tables"]["source"]
    bronze = load_bronze_tables(bronze_dir, source_tables)
    existing = load_existing_silver(silver_dir)

    taxonomy = config["transforms"]["position_taxonomy"]
    season_start_month = config["transforms"]["season"]["start_month"]
    as_of = snapshot_as_of_date(bronze["games"])
    eff_dates = player_effective_dates(
        bronze["players"],
        bronze["player_valuations"],
        as_of,
    )

    dim_competitions = merge_reference_dimension(
        existing.get("dim_competitions"),
        build_dim_competitions(bronze["competitions"]),
        business_key="competition_id",
        sk_column="competition_sk",
    )
    dim_clubs = merge_reference_dimension(
        existing.get("dim_clubs"),
        build_dim_clubs(bronze["clubs"]),
        business_key="club_id",
        sk_column="club_sk",
    )
    dim_players = build_dim_players(
        bronze["players"],
        taxonomy,
        effective_date=as_of,
        existing=existing.get("dim_players"),
        effective_dates=eff_dates,
    )
    dim_player_valuations = build_dim_player_valuations(
        bronze["player_valuations"],
        existing=existing.get("dim_player_valuations"),
    )

    dim_date = build_dim_date(
        bronze["games"],
        bronze["appearances"],
        bronze["player_valuations"],
        date_columns=["date"],
        season_start_month=season_start_month,
        existing=existing.get("dim_date"),
    )

    fact_games = merge_facts(
        existing.get("fact_games"),
        build_fact_games(
            bronze["games"],
            dim_date,
            dim_competitions,
            dim_clubs,
            season_start_month=season_start_month,
        ),
        natural_key="game_id",
    )

    fact_appearances = merge_facts(
        existing.get("fact_appearances"),
        build_fact_appearances(
            bronze["appearances"],
            dim_players,
            dim_clubs,
            dim_competitions,
            dim_date,
            fact_games,
        ),
        natural_key="appearance_id",
    )

    outputs = {
        "dim_date": dim_date,
        "dim_competitions": dim_competitions,
        "dim_clubs": dim_clubs,
        "dim_players": dim_players,
        "dim_player_valuations": dim_player_valuations,
        "fact_games": fact_games,
        "fact_appearances": fact_appearances,
    }

    table_results = []
    for name, df in outputs.items():
        path = silver_dir / f"{name}.parquet"
        write_parquet(df, path)
        logger.info("Wrote silver %s — %d rows to %s", name, len(df), path)
        table_results.append({"table": name, "output": str(path), "row_count": len(df)})

    manifest = {
        "layer": "silver",
        "snapshot": snap_name,
        "transformed_at": datetime.now(timezone.utc).isoformat(),
        "as_of_date": as_of.isoformat(),
        "tables": table_results,
    }
    manifest_path = silver_dir / "_transform_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    logger.info("Silver transform complete — manifest=%s", manifest_path)

    return manifest