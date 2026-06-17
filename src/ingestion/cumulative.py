"""Merge per-snapshot bronze tables into a cumulative bronze store."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import pandas as pd

from src.config_loader import load_config, resolve_path
from src.load.incremental import (
    TABLE_NATURAL_KEYS,
    max_game_date,
    merge_stats,
    upsert_parquet,
)
from src.load.parquet_writer import read_parquet

logger = logging.getLogger(__name__)


def cumulative_bronze_dir(config: dict[str, Any]) -> Path:
    rel = config["incremental"].get("cumulative_bronze", "data/bronze/cumulative")
    return Path(config["paths"]["project_root"]) / rel


def merge_snapshot_to_cumulative(
    config: dict[str, Any],
    snapshot: str,
    *,
    watermark_date: str | None = None,
) -> dict[str, Any]:
    """Upsert a snapshot's bronze parquet tables into the cumulative bronze layer."""
    config = config or load_config()
    bronze_root = resolve_path(config, "bronze")
    snapshot_dir = bronze_root / snapshot
    cumulative_dir = cumulative_bronze_dir(config)
    cumulative_dir.mkdir(parents=True, exist_ok=True)

    tables = config["tables"]["source"]
    incremental_cfg = dict(config.get("incremental", {}))
    incremental_cfg["_watermark_date"] = watermark_date

    table_results: dict[str, Any] = {}
    games_df: pd.DataFrame | None = None

    for table in tables:
        source_path = snapshot_dir / f"{table}.parquet"
        if not source_path.exists():
            raise FileNotFoundError(f"Bronze snapshot table missing: {source_path}")

        incoming = read_parquet(source_path)
        if table == "games":
            games_df = incoming

        dest_path = cumulative_dir / f"{table}.parquet"
        existing = read_parquet(dest_path) if dest_path.exists() else None
        stats = merge_stats(table, existing, incoming, incremental_cfg)
        merged = upsert_parquet(dest_path, incoming, TABLE_NATURAL_KEYS[table])
        stats["cumulative_rows"] = len(merged)

        logger.info(
            "Cumulative bronze %s — incoming=%d new_keys=%d total=%d",
            table,
            stats["incoming_rows"],
            stats["new_keys"],
            stats["cumulative_rows"],
        )
        table_results[table] = stats

    game_date_max = None
    if games_df is not None:
        game_date_max = max_game_date(
            games_df,
            config["incremental"].get("game_date_column", "date"),
        )

    return {
        "snapshot": snapshot,
        "cumulative_dir": str(cumulative_dir),
        "game_date_max": game_date_max,
        "tables": table_results,
    }