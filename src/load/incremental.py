"""Incremental merge helpers — upserts, watermarks, source hashing."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

import pandas as pd

from src.load.parquet_writer import read_parquet, write_parquet

TABLE_NATURAL_KEYS: dict[str, list[str]] = {
    "players": ["player_id"],
    "games": ["game_id"],
    "appearances": ["appearance_id"],
    "clubs": ["club_id"],
    "competitions": ["competition_id"],
    "player_valuations": ["player_id", "date"],
}


def upsert_dataframe(
    existing: pd.DataFrame | None,
    incoming: pd.DataFrame,
    key_columns: list[str],
) -> pd.DataFrame:
    """Merge incoming rows into existing data, keeping the latest version per key."""
    if existing is None or existing.empty:
        return incoming.reset_index(drop=True)
    if incoming.empty:
        return existing.reset_index(drop=True)

    combined = pd.concat([existing, incoming], ignore_index=True)
    return combined.drop_duplicates(subset=key_columns, keep="last").reset_index(drop=True)


def upsert_parquet(path: Path, incoming: pd.DataFrame, key_columns: list[str]) -> pd.DataFrame:
    """Upsert incoming rows into a parquet file."""
    existing = read_parquet(path) if path.exists() else None
    merged = upsert_dataframe(existing, incoming, key_columns)
    write_parquet(merged, path)
    return merged


def compute_source_hash(source_dir: Path, tables: list[str]) -> str:
    """Fingerprint a snapshot directory from CSV file contents."""
    digest = hashlib.sha256()
    for table in sorted(tables):
        csv_path = source_dir / f"{table}.csv"
        if csv_path.exists():
            digest.update(csv_path.name.encode())
            digest.update(csv_path.read_bytes())
    return digest.hexdigest()


def max_game_date(games: pd.DataFrame, date_column: str) -> str | None:
    """Return ISO date string for the newest game in a dataframe."""
    if games.empty or date_column not in games.columns:
        return None
    return pd.to_datetime(games[date_column]).max().normalize().date().isoformat()


def count_rows_after_watermark(
    df: pd.DataFrame,
    date_column: str,
    watermark: str | None,
) -> int:
    """Count rows strictly newer than a watermark date."""
    if watermark is None or df.empty or date_column not in df.columns:
        return len(df)
    dates = pd.to_datetime(df[date_column]).dt.normalize()
    return int((dates > pd.Timestamp(watermark)).sum())


def count_new_natural_keys(
    existing: pd.DataFrame | None,
    incoming: pd.DataFrame,
    key_columns: list[str],
) -> int:
    """Count incoming rows whose natural key is not already present."""
    if existing is None or existing.empty:
        return len(incoming)
    if incoming.empty:
        return 0

    existing_keys = set(
        tuple(row) for row in existing[key_columns].itertuples(index=False, name=None)
    )
    new_count = 0
    for row in incoming[key_columns].itertuples(index=False, name=None):
        if tuple(row) not in existing_keys:
            new_count += 1
    return new_count


def merge_stats(
    table: str,
    existing: pd.DataFrame | None,
    incoming: pd.DataFrame,
    incremental_config: dict[str, Any],
) -> dict[str, Any]:
    """Summarise how many rows are new for a table merge."""
    keys = TABLE_NATURAL_KEYS[table]
    stats: dict[str, Any] = {
        "incoming_rows": len(incoming),
        "new_keys": count_new_natural_keys(existing, incoming, keys),
    }
    date_column = incremental_config.get("game_date_column", "date")
    watermark = incremental_config.get("_watermark_date")
    if table == "games":
        stats["rows_after_watermark"] = count_rows_after_watermark(
            incoming, date_column, watermark
        )
    if table == "appearances" and watermark and "date" in incoming.columns:
        stats["rows_after_watermark"] = count_rows_after_watermark(
            incoming, "date", watermark
        )
    return stats