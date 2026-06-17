"""Gold layer — rebuild business aggregates from cumulative silver tables."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config_loader import load_config, resolve_path
from src.load.parquet_writer import read_parquet, write_parquet
from src.transform.aggregates import (
    build_gold_club_performance,
    build_gold_player_performance,
    build_gold_player_valuation_trend,
)

logger = logging.getLogger(__name__)

GOLD_TABLES = [
    "gold_player_performance",
    "gold_club_performance",
    "gold_player_valuation_trend",
]

SILVER_INPUTS = [
    "fact_appearances",
    "fact_games",
    "dim_date",
    "dim_players",
    "dim_clubs",
    "dim_player_valuations",
]


def silver_output_dir(config: dict[str, Any]) -> Path:
    return resolve_path(config, "silver")


def gold_output_dir(config: dict[str, Any]) -> Path:
    return resolve_path(config, "gold")


def load_silver_tables(silver_dir: Path) -> dict[str, Any]:
    """Load silver tables required for gold aggregation."""
    loaded: dict[str, Any] = {}
    for table in SILVER_INPUTS:
        path = silver_dir / f"{table}.parquet"
        if not path.exists():
            raise FileNotFoundError(
                f"Silver table not found: {path}. Run silver transform first."
            )
        loaded[table] = read_parquet(path)
        logger.info("Loaded silver %s — %d rows", table, len(loaded[table]))
    return loaded


def run_gold_aggregation(config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Rebuild all gold marts from cumulative silver."""
    config = config or load_config()
    silver_dir = silver_output_dir(config)
    gold_dir = gold_output_dir(config)
    silver = load_silver_tables(silver_dir)

    rolling_window = config.get("gold", {}).get("valuation_rolling_window", 3)

    outputs = {
        "gold_player_performance": build_gold_player_performance(
            silver["fact_appearances"],
            silver["dim_date"],
            silver["dim_players"],
        ),
        "gold_club_performance": build_gold_club_performance(
            silver["fact_games"],
            silver["dim_clubs"],
        ),
        "gold_player_valuation_trend": build_gold_player_valuation_trend(
            silver["dim_player_valuations"],
            rolling_window=rolling_window,
        ),
    }

    table_results = []
    for name, df in outputs.items():
        path = gold_dir / f"{name}.parquet"
        write_parquet(df, path)
        logger.info("Wrote gold %s — %d rows to %s", name, len(df), path)
        table_results.append({"table": name, "output": str(path), "row_count": len(df)})

    manifest = {
        "layer": "gold",
        "aggregated_at": datetime.now(timezone.utc).isoformat(),
        "source": str(silver_dir),
        "tables": table_results,
    }
    manifest_path = gold_dir / "_aggregate_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    logger.info("Gold aggregation complete — manifest=%s", manifest_path)

    return manifest


def main() -> int:
    import logging
    import os

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    try:
        run_gold_aggregation()
        return 0
    except Exception:
        logging.exception("Gold aggregation failed")
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())