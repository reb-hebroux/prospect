"""Bronze layer — raw CSV ingest with schema enforcement."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.config_loader import load_bronze_schema, load_config, resolve_path, snapshot_path
from src.ingestion.schema import SchemaReport, validate_and_prepare
from src.load.parquet_writer import write_parquet

logger = logging.getLogger(__name__)


def ingest_table(
    table: str,
    source_dir: Path,
    output_dir: Path,
    schema_columns: list[str],
) -> dict[str, Any]:
    """Ingest one CSV table into bronze parquet."""
    csv_path = source_dir / f"{table}.csv"
    if not csv_path.exists():
        raise FileNotFoundError(f"Source file not found: {csv_path}")

    logger.info("Reading %s", csv_path)
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=True)

    df, report = validate_and_prepare(df, table, schema_columns)
    if report.extra_columns:
        logger.warning(
            "%s: dropped extra columns (schema drift): %s",
            table,
            report.extra_columns,
        )
    if report.optional_added:
        logger.info("%s: added optional columns: %s", table, report.optional_added)

    out_path = output_dir / f"{table}.parquet"
    write_parquet(df, out_path)
    logger.info("Wrote %s rows to %s", len(df), out_path)

    return {
        "table": table,
        "source": str(csv_path),
        "output": str(out_path),
        "row_count": len(df),
        "columns": list(df.columns),
        "schema_report": {
            "extra_columns": report.extra_columns,
            "optional_added": report.optional_added,
        },
    }


def run_bronze_ingest(
    config: dict[str, Any] | None = None,
    snapshot: str | None = None,
) -> dict[str, Any]:
    """Run bronze ingest for all configured source tables."""
    config = config or load_config()
    snap_name = snapshot or config["runtime"]["data_snapshot"]
    source_dir = snapshot_path(config, snap_name)
    bronze_root = resolve_path(config, "bronze")
    output_dir = bronze_root / snap_name

    schema = load_bronze_schema()
    tables = config["tables"]["source"]

    logger.info(
        "Bronze ingest started — snapshot=%s source=%s output=%s",
        snap_name,
        source_dir,
        output_dir,
    )

    results = []
    for table in tables:
        if table not in schema:
            raise KeyError(f"No bronze schema defined for table: {table}")
        results.append(
            ingest_table(table, source_dir, output_dir, schema[table])
        )

    manifest = {
        "layer": "bronze",
        "snapshot": snap_name,
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "tables": results,
    }
    manifest_path = output_dir / "_ingest_manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    logger.info("Bronze ingest complete — %d tables, manifest=%s", len(results), manifest_path)

    return manifest