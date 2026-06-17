"""Incremental pipeline runner with checkpoint state and idempotent snapshot processing."""

from __future__ import annotations

import logging
import os
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config_loader import load_config, resolve_path, snapshot_path
from src.engine import ensure_engine_available
from src.dq.runner import run_dq_checks
from src.ingestion.bronze import run_bronze_ingest
from src.ingestion.cumulative import merge_snapshot_to_cumulative
from src.load.incremental import compute_source_hash
from src.load.state import (
    load_state,
    mark_snapshot_processed,
    save_state,
    should_process_snapshot,
)
from src.transform.gold import run_gold_aggregation
from src.transform.silver import run_silver_transform

logger = logging.getLogger(__name__)


def _clear_layer_parquet(layer_dir: Path) -> None:
    if not layer_dir.exists():
        return
    for path in layer_dir.glob("*.parquet"):
        path.unlink()


def reset_derived_layers(config: dict[str, Any]) -> None:
    """Remove cumulative bronze and silver outputs before a forced full rebuild."""
    from src.ingestion.cumulative import cumulative_bronze_dir

    _clear_layer_parquet(cumulative_bronze_dir(config))
    _clear_layer_parquet(resolve_path(config, "silver"))


def snapshot_order(config: dict[str, Any]) -> list[str]:
    order = config.get("incremental", {}).get("snapshot_order")
    if order:
        return list(order)
    return ["data_day1", "data_day2"]


def pipeline_force_enabled() -> bool:
    return os.getenv("PIPELINE_FORCE", "false").lower() in ("1", "true", "yes")


def run_snapshot_pipeline(
    config: dict[str, Any],
    snapshot: str,
    *,
    update_state: bool = True,
    state: Any | None = None,
) -> dict[str, Any]:
    """Run ingest → cumulative merge → DQ → silver → gold for one snapshot."""
    runtime_config = deepcopy(config)
    runtime_config["runtime"]["data_snapshot"] = snapshot

    bronze_manifest = run_bronze_ingest(config=runtime_config, snapshot=snapshot)
    watermark = None
    if state is not None:
        watermark = state.watermarks.get("games_date")

    cumulative_result = merge_snapshot_to_cumulative(
        runtime_config,
        snapshot,
        watermark_date=watermark,
    )
    dq_report = run_dq_checks(config=runtime_config, snapshot=snapshot)
    silver_manifest = run_silver_transform(config=runtime_config, snapshot=snapshot)
    gold_manifest = run_gold_aggregation(config=runtime_config)

    source_dir = snapshot_path(runtime_config, snapshot)
    content_hash = compute_source_hash(source_dir, runtime_config["tables"]["source"])

    result = {
        "snapshot": snapshot,
        "processed": True,
        "content_hash": content_hash,
        "bronze": bronze_manifest,
        "cumulative": cumulative_result,
        "dq": dq_report,
        "silver": silver_manifest,
        "gold": gold_manifest,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }

    if update_state:
        current_state = state if state is not None else load_state(config)
        mark_snapshot_processed(
            current_state,
            snapshot,
            content_hash=content_hash,
            game_date_max=cumulative_result.get("game_date_max"),
            merge_stats=cumulative_result.get("tables", {}),
        )
        result["state_path"] = str(save_state(config, current_state))

    return result


def run_incremental_pipeline(
    config: dict[str, Any] | None = None,
    *,
    force: bool | None = None,
) -> dict[str, Any]:
    """
    Process pending snapshots in order, skipping already-seen content unless forced.

    Each processed snapshot runs the full ingest → DQ → silver path and ends with a
    gold rebuild. Re-running with unchanged snapshot CSVs is a no-op at the snapshot
    level; re-processing the same snapshot still remains idempotent via upserts.
    """
    config = config or load_config()
    ensure_engine_available(config)
    force_run = pipeline_force_enabled() if force is None else force
    tables = config["tables"]["source"]

    if force_run:
        logger.info("Force rebuild enabled — resetting checkpoint and derived layers")
        reset_pipeline_state(config)
        reset_derived_layers(config)

    state = load_state(config)
    results: list[dict[str, Any]] = []
    skipped: list[str] = []

    for snapshot in snapshot_order(config):
        source_dir = snapshot_path(config, snapshot)
        if not source_dir.exists():
            logger.warning("Skipping %s — source directory not found: %s", snapshot, source_dir)
            skipped.append(snapshot)
            continue

        content_hash = compute_source_hash(source_dir, tables)
        if not should_process_snapshot(state, snapshot, content_hash, force=force_run):
            logger.info(
                "Skipping %s — already processed with identical content (hash=%s)",
                snapshot,
                content_hash[:12],
            )
            skipped.append(snapshot)
            continue

        logger.info("Processing snapshot %s", snapshot)
        result = run_snapshot_pipeline(
            config,
            snapshot,
            update_state=True,
            state=state,
        )
        state = load_state(config)
        results.append(result)

    summary = {
        "layer": "pipeline",
        "mode": "incremental",
        "forced": force_run,
        "processed_snapshots": [r["snapshot"] for r in results],
        "skipped_snapshots": skipped,
        "watermarks": state.watermarks,
        "runs": results,
        "completed_at": datetime.now(timezone.utc).isoformat(),
    }
    logger.info(
        "Incremental pipeline complete — processed=%s skipped=%s",
        summary["processed_snapshots"],
        summary["skipped_snapshots"],
    )
    return summary


def reset_pipeline_state(config: dict[str, Any] | None = None) -> Path | None:
    """Delete checkpoint state so the next incremental run starts fresh."""
    from src.load.state import state_path

    config = config or load_config()
    path = state_path(config)
    if path.exists():
        path.unlink()
        logger.info("Pipeline state reset — removed %s", path)
        return path
    logger.info("Pipeline state reset — no state file at %s", path)
    return None


def main() -> int:
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    try:
        if os.getenv("PIPELINE_RESET", "false").lower() in ("1", "true", "yes"):
            reset_pipeline_state()
            return 0
        run_incremental_pipeline()
        return 0
    except Exception:
        logging.exception("Incremental pipeline failed")
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())