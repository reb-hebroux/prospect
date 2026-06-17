"""Airflow-callable wrappers around the incremental medallion pipeline."""

from __future__ import annotations

import os
from copy import deepcopy
from typing import Any

from src.config_loader import load_config, snapshot_path
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
from src.pipeline.runner import (
    pipeline_force_enabled,
    reset_derived_layers,
    reset_pipeline_state,
    snapshot_order,
)
from src.transform.gold import run_gold_aggregation
from src.transform.silver import run_silver_transform


def _runtime_config(config: dict[str, Any], snapshot: str) -> dict[str, Any]:
    runtime_config = deepcopy(config)
    runtime_config["runtime"]["data_snapshot"] = snapshot
    return runtime_config


def get_next_snapshot_name(config: dict[str, Any] | None = None) -> str | None:
    """Return the next snapshot that should run, or None if all are up to date."""
    config = config or load_config()
    force_run = pipeline_force_enabled()
    state = load_state(config)
    tables = config["tables"]["source"]

    for snapshot in snapshot_order(config):
        source_dir = snapshot_path(config, snapshot)
        if not source_dir.exists():
            continue
        content_hash = compute_source_hash(source_dir, tables)
        if should_process_snapshot(state, snapshot, content_hash, force=force_run):
            return snapshot
    return None


def run_bronze_step(snapshot: str, config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Bronze ingest plus cumulative merge for one snapshot."""
    config = config or load_config()
    runtime_config = _runtime_config(config, snapshot)
    state = load_state(config)

    bronze_manifest = run_bronze_ingest(config=runtime_config, snapshot=snapshot)
    cumulative_result = merge_snapshot_to_cumulative(
        runtime_config,
        snapshot,
        watermark_date=state.watermarks.get("games_date"),
    )
    return {
        "snapshot": snapshot,
        "bronze": bronze_manifest,
        "cumulative": cumulative_result,
    }


def run_dq_step(snapshot: str, config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Run bronze DQ checks for one snapshot."""
    config = config or load_config()
    runtime_config = _runtime_config(config, snapshot)
    dq_report = run_dq_checks(config=runtime_config, snapshot=snapshot)
    return {"snapshot": snapshot, "dq": dq_report}


def run_silver_step(snapshot: str, config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build or merge the silver star schema for one snapshot."""
    config = config or load_config()
    runtime_config = _runtime_config(config, snapshot)
    silver_manifest = run_silver_transform(config=runtime_config, snapshot=snapshot)
    return {"snapshot": snapshot, "silver": silver_manifest}


def run_gold_step(config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Rebuild gold marts from cumulative silver (idempotent full refresh)."""
    config = config or load_config()
    gold_manifest = run_gold_aggregation(config=config)
    return {"gold": gold_manifest}


def finalize_snapshot_state(
    snapshot: str,
    bronze_result: dict[str, Any],
    config: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Persist checkpoint state after bronze → DQ → silver → gold completes."""
    config = config or load_config()
    source_dir = snapshot_path(config, snapshot)
    content_hash = compute_source_hash(source_dir, config["tables"]["source"])
    cumulative = bronze_result.get("cumulative", {})

    state = load_state(config)
    mark_snapshot_processed(
        state,
        snapshot,
        content_hash=content_hash,
        game_date_max=cumulative.get("game_date_max"),
        merge_stats=cumulative.get("tables", {}),
    )
    state_path = save_state(config, state)
    return {
        "snapshot": snapshot,
        "content_hash": content_hash,
        "state_path": str(state_path),
    }


def execute_incremental_pipeline(config: dict[str, Any] | None = None) -> dict[str, Any]:
    """Run the full stateful incremental runner (used by CLI and optional DAG task)."""
    from src.pipeline.runner import run_incremental_pipeline

    return run_incremental_pipeline(config=config)


def prepare_force_rebuild(config: dict[str, Any] | None = None) -> None:
    """Reset checkpoint and derived layers when PIPELINE_FORCE is enabled."""
    if not pipeline_force_enabled():
        return
    config = config or load_config()
    reset_pipeline_state(config)
    reset_derived_layers(config)


def resolve_snapshot_for_dag(config: dict[str, Any] | None = None) -> str:
    """
    Pick the snapshot for a DAG run.

    Uses DATA_SNAPSHOT when set explicitly; otherwise picks the next pending snapshot
    from the incremental checkpoint order.
    """
    config = config or load_config()
    explicit = os.getenv("DATA_SNAPSHOT")
    if explicit:
        return explicit

    pending = get_next_snapshot_name(config)
    if pending is None:
        raise RuntimeError("No pending snapshots — pipeline checkpoint is current")
    return pending