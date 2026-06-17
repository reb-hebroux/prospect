"""Load and validate pipeline configuration from YAML and environment."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"


def get_project_root() -> Path:
    return PROJECT_ROOT


def load_config(config_path: Path | str | None = None) -> dict[str, Any]:
    """Load config.yaml and apply environment overrides."""
    path = Path(config_path) if config_path else Path(
        os.getenv("CONFIG_PATH", DEFAULT_CONFIG_PATH)
    )
    if not path.is_absolute():
        path = PROJECT_ROOT / path

    with path.open(encoding="utf-8") as f:
        config: dict[str, Any] = yaml.safe_load(f)

    config["paths"]["project_root"] = str(PROJECT_ROOT)
    config["runtime"] = {
        "engine": os.getenv("ENGINE", config["engine"]["default"]),
        "environment": os.getenv("ENVIRONMENT", "local"),
        "log_level": os.getenv("LOG_LEVEL", "INFO"),
        "data_snapshot": os.getenv("DATA_SNAPSHOT", "data_day1"),
    }
    return config


def resolve_path(config: dict[str, Any], key: str) -> Path:
    """Resolve a paths.* entry relative to project root."""
    rel = config["paths"][key]
    return PROJECT_ROOT / rel


def snapshot_path(config: dict[str, Any], snapshot: str | None = None) -> Path:
    """Return input directory for data_day1 or data_day2."""
    snap = snapshot or config["runtime"]["data_snapshot"]
    if snap in ("data_day1", "day1"):
        rel = config["paths"]["snapshots"]["day1"]
    elif snap in ("data_day2", "day2"):
        rel = config["paths"]["snapshots"]["day2"]
    else:
        rel = config["paths"]["snapshots"].get(snap, snap)
    return PROJECT_ROOT / rel


def load_bronze_schema() -> dict[str, list[str]]:
    schema_path = PROJECT_ROOT / "config" / "schemas" / "bronze.yaml"
    with schema_path.open(encoding="utf-8") as f:
        return yaml.safe_load(f)