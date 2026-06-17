"""Engine factory — select pandas or spark via config / ENGINE env var."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from src.config_loader import load_config
from src.engine.base import DataEngine
from src.engine.pandas_engine import PandasEngine
from src.engine.spark_engine import SparkEngine

_ENGINE_REGISTRY: dict[str, type[PandasEngine] | type[SparkEngine]] = {
    "pandas": PandasEngine,
    "spark": SparkEngine,
}


def get_engine(config: dict[str, Any] | None = None) -> DataEngine:
    """Return the configured processing engine."""
    config = config or load_config()
    name = config["runtime"]["engine"]
    try:
        engine_cls = _ENGINE_REGISTRY[name]
    except KeyError as exc:
        supported = ", ".join(sorted(_ENGINE_REGISTRY))
        raise ValueError(f"Unknown engine '{name}'. Supported engines: {supported}") from exc
    return engine_cls()


def ensure_engine_available(config: dict[str, Any] | None = None) -> DataEngine:
    """Validate the configured engine before pipeline execution."""
    engine = get_engine(config)
    if engine.name == "spark":
        engine.read_parquet(Path("."))
    return engine