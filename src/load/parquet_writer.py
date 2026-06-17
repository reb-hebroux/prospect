"""Parquet writers for medallion layers."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from src.engine import get_engine


def write_parquet(
    df: pd.DataFrame,
    path: Path,
    *,
    config: dict[str, Any] | None = None,
) -> Path:
    """Write DataFrame to parquet via the configured processing engine."""
    return get_engine(config).write_parquet(df, path)


def read_parquet(path: Path, *, config: dict[str, Any] | None = None) -> pd.DataFrame:
    """Read parquet via the configured processing engine."""
    return get_engine(config).read_parquet(path)