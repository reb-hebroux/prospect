"""Default in-memory processing engine backed by pandas + pyarrow."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd


class PandasEngine:
    name = "pandas"

    def read_parquet(self, path: Path) -> pd.DataFrame:
        return pd.read_parquet(path, engine="pyarrow")

    def write_parquet(self, df: pd.DataFrame, path: Path) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False, engine="pyarrow")
        return path

    def read_csv(self, path: Path, **kwargs: Any) -> pd.DataFrame:
        return pd.read_csv(path, **kwargs)