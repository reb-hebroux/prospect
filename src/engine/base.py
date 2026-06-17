"""Processing engine protocol — swap point for Pandas ↔ Spark."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class DataEngine(Protocol):
    """Minimal dataframe I/O contract used by load and transform layers."""

    name: str

    def read_parquet(self, path: Path) -> Any:
        """Read a parquet file into the engine's native dataframe type."""

    def write_parquet(self, df: Any, path: Path) -> Path:
        """Persist a dataframe as parquet, creating parent directories as needed."""

    def read_csv(self, path: Path, **kwargs: Any) -> Any:
        """Read a CSV file into the engine's native dataframe type."""