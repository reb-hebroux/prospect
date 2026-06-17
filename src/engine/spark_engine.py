"""Reserved Spark engine — documents the swap boundary without bundling PySpark."""

from __future__ import annotations

from pathlib import Path
from typing import Any


class SparkEngine:
    name = "spark"

    _MESSAGE = (
        "ENGINE=spark is reserved for future scale-out runs. "
        "This assignment ships with the pandas engine only. "
        "To enable Spark, add pyspark to requirements and implement "
        "read/write in src/engine/spark_engine.py."
    )

    def _unsupported(self) -> None:
        raise NotImplementedError(self._MESSAGE)

    def read_parquet(self, path: Path) -> Any:
        self._unsupported()

    def write_parquet(self, df: Any, path: Path) -> Path:
        self._unsupported()

    def read_csv(self, path: Path, **kwargs: Any) -> Any:
        self._unsupported()