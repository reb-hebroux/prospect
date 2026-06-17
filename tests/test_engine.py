"""Tests for the pandas/spark engine abstraction."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from src.engine import ensure_engine_available, get_engine
from src.engine.base import DataEngine
from src.engine.pandas_engine import PandasEngine
from src.engine.spark_engine import SparkEngine
from src.load.parquet_writer import read_parquet, write_parquet


def test_get_engine_defaults_to_pandas():
    engine = get_engine({"runtime": {"engine": "pandas"}})
    assert isinstance(engine, PandasEngine)
    assert engine.name == "pandas"


def test_get_engine_unknown_raises():
    with pytest.raises(ValueError, match="Unknown engine"):
        get_engine({"runtime": {"engine": "dask"}})


def test_pandas_engine_round_trip_parquet(tmp_path: Path):
    engine = PandasEngine()
    source = pd.DataFrame({"id": [1, 2], "value": ["a", "b"]})
    path = tmp_path / "sample.parquet"

    engine.write_parquet(source, path)
    loaded = engine.read_parquet(path)

    pd.testing.assert_frame_equal(source, loaded)


def test_parquet_writer_uses_configured_engine(tmp_path: Path):
    df = pd.DataFrame({"metric": [10, 20]})
    path = tmp_path / "writer.parquet"
    config = {"runtime": {"engine": "pandas"}}

    write_parquet(df, path, config=config)
    loaded = read_parquet(path, config=config)
    pd.testing.assert_frame_equal(df, loaded)


def test_spark_engine_raises_with_guidance():
    engine = SparkEngine()
    with pytest.raises(NotImplementedError, match="ENGINE=spark is reserved"):
        engine.read_parquet(Path("data.parquet"))


def test_ensure_engine_available_rejects_spark():
    with pytest.raises(NotImplementedError, match="ENGINE=spark is reserved"):
        ensure_engine_available({"runtime": {"engine": "spark"}})


def test_data_engine_protocol():
    assert isinstance(PandasEngine(), DataEngine)