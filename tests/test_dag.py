"""Lightweight DAG contract tests (structure only — no Airflow cluster)."""

from __future__ import annotations

from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DAG_PATH = PROJECT_ROOT / "dags" / "football_analytics_pipeline.py"


def test_dag_file_exists():
    assert DAG_PATH.is_file()


def test_dag_file_declares_expected_contract():
    source = DAG_PATH.read_text(encoding="utf-8")
    assert 'dag_id="football_analytics_pipeline"' in source
    assert 'schedule="@hourly"' in source
    assert "from airflow.sdk import dag, task" in source
    assert "run_bronze_step" in source
    assert "run_dq_step" in source
    assert "run_silver_step" in source
    assert "run_gold_step" in source
    assert "finalize_snapshot_state" in source


def test_dag_imports_when_airflow_available():
    airflow = pytest.importorskip("airflow")
    assert airflow.__version__.startswith("3.")

    import importlib.util

    spec = importlib.util.spec_from_file_location("football_analytics_pipeline", DAG_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert hasattr(module, "football_analytics_pipeline")