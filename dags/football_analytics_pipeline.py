"""Airflow DAG — bronze → DQ → silver → gold wired to the incremental pipeline."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from src.pipeline.airflow_tasks import (
    finalize_snapshot_state,
    prepare_force_rebuild,
    resolve_snapshot_for_dag,
    run_bronze_step,
    run_dq_step,
    run_gold_step,
    run_silver_step,
)


@dag(
    dag_id="football_analytics_pipeline",
    schedule="@hourly",
    start_date=pendulum.datetime(2024, 8, 1, tz="UTC"),
    catchup=False,
    tags=["football", "medallion", "prospect"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    doc_md="""
    Hourly medallion pipeline orchestration.

    Tasks mirror the local runner: bronze ingest + cumulative merge → DQ → silver → gold,
    then checkpoint update. Snapshot selection follows `DATA_SNAPSHOT` when set, otherwise
    the next pending snapshot from `data/state/pipeline_state.json`.
    """,
)
def football_analytics_pipeline():
    @task
    def prepare_environment() -> None:
        prepare_force_rebuild()

    @task
    def resolve_snapshot() -> str:
        return resolve_snapshot_for_dag()

    @task
    def bronze(snapshot: str) -> dict:
        return run_bronze_step(snapshot)

    @task
    def dq(snapshot: str) -> dict:
        return run_dq_step(snapshot)

    @task
    def silver(snapshot: str) -> dict:
        return run_silver_step(snapshot)

    @task
    def gold() -> dict:
        return run_gold_step()

    @task
    def finalize(snapshot: str, bronze_result: dict) -> dict:
        return finalize_snapshot_state(snapshot, bronze_result)

    prep = prepare_environment()
    snapshot = resolve_snapshot()
    bronze_result = bronze(snapshot)
    dq_result = dq(snapshot)
    silver_result = silver(snapshot)
    gold_result = gold()
    finalize_result = finalize(snapshot, bronze_result)

    prep >> snapshot >> bronze_result >> dq_result >> silver_result >> gold_result >> finalize_result


football_analytics_pipeline()