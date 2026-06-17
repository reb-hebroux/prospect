"""DQ runner — validate bronze tables and write per-run JSON reports."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.config_loader import load_config, resolve_path
from src.dq.checks import (
    CheckResult,
    check_duplicates,
    check_nulls,
    check_referential_integrity,
    check_schema_drift,
    check_valid_ranges,
)
from src.dq.exceptions import DataQualityError
from src.load.parquet_writer import read_parquet

logger = logging.getLogger(__name__)


def bronze_snapshot_dir(config: dict[str, Any], snapshot: str) -> Path:
    return resolve_path(config, "bronze") / snapshot


def dq_reports_dir(config: dict[str, Any]) -> Path:
    return resolve_path(config, "dq_reports")


def load_bronze_tables(bronze_dir: Path, tables: list[str]) -> dict[str, Any]:
    loaded: dict[str, Any] = {}
    for table in tables:
        path = bronze_dir / f"{table}.parquet"
        if not path.exists():
            raise FileNotFoundError(
                f"Bronze table not found: {path}. Run bronze ingest first."
            )
        loaded[table] = read_parquet(path)
        logger.info("DQ loaded bronze %s — %d rows", table, len(loaded[table]))
    return loaded


def load_bronze_manifest(bronze_dir: Path) -> dict[str, Any] | None:
    manifest_path = bronze_dir / "_ingest_manifest.json"
    if not manifest_path.exists():
        return None
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def run_checks(
    tables: dict[str, Any],
    dq_config: dict[str, Any],
    manifest: dict[str, Any] | None = None,
) -> list[CheckResult]:
    """Execute required and bonus DQ checks from config."""
    required = dq_config.get("required", {})
    bonus = dq_config.get("bonus", {})
    results: list[CheckResult] = []

    if required.get("null_checks"):
        results.extend(check_nulls(tables, required["null_checks"]))
    if required.get("referential_integrity"):
        results.extend(check_referential_integrity(tables, required["referential_integrity"]))
    if required.get("valid_ranges"):
        results.extend(check_valid_ranges(tables, required["valid_ranges"]))

    if bonus.get("duplicate_checks"):
        results.extend(check_duplicates(tables, bonus["duplicate_checks"]))

    if bonus.get("schema_drift", {}).get("enabled") and manifest:
        results.append(check_schema_drift(manifest.get("tables", [])))

    return results


def build_report(
    snapshot: str,
    results: list[CheckResult],
    bronze_dir: Path,
) -> dict[str, Any]:
    failed = [r for r in results if not r.passed]
    total_violations = sum(r.violation_count for r in results)
    return {
        "layer": "dq",
        "snapshot": snapshot,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "source": str(bronze_dir),
        "passed": len(failed) == 0,
        "summary": {
            "total_checks": len(results),
            "passed_checks": len(results) - len(failed),
            "failed_checks": len(failed),
            "total_violations": total_violations,
        },
        "checks": [r.to_dict() for r in results],
    }


def write_report(report: dict[str, Any], reports_dir: Path, snapshot: str) -> Path:
    reports_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    stamped_path = reports_dir / f"dq_report_{snapshot}_{timestamp}.json"
    latest_path = reports_dir / f"dq_report_{snapshot}_latest.json"

    payload = json.dumps(report, indent=2)
    stamped_path.write_text(payload, encoding="utf-8")
    latest_path.write_text(payload, encoding="utf-8")
    logger.info("DQ report written — %s", stamped_path)
    return stamped_path


def should_fail_fast() -> bool:
    return os.getenv("DQ_FAIL_ON_ERROR", "true").lower() in ("1", "true", "yes")


def run_dq_checks(
    config: dict[str, Any] | None = None,
    snapshot: str | None = None,
    *,
    fail_on_error: bool | None = None,
) -> dict[str, Any]:
    """Run DQ checks against a bronze snapshot and write a JSON report."""
    config = config or load_config()
    snap_name = snapshot or config["runtime"]["data_snapshot"]
    bronze_dir = bronze_snapshot_dir(config, snap_name)
    reports_dir = dq_reports_dir(config)

    tables = load_bronze_tables(bronze_dir, config["tables"]["source"])
    manifest = load_bronze_manifest(bronze_dir)
    results = run_checks(tables, config.get("data_quality", {}), manifest)
    report = build_report(snap_name, results, bronze_dir)
    report_path = write_report(report, reports_dir, snap_name)

    if not report["passed"]:
        logger.error(
            "DQ checks failed — %d failing checks, report=%s",
            report["summary"]["failed_checks"],
            report_path,
        )
        if fail_on_error if fail_on_error is not None else should_fail_fast():
            raise DataQualityError(
                f"DQ checks failed for snapshot {snap_name}",
                report_path=str(report_path),
            )
    else:
        logger.info(
            "DQ checks passed — %d checks, report=%s",
            report["summary"]["total_checks"],
            report_path,
        )

    report["report_path"] = str(report_path)
    return report


def main() -> int:
    import logging

    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, log_level, logging.INFO),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    try:
        run_dq_checks()
        return 0
    except DataQualityError:
        logging.exception("DQ checks failed")
        return 1
    except Exception:
        logging.exception("DQ runner failed")
        return 1


if __name__ == "__main__":
    import sys

    sys.exit(main())