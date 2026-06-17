"""Configurable data quality checks for bronze-layer tables."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

import pandas as pd


@dataclass
class CheckResult:
    check_type: str
    table: str
    passed: bool
    violation_count: int = 0
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def check_nulls(
    tables: dict[str, pd.DataFrame],
    rules: list[dict[str, Any]],
) -> list[CheckResult]:
    """Fail when required columns contain null values."""
    results: list[CheckResult] = []
    for rule in rules:
        table_name = rule["table"]
        df = tables[table_name]
        column_violations: dict[str, int] = {}
        total = 0
        for column in rule["columns"]:
            if column not in df.columns:
                column_violations[column] = len(df)
                total += len(df)
                continue
            count = int(df[column].isna().sum())
            if count:
                column_violations[column] = count
                total += count

        results.append(
            CheckResult(
                check_type="null_check",
                table=table_name,
                passed=total == 0,
                violation_count=total,
                details={"columns": column_violations},
            )
        )
    return results


def check_referential_integrity(
    tables: dict[str, pd.DataFrame],
    rules: list[dict[str, Any]],
) -> list[CheckResult]:
    """Fail when child keys are not present in the parent table."""
    results: list[CheckResult] = []
    for rule in rules:
        child = tables[rule["child_table"]]
        parent = tables[rule["parent_table"]]
        child_col = rule["child_column"]
        parent_col = rule["parent_column"]

        child_keys = child[child_col].dropna().unique()
        parent_keys = set(parent[parent_col].dropna().unique())
        orphans = [key for key in child_keys if key not in parent_keys]
        sample = orphans[:10]

        results.append(
            CheckResult(
                check_type="referential_integrity",
                table=rule["child_table"],
                passed=len(orphans) == 0,
                violation_count=len(orphans),
                details={
                    "child_column": child_col,
                    "parent_table": rule["parent_table"],
                    "parent_column": parent_col,
                    "orphan_keys_sample": sample,
                },
            )
        )
    return results


def check_valid_ranges(
    tables: dict[str, pd.DataFrame],
    rules: list[dict[str, Any]],
) -> list[CheckResult]:
    """Fail when numeric column values fall below configured minimums."""
    results: list[CheckResult] = []
    for rule in rules:
        table_name = rule["table"]
        column = rule["column"]
        minimum = rule["min"]
        df = tables[table_name]

        if column not in df.columns:
            results.append(
                CheckResult(
                    check_type="valid_range",
                    table=table_name,
                    passed=False,
                    violation_count=len(df),
                    details={"column": column, "min": minimum, "missing_column": True},
                )
            )
            continue

        numeric = pd.to_numeric(df[column], errors="coerce")
        violations = df[numeric < minimum]
        sample_idx = violations.head(5).index.tolist()

        results.append(
            CheckResult(
                check_type="valid_range",
                table=table_name,
                passed=violations.empty,
                violation_count=len(violations),
                details={
                    "column": column,
                    "min": minimum,
                    "violating_row_indices": sample_idx,
                },
            )
        )
    return results


def check_duplicates(
    tables: dict[str, pd.DataFrame],
    rules: list[dict[str, Any]],
) -> list[CheckResult]:
    """Bonus check — duplicate natural keys within a table."""
    results: list[CheckResult] = []
    for rule in rules:
        table_name = rule["table"]
        columns = rule["columns"]
        df = tables[table_name]
        dup_mask = df.duplicated(subset=columns, keep=False)
        duplicate_rows = df[dup_mask]

        results.append(
            CheckResult(
                check_type="duplicate_check",
                table=table_name,
                passed=duplicate_rows.empty,
                violation_count=int(dup_mask.sum()),
                details={
                    "columns": columns,
                    "duplicate_keys_sample": (
                        duplicate_rows[columns].drop_duplicates().head(5).to_dict("records")
                    ),
                },
            )
        )
    return results


def check_schema_drift(manifest_tables: list[dict[str, Any]]) -> CheckResult:
    """Bonus check — report schema drift captured during bronze ingest."""
    drifted = [
        entry["table"]
        for entry in manifest_tables
        if entry.get("schema_report", {}).get("extra_columns")
    ]
    details = {
        entry["table"]: entry["schema_report"]["extra_columns"]
        for entry in manifest_tables
        if entry.get("schema_report", {}).get("extra_columns")
    }
    return CheckResult(
        check_type="schema_drift",
        table="bronze",
        passed=len(drifted) == 0,
        violation_count=len(drifted),
        details={"tables_with_extra_columns": details},
    )