"""SCD Type 2 helpers for silver dimensions."""

from __future__ import annotations

from typing import Any

import pandas as pd

OPEN_END_DATE = pd.Timestamp("9999-12-31")


def _next_surrogate_key(existing: pd.DataFrame | None, sk_column: str) -> int:
    if existing is None or existing.empty or sk_column not in existing.columns:
        return 1
    return int(existing[sk_column].max()) + 1


def apply_scd_type2(
    existing: pd.DataFrame | None,
    incoming: pd.DataFrame,
    *,
    business_key: str,
    compare_columns: list[str],
    effective_date: pd.Timestamp,
    sk_column: str,
    close_deleted: bool = False,
    deleted_flag_column: str | None = None,
) -> pd.DataFrame:
    """
    Merge incoming dimension rows using SCD Type 2 semantics.

    - Unchanged current rows are kept as-is.
    - Changed rows close the current version and insert a new current version.
    - Soft-deleted rows (deleted_flag_column == 1) close the current version.
    """
    effective_date = pd.Timestamp(effective_date).normalize()
    incoming = incoming.copy()

    if existing is None or existing.empty:
        result = incoming.copy()
        result[sk_column] = range(1, len(result) + 1)
        result["effective_date"] = effective_date
        result["end_date"] = OPEN_END_DATE
        result["is_current"] = True
        return result

    history = existing.copy()
    next_sk = _next_surrogate_key(history, sk_column)
    current = history[history["is_current"]].copy()
    current_by_key = current.set_index(business_key, drop=False)

    updated_rows: list[pd.DataFrame] = []
    keys_in_incoming = set(incoming[business_key].tolist())

    for _, row in incoming.iterrows():
        key = row[business_key]
        if key not in current_by_key.index:
            new_row = row.to_frame().T
            new_row[sk_column] = next_sk
            next_sk += 1
            new_row["effective_date"] = effective_date
            new_row["end_date"] = OPEN_END_DATE
            new_row["is_current"] = True
            updated_rows.append(new_row)
            continue

        current_row = current_by_key.loc[key]
        if isinstance(current_row, pd.DataFrame):
            current_row = current_row.iloc[-1]

        is_deleted = False
        if close_deleted and deleted_flag_column and deleted_flag_column in row.index:
            is_deleted = bool(row[deleted_flag_column] == 1)

        changed = is_deleted or any(
            _values_differ(current_row[col], row[col]) for col in compare_columns
        )

        if not changed:
            continue

        closed = current_row.to_frame().T.copy()
        closed["end_date"] = effective_date - pd.Timedelta(days=1)
        closed["is_current"] = False
        history = _replace_row(history, sk_column, int(closed[sk_column].iloc[0]), closed.iloc[0])

        if not is_deleted:
            new_row = row.to_frame().T
            new_row[sk_column] = next_sk
            next_sk += 1
            new_row["effective_date"] = effective_date
            new_row["end_date"] = OPEN_END_DATE
            new_row["is_current"] = True
            updated_rows.append(new_row)

    if updated_rows:
        history = pd.concat([history, *updated_rows], ignore_index=True)

    # Preserve historical rows for keys not present in the incoming snapshot.
    missing_keys = set(current_by_key.index) - keys_in_incoming
    if missing_keys:
        pass  # history already contains them

    return history.sort_values([business_key, "effective_date"]).reset_index(drop=True)


def build_valuation_scd(
    valuations: pd.DataFrame,
    *,
    sk_column: str = "valuation_sk",
) -> pd.DataFrame:
    """Build SCD Type 2 history from chronological valuation points."""
    if valuations.empty:
        return pd.DataFrame(
            columns=[
                sk_column,
                "player_id",
                "valuation_date",
                "market_value_in_eur",
                "current_club_id",
                "current_club_name",
                "player_club_domestic_competition_id",
                "effective_date",
                "end_date",
                "is_current",
            ]
        )

    df = valuations.copy()
    df["valuation_date"] = pd.to_datetime(df["date"]).dt.normalize()
    df = df.sort_values(["player_id", "valuation_date"]).reset_index(drop=True)

    records: list[dict] = []
    sk = 1
    for player_id, group in df.groupby("player_id", sort=False):
        ordered = group.sort_values("valuation_date").reset_index(drop=True)
        for idx, row in ordered.iterrows():
            effective = row["valuation_date"]
            if idx < len(ordered) - 1:
                end = ordered.iloc[idx + 1]["valuation_date"] - pd.Timedelta(days=1)
                is_current = False
            else:
                end = OPEN_END_DATE
                is_current = True

            records.append(
                {
                    sk_column: sk,
                    "player_id": player_id,
                    "valuation_date": effective,
                    "market_value_in_eur": row["market_value_in_eur"],
                    "current_club_id": row["current_club_id"],
                    "current_club_name": row["current_club_name"],
                    "player_club_domestic_competition_id": row[
                        "player_club_domestic_competition_id"
                    ],
                    "effective_date": effective,
                    "end_date": end,
                    "is_current": is_current,
                }
            )
            sk += 1

    return pd.DataFrame.from_records(records)


def merge_valuation_scd(
    existing: pd.DataFrame | None,
    incoming_valuations: pd.DataFrame,
    *,
    sk_column: str = "valuation_sk",
) -> pd.DataFrame:
    """Rebuild valuation SCD from the union of existing and incoming points."""
    frames = []
    if existing is not None and not existing.empty:
        frames.append(
            existing[
                [
                    "player_id",
                    "valuation_date",
                    "market_value_in_eur",
                    "current_club_id",
                    "current_club_name",
                    "player_club_domestic_competition_id",
                ]
            ].rename(columns={"valuation_date": "date"})
        )
    if not incoming_valuations.empty:
        frames.append(incoming_valuations)

    combined = pd.concat(frames, ignore_index=True) if frames else incoming_valuations
    if combined.empty:
        return build_valuation_scd(combined, sk_column=sk_column)

    combined["date"] = pd.to_datetime(combined["date"]).dt.normalize()
    combined = combined.drop_duplicates(
        subset=["player_id", "date", "market_value_in_eur", "current_club_id"],
        keep="last",
    )
    return build_valuation_scd(combined, sk_column=sk_column)


def lookup_surrogate_at_date(
    dimension: pd.DataFrame,
    business_key: str,
    sk_column: str,
    business_id: Any,
    as_of: pd.Timestamp,
) -> int | None:
    """Return dimension SK valid for business_id on as_of date."""
    if dimension.empty:
        return None

    as_of = pd.Timestamp(as_of).normalize()
    subset = dimension[dimension[business_key] == business_id]
    if subset.empty:
        return None

    valid = subset[
        (subset["effective_date"] <= as_of) & (subset["end_date"] >= as_of)
    ]
    if valid.empty:
        current = subset[subset["is_current"]]
        if current.empty:
            return None
        return int(current.iloc[-1][sk_column])
    return int(valid.iloc[-1][sk_column])


def _values_differ(left: Any, right: Any) -> bool:
    if pd.isna(left) and pd.isna(right):
        return False
    if pd.isna(left) or pd.isna(right):
        return True
    return left != right


def _replace_row(
    df: pd.DataFrame,
    sk_column: str,
    sk_value: int,
    new_row: pd.Series,
) -> pd.DataFrame:
    mask = df[sk_column] == sk_value
    updated = df.copy()
    for col in new_row.index:
        if col in updated.columns:
            updated.loc[mask, col] = new_row[col]
    return updated