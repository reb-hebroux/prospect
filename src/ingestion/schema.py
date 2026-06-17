"""Bronze schema validation and minimal type casting."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

# Columns allowed to be absent in source CSV (added with defaults on ingest)
OPTIONAL_COLUMNS: dict[str, list[str]] = {
    "players": ["is_deleted"],
}

DEFAULTS: dict[str, dict[str, object]] = {
    "players": {"is_deleted": 0},
}

# Minimal type map for bronze — light casting only
DTYPE_HINTS: dict[str, dict[str, str]] = {
    "players": {
        "player_id": "Int64",
        "last_season": "Int64",
        "current_club_id": "Int64",
        "height_in_cm": "Int64",
        "international_caps": "Int64",
        "international_goals": "Int64",
        "current_national_team_id": "Int64",
        "market_value_in_eur": "Int64",
        "highest_market_value_in_eur": "Int64",
        "is_deleted": "Int64",
    },
    "games": {
        "game_id": "Int64",
        "season": "Int64",
        "home_club_id": "Int64",
        "away_club_id": "Int64",
        "home_club_goals": "Int64",
        "away_club_goals": "Int64",
        "home_club_position": "Int64",
        "away_club_position": "Int64",
        "attendance": "Int64",
    },
    "appearances": {
        "appearance_id": "Int64",
        "game_id": "Int64",
        "player_id": "Int64",
        "player_club_id": "Int64",
        "player_current_club_id": "Int64",
        "yellow_cards": "Int64",
        "red_cards": "Int64",
        "goals": "Int64",
        "assists": "Int64",
        "minutes_played": "Int64",
    },
    "clubs": {
        "club_id": "Int64",
        "total_market_value": "Int64",
        "squad_size": "Int64",
        "foreigners_number": "Int64",
        "national_team_players": "Int64",
        "stadium_seats": "Int64",
        "last_season": "Int64",
    },
    "competitions": {
        "country_id": "Int64",
        "total_clubs": "Int64",
    },
    "player_valuations": {
        "player_id": "Int64",
        "market_value_in_eur": "Int64",
        "current_club_id": "Int64",
    },
}

DATETIME_COLUMNS: dict[str, list[str]] = {
    "players": ["date_of_birth", "contract_expiration_date"],
    "games": ["date"],
    "appearances": ["date"],
    "player_valuations": ["date"],
}


class SchemaValidationError(ValueError):
    """Raised when source data does not meet bronze schema requirements."""


@dataclass
class SchemaReport:
    table: str
    missing_columns: list[str] = field(default_factory=list)
    extra_columns: list[str] = field(default_factory=list)
    optional_added: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.missing_columns


def validate_and_prepare(
    df: pd.DataFrame,
    table: str,
    expected_columns: list[str],
) -> tuple[pd.DataFrame, SchemaReport]:
    """Validate columns, handle optional fields, drop extras, apply bronze types."""
    optional = set(OPTIONAL_COLUMNS.get(table, []))
    required = [c for c in expected_columns if c not in optional]

    report = SchemaReport(table=table)
    actual = list(df.columns)

    report.missing_columns = [c for c in required if c not in actual]
    if report.missing_columns:
        raise SchemaValidationError(
            f"{table}: missing required columns {report.missing_columns}"
        )

    report.extra_columns = [c for c in actual if c not in expected_columns]
    for col in optional:
        if col in expected_columns and col not in actual:
            df[col] = DEFAULTS.get(table, {}).get(col)
            report.optional_added.append(col)

    still_missing = [c for c in expected_columns if c not in df.columns]
    if still_missing:
        raise SchemaValidationError(
            f"{table}: columns missing after optional fill: {still_missing}"
        )
    df = df[expected_columns].copy()

    for col in DATETIME_COLUMNS.get(table, []):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    for col, dtype in DTYPE_HINTS.get(table, {}).items():
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype(dtype)

    for col in df.columns:
        if pd.api.types.is_object_dtype(df[col]) or pd.api.types.is_string_dtype(df[col]):
            df[col] = df[col].astype("string")

    return df, report