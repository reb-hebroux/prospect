"""Silver dimension table builders."""

from __future__ import annotations

from typing import Any

import pandas as pd

from src.transform.cleaning import map_position_category, normalize_country_iso
from src.transform.scd import (
    apply_scd_type2,
    build_valuation_scd,
    merge_valuation_scd,
)


def build_dim_date(
    *frames: pd.DataFrame,
    date_columns: list[str],
    season_start_month: int = 8,
    existing: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Conformed date dimension from all supplied date columns."""
    from src.transform.cleaning import derive_season

    dates: set[pd.Timestamp] = set()
    if existing is not None and not existing.empty and "full_date" in existing.columns:
        for value in pd.to_datetime(existing["full_date"], errors="coerce").dropna():
            dates.add(pd.Timestamp(value).normalize())

    for frame in frames:
        if frame is None or frame.empty:
            continue
        for col in date_columns:
            if col not in frame.columns:
                continue
            for value in pd.to_datetime(frame[col], errors="coerce").dropna():
                dates.add(pd.Timestamp(value).normalize())

    if not dates:
        return pd.DataFrame(
            columns=[
                "date_sk",
                "full_date",
                "year",
                "month",
                "day",
                "quarter",
                "day_of_week",
                "season",
            ]
        )

    rows = []
    for full_date in sorted(dates):
        rows.append(
            {
                "date_sk": int(full_date.strftime("%Y%m%d")),
                "full_date": full_date,
                "year": full_date.year,
                "month": full_date.month,
                "day": full_date.day,
                "quarter": (full_date.month - 1) // 3 + 1,
                "day_of_week": full_date.day_name(),
                "season": derive_season(full_date, season_start_month),
            }
        )
    return pd.DataFrame(rows)


def build_dim_competitions(competitions: pd.DataFrame) -> pd.DataFrame:
    """Competition dimension keyed by competition_id."""
    if competitions.empty:
        return pd.DataFrame(columns=["competition_sk", "competition_id"])

    df = competitions.copy()
    df["competition_sk"] = range(1, len(df) + 1)
    cols = ["competition_sk"] + [
        c for c in df.columns if c != "competition_sk"
    ]
    return df[cols].drop_duplicates(subset=["competition_id"]).reset_index(drop=True)


def build_dim_clubs(clubs: pd.DataFrame) -> pd.DataFrame:
    """Club dimension keyed by club_id."""
    if clubs.empty:
        return pd.DataFrame(columns=["club_sk", "club_id"])

    df = clubs.copy()
    df["club_sk"] = range(1, len(df) + 1)
    cols = ["club_sk"] + [c for c in df.columns if c != "club_sk"]
    return df[cols].drop_duplicates(subset=["club_id"]).reset_index(drop=True)


def prepare_players_for_scd(
    players: pd.DataFrame,
    taxonomy: dict[str, list[str]],
) -> pd.DataFrame:
    """Enrich bronze players with taxonomy and ISO country codes."""
    df = players.copy()
    df = df[df["is_deleted"].fillna(0) == 0].copy()

    df["position_category"] = df.apply(
        lambda row: map_position_category(row["position"], row["sub_position"], taxonomy),
        axis=1,
    )
    df["country_of_birth_iso"] = df["country_of_birth"].map(normalize_country_iso)
    df["country_of_citizenship_iso"] = df["country_of_citizenship"].map(
        normalize_country_iso
    )
    return df


PLAYER_SCD_COMPARE_COLUMNS = [
    "current_club_id",
    "current_club_name",
    "market_value_in_eur",
    "position_category",
    "sub_position",
    "last_season",
]


def build_dim_players(
    players: pd.DataFrame,
    taxonomy: dict[str, list[str]],
    effective_date: pd.Timestamp,
    existing: pd.DataFrame | None = None,
    effective_dates: dict[Any, pd.Timestamp] | None = None,
) -> pd.DataFrame:
    """SCD Type 2 player dimension."""
    prepared = prepare_players_for_scd(players, taxonomy)
    attribute_columns = [
        "player_id",
        "first_name",
        "last_name",
        "name",
        "last_season",
        "current_club_id",
        "player_code",
        "country_of_birth",
        "country_of_birth_iso",
        "city_of_birth",
        "country_of_citizenship",
        "country_of_citizenship_iso",
        "date_of_birth",
        "sub_position",
        "position",
        "position_category",
        "foot",
        "height_in_cm",
        "current_club_name",
        "market_value_in_eur",
        "highest_market_value_in_eur",
        "is_deleted",
    ]
    incoming = prepared.reindex(columns=attribute_columns)

    all_players = players.copy()
    deleted_ids = all_players.loc[
        all_players["is_deleted"].fillna(0) == 1, "player_id"
    ].tolist()
    if deleted_ids:
        deleted_rows = all_players[all_players["player_id"].isin(deleted_ids)].copy()
        deleted_rows["position_category"] = deleted_rows.apply(
            lambda row: map_position_category(row["position"], row["sub_position"], taxonomy),
            axis=1,
        )
        deleted_rows["country_of_birth_iso"] = deleted_rows["country_of_birth"].map(
            normalize_country_iso
        )
        deleted_rows["country_of_citizenship_iso"] = deleted_rows[
            "country_of_citizenship"
        ].map(normalize_country_iso)
        deleted_rows["is_deleted"] = 1
        incoming = pd.concat(
            [
                incoming,
                deleted_rows.reindex(columns=attribute_columns),
            ],
            ignore_index=True,
        ).drop_duplicates(subset=["player_id"], keep="last")

    result = existing
    for _, row in incoming.iterrows():
        player_id = row["player_id"]
        if row.get("is_deleted", 0) == 1:
            eff = effective_date
        elif effective_dates:
            eff = effective_dates.get(player_id, effective_date)
        else:
            eff = effective_date
        result = apply_scd_type2(
            result,
            row.to_frame().T,
            business_key="player_id",
            compare_columns=PLAYER_SCD_COMPARE_COLUMNS,
            effective_date=eff,
            sk_column="player_sk",
            close_deleted=True,
            deleted_flag_column="is_deleted",
        )
    return result if result is not None else pd.DataFrame()


def build_dim_player_valuations(
    valuations: pd.DataFrame,
    existing: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """SCD Type 2 valuation history dimension."""
    if existing is None or existing.empty:
        return build_valuation_scd(valuations)
    return merge_valuation_scd(existing, valuations)


def merge_reference_dimension(
    existing: pd.DataFrame | None,
    incoming: pd.DataFrame,
    *,
    business_key: str,
    sk_column: str,
) -> pd.DataFrame:
    """Upsert static dimensions (competitions, clubs) by business key."""
    if existing is None or existing.empty:
        return incoming

    merged = existing.set_index(business_key)
    next_sk = int(existing[sk_column].max()) + 1
    for _, row in incoming.iterrows():
        key = row[business_key]
        row = row.copy()
        if key in merged.index:
            sk = merged.loc[key, sk_column]
            if isinstance(sk, pd.Series):
                sk = sk.iloc[0]
            row[sk_column] = sk
        else:
            row[sk_column] = next_sk
            next_sk += 1
        merged.loc[key] = row

    result = merged.reset_index()
    if sk_column in result.columns:
        cols = [sk_column] + [c for c in result.columns if c != sk_column]
        result = result[cols]
    return result.reset_index(drop=True)


def player_effective_dates(
    players: pd.DataFrame,
    valuations: pd.DataFrame,
    default_date: pd.Timestamp,
) -> dict[Any, pd.Timestamp]:
    """Per-player effective dates — latest valuation date when present."""
    dates: dict[Any, pd.Timestamp] = {}
    if not valuations.empty:
        vals = valuations.copy()
        vals["date"] = pd.to_datetime(vals["date"]).dt.normalize()
        latest = vals.groupby("player_id")["date"].max()
        dates = {k: pd.Timestamp(v) for k, v in latest.items()}

    for player_id in players["player_id"].tolist():
        dates.setdefault(player_id, default_date)
    return dates