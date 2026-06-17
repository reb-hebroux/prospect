"""Silver fact table builders."""

from __future__ import annotations

import pandas as pd

from src.transform.cleaning import compute_match_results, derive_season
from src.transform.scd import lookup_surrogate_at_date


def build_fact_games(
    games: pd.DataFrame,
    dim_date: pd.DataFrame,
    dim_competitions: pd.DataFrame,
    dim_clubs: pd.DataFrame,
    season_start_month: int = 8,
) -> pd.DataFrame:
    """Game fact table with outcomes and dimension surrogate keys."""
    if games.empty:
        return pd.DataFrame()

    df = games.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()
    df["season_derived"] = df["date"].apply(
        lambda d: derive_season(d, season_start_month)
    )

    home_away = df.apply(
        lambda row: compute_match_results(row["home_club_goals"], row["away_club_goals"]),
        axis=1,
        result_type="expand",
    )
    df["home_result"] = home_away[0]
    df["away_result"] = home_away[1]

    date_lookup = dim_date.set_index("full_date")["date_sk"].to_dict()
    comp_lookup = dim_competitions.set_index("competition_id")["competition_sk"].to_dict()
    club_lookup = dim_clubs.set_index("club_id")["club_sk"].to_dict()

    df["date_sk"] = df["date"].map(date_lookup)
    df["competition_sk"] = df["competition_id"].map(comp_lookup)
    df["home_club_sk"] = df["home_club_id"].map(club_lookup)
    df["away_club_sk"] = df["away_club_id"].map(club_lookup)

    columns = [
        "game_id",
        "competition_id",
        "competition_sk",
        "season",
        "season_derived",
        "round",
        "date",
        "date_sk",
        "home_club_id",
        "away_club_id",
        "home_club_sk",
        "away_club_sk",
        "home_club_goals",
        "away_club_goals",
        "home_result",
        "away_result",
        "home_club_name",
        "away_club_name",
        "stadium",
        "attendance",
        "competition_type",
    ]
    return df[columns]


def build_fact_appearances(
    appearances: pd.DataFrame,
    dim_players: pd.DataFrame,
    dim_clubs: pd.DataFrame,
    dim_competitions: pd.DataFrame,
    dim_date: pd.DataFrame,
    fact_games: pd.DataFrame,
) -> pd.DataFrame:
    """Appearance fact table linked to dimensions and games."""
    if appearances.empty:
        return pd.DataFrame()

    df = appearances.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.normalize()

    club_lookup = dim_clubs.set_index("club_id")["club_sk"].to_dict()
    comp_lookup = dim_competitions.set_index("competition_id")["competition_sk"].to_dict()
    date_lookup = dim_date.set_index("full_date")["date_sk"].to_dict()
    game_lookup = fact_games.set_index("game_id")["date_sk"].to_dict()

    df["player_sk"] = df.apply(
        lambda row: lookup_surrogate_at_date(
            dim_players,
            "player_id",
            "player_sk",
            row["player_id"],
            row["date"],
        ),
        axis=1,
    )
    df["player_club_sk"] = df["player_club_id"].map(club_lookup)
    df["competition_sk"] = df["competition_id"].map(comp_lookup)
    df["date_sk"] = df["date"].map(date_lookup)
    df["game_date_sk"] = df["game_id"].map(game_lookup)

    columns = [
        "appearance_id",
        "game_id",
        "player_id",
        "player_sk",
        "player_club_id",
        "player_club_sk",
        "player_current_club_id",
        "date",
        "date_sk",
        "game_date_sk",
        "player_name",
        "competition_id",
        "competition_sk",
        "yellow_cards",
        "red_cards",
        "goals",
        "assists",
        "minutes_played",
    ]
    return df[columns]


def merge_facts(
    existing: pd.DataFrame | None,
    incoming: pd.DataFrame,
    natural_key: str,
) -> pd.DataFrame:
    """Upsert fact rows by natural key (append new, replace changed)."""
    if existing is None or existing.empty:
        return incoming.reset_index(drop=True)
    if incoming.empty:
        return existing.reset_index(drop=True)

    combined = pd.concat([existing, incoming], ignore_index=True)
    return combined.drop_duplicates(subset=[natural_key], keep="last").reset_index(drop=True)