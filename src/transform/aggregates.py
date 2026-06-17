"""Gold layer — business aggregates built from silver star schema."""

from __future__ import annotations

import pandas as pd


def build_gold_player_performance(
    fact_appearances: pd.DataFrame,
    dim_date: pd.DataFrame,
    dim_players: pd.DataFrame,
) -> pd.DataFrame:
    """Player × season performance metrics from appearances."""
    if fact_appearances.empty:
        return _empty_player_performance()

    season_lookup = dim_date.set_index("date_sk")["season"].to_dict()
    df = fact_appearances.copy()
    df["season"] = df["date_sk"].map(season_lookup)
    df = df.dropna(subset=["season"])

    agg = (
        df.groupby(["player_id", "season"], as_index=False)
        .agg(
            matches_played=("game_id", "nunique"),
            total_goals=("goals", "sum"),
            total_assists=("assists", "sum"),
            total_minutes=("minutes_played", "sum"),
        )
        .sort_values(["player_id", "season"])
        .reset_index(drop=True)
    )

    current_players = dim_players[dim_players["is_current"]][
        ["player_id", "name", "position_category", "current_club_id", "current_club_name"]
    ].drop_duplicates(subset=["player_id"])
    return agg.merge(current_players, on="player_id", how="left")


def build_gold_club_performance(
    fact_games: pd.DataFrame,
    dim_clubs: pd.DataFrame,
) -> pd.DataFrame:
    """Club × season results derived from home and away game perspectives."""
    if fact_games.empty:
        return _empty_club_performance()

    home = fact_games.assign(
        club_id=fact_games["home_club_id"],
        club_name=fact_games["home_club_name"],
        result=fact_games["home_result"],
        goals_scored=fact_games["home_club_goals"],
        goals_conceded=fact_games["away_club_goals"],
    )
    away = fact_games.assign(
        club_id=fact_games["away_club_id"],
        club_name=fact_games["away_club_name"],
        result=fact_games["away_result"],
        goals_scored=fact_games["away_club_goals"],
        goals_conceded=fact_games["home_club_goals"],
    )
    combined = pd.concat([home, away], ignore_index=True)
    combined["season"] = combined["season_derived"]

    agg = (
        combined.groupby(["club_id", "season"], as_index=False)
        .agg(
            club_name=("club_name", "first"),
            matches_played=("game_id", "count"),
            wins=("result", lambda s: (s == "W").sum()),
            losses=("result", lambda s: (s == "L").sum()),
            draws=("result", lambda s: (s == "D").sum()),
            goals_scored=("goals_scored", "sum"),
            goals_conceded=("goals_conceded", "sum"),
        )
        .sort_values(["club_id", "season"])
        .reset_index(drop=True)
    )

    club_lookup = dim_clubs.set_index("club_id")["name"].to_dict()
    agg["club_name"] = agg["club_id"].map(club_lookup).fillna(agg["club_name"])
    return agg


def build_gold_player_valuation_trend(
    dim_player_valuations: pd.DataFrame,
    rolling_window: int = 3,
) -> pd.DataFrame:
    """Player valuation history with rolling average market value."""
    if dim_player_valuations.empty:
        return _empty_valuation_trend()

    df = dim_player_valuations.copy()
    df["valuation_date"] = pd.to_datetime(df["valuation_date"]).dt.normalize()
    df = df.sort_values(["player_id", "valuation_date"]).reset_index(drop=True)

    df["rolling_average_market_value"] = (
        df.groupby("player_id")["market_value_in_eur"]
        .transform(lambda s: s.rolling(window=rolling_window, min_periods=1).mean())
        .round(0)
    )

    return df[
        [
            "player_id",
            "valuation_date",
            "market_value_in_eur",
            "rolling_average_market_value",
            "current_club_id",
            "current_club_name",
            "is_current",
        ]
    ].rename(columns={"market_value_in_eur": "market_value"})


def _empty_player_performance() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "player_id",
            "season",
            "matches_played",
            "total_goals",
            "total_assists",
            "total_minutes",
            "name",
            "position_category",
            "current_club_id",
            "current_club_name",
        ]
    )


def _empty_club_performance() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "club_id",
            "season",
            "club_name",
            "matches_played",
            "wins",
            "losses",
            "draws",
            "goals_scored",
            "goals_conceded",
        ]
    )


def _empty_valuation_trend() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "player_id",
            "valuation_date",
            "market_value",
            "rolling_average_market_value",
            "current_club_id",
            "current_club_name",
            "is_current",
        ]
    )