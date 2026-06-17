"""Silver-layer cleaning and enrichment transforms."""

from __future__ import annotations

from typing import Any

import pandas as pd
import pycountry

# Football dataset uses constituent country names not always in ISO 3166 as-is.
COUNTRY_ALIASES: dict[str, str] = {
    "england": "GB",
    "scotland": "GB",
    "wales": "GB",
    "northern ireland": "GB",
    "korea, south": "KR",
    "south korea": "KR",
    "usa": "US",
    "united states": "US",
}


def map_position_category(
    position: str | None,
    sub_position: str | None,
    taxonomy: dict[str, list[str]],
) -> str | None:
    """Map raw position / sub_position to a coarse taxonomy bucket."""
    candidates = [sub_position, position]
    for raw in candidates:
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            continue
        value = str(raw).strip()
        for category, values in taxonomy.items():
            if value in values:
                return category
    return None


def normalize_country_iso(country_name: str | None) -> str | None:
    """Return ISO 3166-1 alpha-2 code for a country name, when known."""
    if country_name is None or (isinstance(country_name, float) and pd.isna(country_name)):
        return None
    name = str(country_name).strip()
    if not name:
        return None

    alias = COUNTRY_ALIASES.get(name.lower())
    if alias:
        return alias

    try:
        match = pycountry.countries.lookup(name)
        return match.alpha_2
    except LookupError:
        pass

    for country in pycountry.countries:
        if country.name.lower() == name.lower():
            return country.alpha_2

    return None


def derive_season(game_date: pd.Timestamp | Any, start_month: int = 8) -> int | None:
    """European season: Aug–Jul labelled by the starting calendar year."""
    if game_date is None or pd.isna(game_date):
        return None
    ts = pd.Timestamp(game_date)
    return ts.year if ts.month >= start_month else ts.year - 1


def compute_match_results(
    home_goals: int | float | None,
    away_goals: int | float | None,
) -> tuple[str | None, str | None]:
    """Return (home_result, away_result) as W, L, or D."""
    if home_goals is None or away_goals is None:
        return None, None
    if pd.isna(home_goals) or pd.isna(away_goals):
        return None, None

    home = int(home_goals)
    away = int(away_goals)
    if home > away:
        return "W", "L"
    if home < away:
        return "L", "W"
    return "D", "D"


def snapshot_as_of_date(games: pd.DataFrame, date_column: str = "date") -> pd.Timestamp:
    """Infer logical as-of date from the newest game in a bronze snapshot."""
    if games.empty or date_column not in games.columns:
        return pd.Timestamp.utcnow().normalize()
    return pd.to_datetime(games[date_column]).max().normalize()