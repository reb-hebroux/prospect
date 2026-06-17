"""Tests for silver cleaning transforms."""

from __future__ import annotations

import pandas as pd

from src.config_loader import load_config
from src.transform.cleaning import (
    compute_match_results,
    derive_season,
    map_position_category,
    normalize_country_iso,
    snapshot_as_of_date,
)


def test_map_position_category_attack():
    taxonomy = load_config()["transforms"]["position_taxonomy"]
    assert map_position_category("Attack", "Right Winger", taxonomy) == "Attack"
    assert map_position_category("Midfield", "Attacking Midfield", taxonomy) == "Midfield"
    assert map_position_category("Defence", "Centre-Back", taxonomy) == "Defence"
    assert map_position_category("Goalkeeper", "Goalkeeper", taxonomy) == "Goalkeeper"


def test_normalize_country_iso():
    assert normalize_country_iso("England") == "GB"
    assert normalize_country_iso("Egypt") == "EG"
    assert normalize_country_iso("Netherlands") == "NL"


def test_derive_season_august_cutoff():
    assert derive_season(pd.Timestamp("2024-08-17")) == 2024
    assert derive_season(pd.Timestamp("2026-03-15")) == 2025
    assert derive_season(pd.Timestamp("2025-07-31")) == 2024


def test_compute_match_results():
    assert compute_match_results(2, 1) == ("W", "L")
    assert compute_match_results(0, 2) == ("L", "W")
    assert compute_match_results(1, 1) == ("D", "D")


def test_snapshot_as_of_date():
    games = pd.DataFrame({"date": ["2024-08-17", "2026-03-22"]})
    as_of = snapshot_as_of_date(games)
    assert as_of == pd.Timestamp("2026-03-22")