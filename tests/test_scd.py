"""Tests for SCD Type 2 helpers."""

from __future__ import annotations

import pandas as pd

from src.transform.scd import (
    OPEN_END_DATE,
    apply_scd_type2,
    build_valuation_scd,
    lookup_surrogate_at_date,
)


def test_apply_scd_type2_initial_load():
    incoming = pd.DataFrame(
        {
            "player_id": [1001, 1002],
            "current_club_id": [101, 101],
            "market_value_in_eur": [100, 90],
        }
    )
    result = apply_scd_type2(
        None,
        incoming,
        business_key="player_id",
        compare_columns=["current_club_id", "market_value_in_eur"],
        effective_date=pd.Timestamp("2024-08-01"),
        sk_column="player_sk",
    )
    assert len(result) == 2
    assert result["is_current"].tolist() == [True, True]
    assert (result["end_date"] == OPEN_END_DATE).all()


def test_apply_scd_type2_club_change():
    initial = apply_scd_type2(
        None,
        pd.DataFrame(
            {
                "player_id": [1003],
                "current_club_id": [102],
                "market_value_in_eur": [75],
            }
        ),
        business_key="player_id",
        compare_columns=["current_club_id", "market_value_in_eur"],
        effective_date=pd.Timestamp("2024-08-01"),
        sk_column="player_sk",
    )
    updated = apply_scd_type2(
        initial,
        pd.DataFrame(
            {
                "player_id": [1003],
                "current_club_id": [103],
                "market_value_in_eur": [85],
            }
        ),
        business_key="player_id",
        compare_columns=["current_club_id", "market_value_in_eur"],
        effective_date=pd.Timestamp("2026-03-01"),
        sk_column="player_sk",
    )
    assert len(updated) == 2
    current = updated[updated["is_current"]]
    assert len(current) == 1
    assert current.iloc[0]["current_club_id"] == 103
    closed = updated[~updated["is_current"]]
    assert closed.iloc[0]["end_date"] == pd.Timestamp("2026-02-28")


def test_build_valuation_scd_end_dates():
    valuations = pd.DataFrame(
        {
            "player_id": [1001, 1001],
            "date": ["2024-08-01", "2024-09-01"],
            "market_value_in_eur": [130, 135],
            "current_club_id": [101, 101],
            "current_club_name": ["Arsenal FC", "Arsenal FC"],
            "player_club_domestic_competition_id": ["GB1", "GB1"],
        }
    )
    result = build_valuation_scd(valuations)
    assert len(result) == 2
    first = result.iloc[0]
    second = result.iloc[1]
    assert first["end_date"] == pd.Timestamp("2024-08-31")
    assert bool(second["is_current"])
    assert second["end_date"] == OPEN_END_DATE


def test_lookup_surrogate_at_date():
    dim = pd.DataFrame(
        {
            "player_sk": [1, 2],
            "player_id": [1003, 1003],
            "effective_date": [pd.Timestamp("2024-08-01"), pd.Timestamp("2026-03-01")],
            "end_date": [pd.Timestamp("2026-02-28"), OPEN_END_DATE],
            "is_current": [False, True],
        }
    )
    assert lookup_surrogate_at_date(
        dim, "player_id", "player_sk", 1003, pd.Timestamp("2024-08-17")
    ) == 1
    assert lookup_surrogate_at_date(
        dim, "player_id", "player_sk", 1003, pd.Timestamp("2026-03-16")
    ) == 2