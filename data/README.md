# Data snapshots

## `data_day1/` — baseline load

Initial feed containing 6 tables aligned with the [Kaggle Player Scores](https://www.kaggle.com/datasets/davidcariboo/player-scores/data) schema:

| Table | Rows | Notes |
|-------|------|-------|
| competitions | 2 | GB1, ES1 |
| clubs | 3 | Arsenal, Chelsea, Liverpool |
| players | 5 | IDs 1001–1005 |
| games | 4 | Aug 2024 fixtures |
| appearances | 9 | One row per player per game |
| player_valuations | 7 | Historical valuation points |

## `data_day2/` — incremental feed

Same tables with **delta scenarios** for testing incremental load and SCD Type 2:

| Change type | Example |
|-------------|---------|
| **New games** | `2005`, `2006`, `2007` — Mar 2026 future dates |
| **New appearances** | `3010`–`3015` linked to new games |
| **Updated player** | `1003` Cole Palmer: `current_club_id` 102 → 103 (Chelsea → Liverpool) |
| **Updated valuations** | New rows dated `2026-03-01`; Salah 112M → 125M |
| **Soft delete** | `1005` van Dijk: `is_deleted=1` |

## Full Kaggle dataset (optional)

Replace snapshot CSVs with the full download:

```bash
kaggle datasets download -d davidcariboo/player-scores -p data/raw --unzip
# Then subset or copy into data_day1/ for local dev
```

Column names match the public Transfermarkt export (see `config/schemas/bronze.yaml`).