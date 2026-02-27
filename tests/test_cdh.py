from datetime import date

import polars as pl

from dtk.cdh import cdh


def test_cdh_returns_dataframe(seeded_store):
    result = cdh(
        seeded_store,
        ["SPY"],
        ["PX_CLOSE"],
        start_dt=date(2024, 1, 1),
        end_dt=date(2024, 1, 3),
    )
    assert isinstance(result, pl.DataFrame)


def test_cdh_columns(seeded_store):
    result = cdh(
        seeded_store,
        ["SPY"],
        ["PX_CLOSE"],
        start_dt=date(2024, 1, 1),
        end_dt=date(2024, 1, 3),
    )
    assert "SecurityId" in result.columns
    assert "Ticker" in result.columns
    assert "ValueDate" in result.columns
    assert "PX_CLOSE" in result.columns


def test_cdh_row_count(seeded_store):
    result = cdh(
        seeded_store,
        ["SPY"],
        ["PX_CLOSE"],
        start_dt=date(2024, 1, 1),
        end_dt=date(2024, 1, 3),
    )
    assert len(result) == 2


def test_cdh_multiple_securities(seeded_store):
    result = cdh(
        seeded_store,
        ["SPY", "AAPL"],
        ["PX_CLOSE"],
        start_dt=date(2024, 1, 1),
        end_dt=date(2024, 1, 3),
    )
    assert len(result) == 4


def test_cdh_no_data_range_returns_empty(seeded_store):
    result = cdh(
        seeded_store,
        ["SPY"],
        ["PX_CLOSE"],
        start_dt=date(2023, 1, 1),
        end_dt=date(2023, 12, 31),
    )
    assert result.is_empty()
