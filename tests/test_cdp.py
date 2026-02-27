from datetime import date

import polars as pl

from dtk.cdp import cdp


def test_cdp_returns_dataframe(seeded_store):
    result = cdp(seeded_store, ["SPY"], ["PX_CLOSE"], dt=date(2024, 1, 2))
    assert isinstance(result, pl.DataFrame)


def test_cdp_columns(seeded_store):
    result = cdp(seeded_store, ["SPY"], ["PX_CLOSE"], dt=date(2024, 1, 2))
    assert "SecurityId" in result.columns
    assert "Ticker" in result.columns
    assert "PX_CLOSE" in result.columns


def test_cdp_value(seeded_store):
    result = cdp(seeded_store, ["SPY"], ["PX_CLOSE"], dt=date(2024, 1, 2))
    assert result.filter(pl.col("Ticker") == "SPY")["PX_CLOSE"][0] == 450.0


def test_cdp_multiple_securities(seeded_store):
    result = cdp(seeded_store, ["SPY", "AAPL"], ["PX_CLOSE"], dt=date(2024, 1, 3))
    assert len(result) == 2


def test_cdp_unknown_security_returns_empty(seeded_store):
    result = cdp(seeded_store, ["UNKNOWN"], ["PX_CLOSE"], dt=date(2024, 1, 2))
    assert result.is_empty()
