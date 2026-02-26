from datetime import date

import polars as pl
import pytest

from dtk.convert import (
    apply_type_map,
    build_type_map,
    cast_series,
    dates_to_str,
    from_storage_row,
    to_storage_value,
)


def test_to_storage_value_dbl():
    result = to_storage_value(3.14, "dbl")
    assert result["ValDbl"] == pytest.approx(3.14)
    assert result["ValChr"] is None


def test_to_storage_value_chr():
    result = to_storage_value("hello", "chr")
    assert result["ValChr"] == "hello"


def test_to_storage_value_date():
    result = to_storage_value("2024-01-15", "date")
    assert result["ValDate"] == date(2024, 1, 15)


def test_to_storage_value_none():
    result = to_storage_value(None, "dbl")
    assert all(v is None for v in result.values())


def test_from_storage_row_dbl():
    row = {"ValChr": None, "ValDbl": 42.0, "ValInt": None, "ValDate": None}
    assert from_storage_row(row, "dbl") == 42.0


def test_cast_series_float():
    s = pl.Series("x", ["1.5", "2.5"])
    result = cast_series(s, "dbl")
    assert result.dtype == pl.Float64


def test_apply_type_map():
    df = pl.DataFrame({"PX_CLOSE": ["1.5", "2.5"], "Ticker": ["SPY", "AAPL"]})
    type_map = {"PX_CLOSE": "dbl"}
    result = apply_type_map(df, type_map)
    assert result["PX_CLOSE"].dtype == pl.Float64


def test_dates_to_str():
    df = pl.DataFrame({"d": [date(2024, 1, 1)], "v": [1.0]})
    result = dates_to_str(df)
    assert result["d"].dtype == pl.String
