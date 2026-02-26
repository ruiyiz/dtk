"""Historical time series data access."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import polars as pl

if TYPE_CHECKING:
    from dtk.store import Store


def cdh(
    store: Store,
    x: list[str] | list[int],
    flds: list[str],
    start_dt: date,
    end_dt: date | None = None,
    date_mode: str = "as_of",
    exch_code: str = "US",
    period: str = "D",
    days: str = "N",
    fill: str = "NA",
    fx: str | None = None,
    id_type: str = "ticker",
    overrides: bool = False,
) -> pl.DataFrame:
    """Return a time series of fields for securities over a date range.

    Parameters
    ----------
    store:
        The Store instance.
    x:
        Security identifiers.
    flds:
        Field mnemonics to retrieve.
    start_dt:
        Start date (inclusive).
    end_dt:
        End date (inclusive). Defaults to today.
    date_mode:
        "as_of" or "as_seen".
    exch_code:
        Exchange code for calendar-based operations.
    period:
        Periodicity: "D", "W", "M", "Q", "HY", "Y".
    days:
        Day type: "N" (non-trading/weekdays), "C" (calendar), "T" (trading).
    fill:
        "NA" or "P" (forward-fill).
    fx:
        Target currency for FX conversion.
    id_type:
        "ticker", "id", or "blp".
    overrides:
        Whether to apply FieldOverride values.

    Returns
    -------
    pl.DataFrame
        Columns: SecurityId, Ticker, ValueDate, <fld1>, <fld2>, ...
        For single-security + single-field queries, schema is {"date": pl.Date, "value": pl.Float64}.
    """
    raise NotImplementedError("cdh is not yet implemented")


def _query_cdh_wide(
    store: Store,
    sec_ids: list[int],
    fld_spec: pl.DataFrame,
    start_dt: date,
    end_dt: date,
) -> pl.DataFrame:
    raise NotImplementedError


def _query_cdh_long(
    store: Store,
    sec_ids: list[int],
    fld_spec: pl.DataFrame,
    start_dt: date,
    end_dt: date,
    date_mode: str,
) -> pl.DataFrame:
    raise NotImplementedError


def _scale_tseries(
    df: pl.DataFrame,
    period: str,
    exch_code: str,
    days: str,
    partial: bool,
) -> pl.DataFrame:
    """Aggregate daily data to the requested periodicity."""
    raise NotImplementedError


def _fill_cdh_previous(df: pl.DataFrame) -> pl.DataFrame:
    """Forward-fill null values within each security's time series."""
    raise NotImplementedError
