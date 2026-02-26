"""Point-in-time data access."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import polars as pl

from dtk.convert import apply_type_map
from dtk.security import lookup_securities

if TYPE_CHECKING:
    from dtk.store import Store


def cdp(
    store: Store,
    x: list[str] | list[int],
    flds: list[str],
    dt: date | None = None,
    date_mode: str = "as_of",
    fill: str = "NA",
    fx: str | None = None,
    id_type: str = "ticker",
    overrides: bool = False,
) -> pl.DataFrame:
    """Return a cross-sectional snapshot of fields for securities at a given date.

    Parameters
    ----------
    store:
        The Store instance.
    x:
        Security identifiers (tickers or IDs).
    flds:
        Field mnemonics to retrieve.
    dt:
        Snapshot date. Defaults to today.
    date_mode:
        "as_of" (latest available up to dt) or "as_seen" (exactly dt).
    fill:
        "NA" (leave nulls) or "P" (forward-fill from previous date).
    fx:
        Target currency for FX conversion (None = no conversion).
    id_type:
        "ticker", "id", or "blp".
    overrides:
        Whether to apply FieldOverride values.

    Returns
    -------
    pl.DataFrame
        Columns: SecurityId, Ticker, <fld1>, <fld2>, ...
    """
    raise NotImplementedError("cdp is not yet implemented")


def _raw_data_wide(
    store: Store,
    sec_ids: list[int],
    fld_spec: pl.DataFrame,
    dt: date,
) -> pl.DataFrame:
    """Query wide-format tables for a list of securities at a single date."""
    raise NotImplementedError


def _raw_data_long(
    store: Store,
    sec_ids: list[int],
    fld_spec: pl.DataFrame,
    dt: date,
    date_mode: str,
) -> pl.DataFrame:
    """Query long-format tables (FieldSnapshot/SecuritySnapshot) at a single date."""
    raise NotImplementedError


def _apply_overrides(
    store: Store,
    df: pl.DataFrame,
    flds: list[str],
    dt: date,
) -> pl.DataFrame:
    """Apply FieldOverride values to the output DataFrame."""
    raise NotImplementedError


def _apply_fx_conversion(
    store: Store,
    df: pl.DataFrame,
    flds: list[str],
    fx: str,
    dt: date,
) -> pl.DataFrame:
    """Convert field values from security currency to target FX."""
    raise NotImplementedError


def _fill_previous(
    store: Store,
    df: pl.DataFrame,
    sec_ids: list[int],
    flds: list[str],
    dt: date,
) -> pl.DataFrame:
    """Forward-fill null field values from previous date."""
    raise NotImplementedError
