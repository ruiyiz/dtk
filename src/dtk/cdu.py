"""Data upload with upserts and revision tracking."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import polars as pl

from dtk._constants import DATA_SETS, WIDE_COLUMN_MAP
from dtk.errors import DtkFieldNotFoundError, DtkValueError
from dtk.security import lookup_securities

if TYPE_CHECKING:
    from dtk.store import Store


def cdu(
    store: Store,
    df: pl.DataFrame,
    flds: list[str] | None = None,
    ignore_older: bool = False,
    check_diff: bool = True,
) -> dict[str, int]:
    """Upload a DataFrame of field values to the appropriate storage tables.

    Parameters
    ----------
    store:
        The Store instance.
    df:
        Must contain SecurityId (or Ticker) and ValueDate columns.
    flds:
        Field mnemonics to upload. Auto-detected from df columns if None.
    ignore_older:
        If True, don't set LastFlag=False on existing rows.
    check_diff:
        If True, skip rows identical to existing data.

    Returns
    -------
    dict with keys "wide" and "long" showing rows uploaded to each storage type.
    """
    raise NotImplementedError("cdu is not yet implemented")


def cdu_pricing(store: Store, df: pl.DataFrame) -> int:
    """Upload pricing data (upsert to Pricing table).

    Parameters
    ----------
    store:
        The Store instance.
    df:
        Must contain SecurityId (or Ticker) and ValueDate, plus at least one
        pricing column (PxClose, PxHigh, PxLow, PxOpen, PxLast, Volume,
        NavClose, NavLast, TotalReturn, DividendAmount, AdjFactor).

    Returns
    -------
    Number of rows upserted.
    """
    raise NotImplementedError("cdu_pricing is not yet implemented")


def cdu_security(store: Store, df: pl.DataFrame) -> int:
    """Upload security master records (upsert to SecurityMaster table).

    Parameters
    ----------
    store:
        The Store instance.
    df:
        Must contain Ticker and SecurityType columns.

    Returns
    -------
    Number of rows upserted.
    """
    raise NotImplementedError("cdu_security is not yet implemented")


def cdu_dataset(store: Store, df: pl.DataFrame, dataset: str) -> int:
    """Upload records to a data set table (Dividend, CorpEvent, AdjFactor).

    Parameters
    ----------
    store:
        The Store instance.
    df:
        Records to upload. Must contain SecurityId (or Ticker).
    dataset:
        One of "dividend", "event", "adjfactor".

    Returns
    -------
    Number of rows inserted.
    """
    dataset = dataset.lower()
    if dataset not in DATA_SETS:
        known = ", ".join(DATA_SETS.keys())
        raise DtkValueError(f"Unknown dataset {dataset!r}. Available: {known}")

    raise NotImplementedError("cdu_dataset is not yet implemented")


def cdu_override(
    store: Store,
    security_id: int,
    field: str,
    value,
    value_date: date | None = None,
    reason: str | None = None,
    created_by: str | None = None,
) -> None:
    """Set a field override for a security on a specific date.

    Parameters
    ----------
    store:
        The Store instance.
    security_id:
        SecurityMaster.Id.
    field:
        Field mnemonic.
    value:
        Override value.
    value_date:
        Date for the override. Defaults to today.
    reason:
        Optional reason string.
    created_by:
        Optional creator identifier.
    """
    raise NotImplementedError("cdu_override is not yet implemented")
