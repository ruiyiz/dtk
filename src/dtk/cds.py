"""Data set queries: dividends, corporate events, adjustment factors."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import polars as pl

from dtk._constants import DATA_SETS
from dtk.errors import DtkValueError
from dtk.security import lookup_securities

if TYPE_CHECKING:
    from dtk.store import Store


def cds(
    store: Store,
    x: list[str] | list[int],
    dataset: str,
    start_dt: date | None = None,
    end_dt: date | None = None,
    id_type: str = "ticker",
    **kwargs,
) -> pl.DataFrame:
    """Query an event-style data set for given securities.

    Parameters
    ----------
    store:
        The Store instance.
    x:
        Security identifiers.
    dataset:
        One of "dividend", "event", "adjfactor".
    start_dt:
        Lower bound on the date column (inclusive).
    end_dt:
        Upper bound on the date column (inclusive).
    id_type:
        "ticker" or "id".
    **kwargs:
        Additional filters (e.g. dividend_type, event_type, adj_type).

    Returns
    -------
    pl.DataFrame
    """
    dataset = dataset.lower()
    if dataset not in DATA_SETS:
        known = ", ".join(DATA_SETS.keys())
        raise DtkValueError(f"Unknown dataset {dataset!r}. Available: {known}")

    set_def = DATA_SETS[dataset]
    secs = lookup_securities(store, x, id_type=id_type, include_inactive=True)
    if secs.is_empty():
        return pl.DataFrame()

    sec_ids_str = ", ".join(str(i) for i in secs["Id"].to_list())
    where_parts = [f"t.SecurityId IN ({sec_ids_str})"]

    if start_dt is not None:
        where_parts.append(f"t.{set_def.date_col} >= '{start_dt}'")
    if end_dt is not None:
        where_parts.append(f"t.{set_def.date_col} <= '{end_dt}'")

    extra = _build_extra_filters(dataset, kwargs)
    where_parts.extend(extra)

    where_clause = " AND ".join(where_parts)
    sql = (
        f"SELECT t.*, s.Ticker "
        f"FROM {set_def.table} t "
        f"JOIN SecurityMaster s ON t.SecurityId = s.Id "
        f"WHERE {where_clause} "
        f"ORDER BY t.SecurityId, t.{set_def.date_col}"
    )

    result = store._backend.query(sql)
    return result


def cds_dividend(
    store: Store,
    x: list[str] | list[int],
    start_dt: date | None = None,
    end_dt: date | None = None,
    dividend_type: str | None = None,
    special_only: bool = False,
    id_type: str = "ticker",
) -> pl.DataFrame:
    return cds(
        store,
        x,
        "dividend",
        start_dt=start_dt,
        end_dt=end_dt,
        id_type=id_type,
        dividend_type=dividend_type,
        special_only=special_only,
    )


def cds_event(
    store: Store,
    x: list[str] | list[int],
    start_dt: date | None = None,
    end_dt: date | None = None,
    event_type: str | int | None = None,
    status: str = "Active",
    id_type: str = "ticker",
) -> pl.DataFrame:
    return cds(
        store,
        x,
        "event",
        start_dt=start_dt,
        end_dt=end_dt,
        id_type=id_type,
        event_type=event_type,
        status=status,
    )


def cds_adjfactor(
    store: Store,
    x: list[str] | list[int],
    start_dt: date | None = None,
    end_dt: date | None = None,
    adj_type: str | None = None,
    id_type: str = "ticker",
) -> pl.DataFrame:
    return cds(
        store,
        x,
        "adjfactor",
        start_dt=start_dt,
        end_dt=end_dt,
        id_type=id_type,
        adj_type=adj_type,
    )


def _build_extra_filters(dataset: str, params: dict) -> list[str]:
    filters: list[str] = []

    if dataset == "dividend":
        if params.get("dividend_type") is not None:
            filters.append(f"DividendType = '{params['dividend_type']}'")
        if params.get("special_only"):
            filters.append("SpecialFlag = TRUE")

    elif dataset == "event":
        if params.get("event_type") is not None:
            et = params["event_type"]
            if isinstance(et, str):
                filters.append(
                    f"EventTypeId IN ("
                    f"SELECT EventTypeId FROM CorpEventRef WHERE EventType = '{et}')"
                )
            else:
                filters.append(f"EventTypeId = {int(et)}")
        if params.get("status") is not None:
            filters.append(f"Status = '{params['status']}'")

    elif dataset == "adjfactor":
        if params.get("adj_type") is not None:
            filters.append(f"AdjType = '{params['adj_type']}'")

    return filters
