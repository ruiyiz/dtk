"""Security dataclass, lookup, and resolution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

import polars as pl

from dtk.errors import DtkSecurityNotFoundError

if TYPE_CHECKING:
    from dtk.store import Store


@dataclass
class Security:
    id: int
    ticker: str
    security_type: str
    currency: str
    exchange_code: str
    blp_ticker: str | None
    inception_date: date | None
    termination_date: date | None
    is_active: bool


def lookup_securities(
    store: Store,
    x: list[str] | list[int],
    id_type: str = "ticker",
    dt: date | None = None,
    include_inactive: bool = False,
) -> pl.DataFrame:
    """Look up securities and return a DataFrame with SecurityMaster columns."""
    where_parts: list[str] = []

    if id_type == "id":
        ids_str = ", ".join(str(int(v)) for v in x)
        where_parts.append(f"Id IN ({ids_str})")
    elif id_type == "ticker":
        quoted = ", ".join(f"'{v}'" for v in x)
        where_parts.append(f"Ticker IN ({quoted})")
    elif id_type == "blp":
        quoted = ", ".join(f"'{v}'" for v in x)
        where_parts.append(f"BlpTicker IN ({quoted})")

    if not include_inactive:
        where_parts.append("IsActive = TRUE")

    where_clause = "WHERE " + " AND ".join(where_parts) if where_parts else ""
    secs = store._backend.query(f"SELECT * FROM SecurityMaster {where_clause}")

    if dt is not None and not secs.is_empty():
        secs = secs.filter(
            (pl.col("InceptionDate").is_null() | (pl.col("InceptionDate") <= dt))
            & (pl.col("TerminationDate").is_null() | (pl.col("TerminationDate") >= dt))
        )

    return secs


def as_security(
    store: Store,
    x,
    dt: date | None = None,
    include_inactive: bool = False,
) -> pl.DataFrame:
    """Convert various inputs to a SecurityMaster DataFrame."""
    if isinstance(x, pl.DataFrame):
        if "Id" in x.columns:
            return lookup_securities(
                store,
                x["Id"].to_list(),
                id_type="id",
                dt=dt,
                include_inactive=include_inactive,
            )
        if "Ticker" in x.columns:
            return lookup_securities(
                store,
                x["Ticker"].to_list(),
                id_type="ticker",
                dt=dt,
                include_inactive=include_inactive,
            )
        if "SecurityId" in x.columns:
            return lookup_securities(
                store,
                x["SecurityId"].to_list(),
                id_type="id",
                dt=dt,
                include_inactive=include_inactive,
            )
        raise DtkSecurityNotFoundError(
            "DataFrame must have Id, Ticker, or SecurityId column"
        )

    if isinstance(x, (list, tuple)):
        if len(x) == 0:
            return pl.DataFrame()
        if isinstance(x[0], int):
            return lookup_securities(
                store, list(x), id_type="id", dt=dt, include_inactive=include_inactive
            )
        return lookup_securities(
            store, list(x), id_type="ticker", dt=dt, include_inactive=include_inactive
        )

    if isinstance(x, str):
        return lookup_securities(
            store, [x], id_type="ticker", dt=dt, include_inactive=include_inactive
        )
    if isinstance(x, int):
        return lookup_securities(
            store, [x], id_type="id", dt=dt, include_inactive=include_inactive
        )

    raise DtkSecurityNotFoundError(f"Cannot resolve securities from {type(x).__name__}")


def get_security_ids(
    store: Store,
    x,
    dt: date | None = None,
    include_inactive: bool = False,
) -> list[int]:
    secs = as_security(store, x, dt=dt, include_inactive=include_inactive)
    return secs["Id"].to_list()


def get_security_tickers(
    store: Store,
    x,
    dt: date | None = None,
    include_inactive: bool = False,
) -> list[str]:
    secs = as_security(store, x, dt=dt, include_inactive=include_inactive)
    return secs["Ticker"].to_list()


def get_security_types(
    store: Store,
    x,
    include_inactive: bool = False,
) -> list[str]:
    secs = as_security(store, x, include_inactive=include_inactive)
    return secs["SecurityType"].unique().to_list()


def is_active(
    store: Store,
    x,
    dt: date | None = None,
) -> list[bool]:
    secs = as_security(store, x, include_inactive=True)
    if dt is None:
        dt = date.today()
    mask = (secs["InceptionDate"].is_null() | (secs["InceptionDate"] <= dt)) & (
        secs["TerminationDate"].is_null() | (secs["TerminationDate"] >= dt)
    )
    return mask.to_list()
