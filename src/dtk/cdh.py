"""Historical time series data access."""

from __future__ import annotations

import bisect
from datetime import date
from typing import TYPE_CHECKING

import polars as pl

from dtk._constants import WIDE_COLUMN_MAP
from dtk.convert import apply_type_map
from dtk.date_utils import prev_date, seq_date
from dtk.security import lookup_securities

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
        End date (inclusive). Defaults to previous business day.
    date_mode:
        "as_of" or "as_seen".
    exch_code:
        Exchange code for calendar-based period scaling.
    period:
        Periodicity: "D", "W", "M", "Q", "Y".
    days:
        Day type: "N" (non-trading/weekdays), "C" (calendar), "T" (trading).
    fill:
        "NA" or "P" (forward-fill within each security).
    fx:
        Target currency for FX conversion.
    id_type:
        "ticker", "id", or "blp".
    overrides:
        Unused for now (overrides not applied to historical series).

    Returns
    -------
    pl.DataFrame
        Columns: SecurityId, Ticker, ValueDate, <fld1>, <fld2>, ...
    """
    if end_dt is None:
        end_dt = prev_date(date.today())

    secs = lookup_securities(store, x, id_type=id_type, include_inactive=True)
    if secs.is_empty():
        return pl.DataFrame()

    sec_ids = secs["Id"].to_list()
    fld_map = _map_cdh_fields(store, flds, period)

    results: dict[str, pl.DataFrame] = {}

    if "wide" in fld_map:
        wide_result = _query_cdh_wide(store, sec_ids, fld_map["wide"], start_dt, end_dt)
        if not wide_result.is_empty():
            results["wide"] = wide_result

    if "long" in fld_map:
        long_result = _query_cdh_long(
            store, sec_ids, fld_map["long"], start_dt, end_dt, date_mode
        )
        if not long_result.is_empty():
            results["long"] = long_result

    if not results:
        return pl.DataFrame()

    if len(results) == 1:
        dat = next(iter(results.values()))
    else:
        dat = results["wide"]
        dat = dat.join(
            results["long"], on=["SecurityId", "ValueDate"], how="full", coalesce=True
        )

    if fx is not None:
        dat = _apply_cdh_fx(store, dat, flds, fx, start_dt, end_dt)

    if period != "D":
        dat = _scale_tseries(dat, period, exch_code, days, partial=False)

    if fill == "P":
        dat = _fill_cdh_previous(dat)

    ticker_df = secs.select(["Id", "Ticker"])
    dat = dat.join(ticker_df, left_on="SecurityId", right_on="Id", how="left")

    type_map = store._fields.build_type_map(flds)
    dat = apply_type_map(dat, type_map)

    ordered_cols = ["SecurityId", "Ticker", "ValueDate"] + [
        f for f in flds if f in dat.columns
    ]
    extra = [c for c in dat.columns if c not in ordered_cols]
    dat = dat.select(ordered_cols + extra)
    return dat


def _map_cdh_fields(
    store: Store, flds: list[str], period: str
) -> dict[str, pl.DataFrame]:
    """Map fields to storage, preferring the table that matches the period."""
    period_table_map = {"D": "Pricing", "W": "WeeklyData", "M": "MonthlyData"}
    preferred_table = period_table_map.get(period)

    sub = store._fields.all.filter(
        pl.col("FieldMnemonic").is_in(flds) & pl.col("IsCdh")
    )

    if preferred_table is not None and not sub.is_empty():
        priority = (
            pl.when(pl.col("StorageTable") == preferred_table).then(1).otherwise(2)
        )
        sub = sub.with_columns(priority.alias("_priority"))
        sub = sub.sort("_priority")
        sub = sub.unique(subset=["FieldMnemonic"], keep="first")
        sub = sub.drop("_priority")

    result: dict[str, pl.DataFrame] = {}
    for mode, group in sub.group_by("StorageMode"):
        result[mode[0]] = group
    return result


def _query_cdh_wide(
    store: Store,
    sec_ids: list[int],
    fld_spec: pl.DataFrame,
    start_dt: date,
    end_dt: date,
) -> pl.DataFrame:
    ids_str = ", ".join(str(i) for i in sec_ids)
    tables = fld_spec["StorageTable"].unique().to_list()
    parts: list[pl.DataFrame] = []

    for tbl in tables:
        tbl_flds = fld_spec.filter(pl.col("StorageTable") == tbl)
        mnemonics = tbl_flds["FieldMnemonic"].to_list()

        sql = (
            f"SELECT * FROM {tbl} "
            f"WHERE SecurityId IN ({ids_str}) "
            f"AND ValueDate BETWEEN '{start_dt}' AND '{end_dt}' "
            f"ORDER BY SecurityId, ValueDate"
        )
        result = store._backend.query(sql)
        if result.is_empty():
            continue

        rename = {}
        for mnemonic in mnemonics:
            storage_col = WIDE_COLUMN_MAP.get(mnemonic, mnemonic)
            if storage_col in result.columns and storage_col != mnemonic:
                rename[storage_col] = mnemonic

        if rename:
            result = result.rename(rename)

        keep = ["SecurityId", "ValueDate"] + [
            m for m in mnemonics if m in result.columns
        ]
        result = result.select(keep)
        parts.append(result)

    if not parts:
        return pl.DataFrame()

    out = parts[0]
    for part in parts[1:]:
        out = out.join(part, on=["SecurityId", "ValueDate"], how="full", coalesce=True)
    return out


def _query_cdh_long(
    store: Store,
    sec_ids: list[int],
    fld_spec: pl.DataFrame,
    start_dt: date,
    end_dt: date,
    date_mode: str,
) -> pl.DataFrame:
    ids_str = ", ".join(str(i) for i in sec_ids)
    tables = fld_spec["StorageTable"].unique().to_list()
    parts: list[pl.DataFrame] = []

    for tbl in tables:
        tbl_flds = fld_spec.filter(pl.col("StorageTable") == tbl)
        if tbl != "FieldSnapshot":
            continue

        fld_ids_str = ", ".join(str(i) for i in tbl_flds["FieldId"].to_list())

        if date_mode == "as_of":
            sql = f"""
                WITH ranked AS (
                    SELECT SecurityId, FieldId, ValueDate,
                           ValChr, ValDbl, ValInt, ValDate,
                           ROW_NUMBER() OVER (
                               PARTITION BY SecurityId, FieldId, ValueDate
                               ORDER BY AsOfDate DESC
                           ) AS rn
                    FROM FieldSnapshot
                    WHERE SecurityId IN ({ids_str})
                      AND FieldId IN ({fld_ids_str})
                      AND ValueDate BETWEEN '{start_dt}' AND '{end_dt}'
                )
                SELECT SecurityId, FieldId, ValueDate, ValChr, ValDbl, ValInt, ValDate
                FROM ranked WHERE rn = 1
                ORDER BY SecurityId, ValueDate
            """
        else:
            sql = f"""
                SELECT SecurityId, FieldId, ValueDate, ValChr, ValDbl, ValInt, ValDate
                FROM FieldSnapshot
                WHERE SecurityId IN ({ids_str})
                  AND FieldId IN ({fld_ids_str})
                  AND ValueDate BETWEEN '{start_dt}' AND '{end_dt}'
                  AND LastFlag = TRUE
                ORDER BY SecurityId, ValueDate
            """

        result = store._backend.query(sql)
        if result.is_empty():
            continue

        fld_info = tbl_flds.select(["FieldId", "FieldMnemonic", "DataType"])
        result = result.join(fld_info, on="FieldId", how="left")
        result = result.with_columns(
            pl.when(pl.col("DataType") == "chr")
            .then(pl.col("ValChr"))
            .when(pl.col("DataType") == "dbl")
            .then(pl.col("ValDbl").cast(pl.String))
            .when(pl.col("DataType") == "int")
            .then(pl.col("ValInt").cast(pl.String))
            .when(pl.col("DataType") == "date")
            .then(pl.col("ValDate").cast(pl.String))
            .otherwise(pl.col("ValChr"))
            .alias("Value")
        )
        pivoted = result.pivot(
            on="FieldMnemonic",
            index=["SecurityId", "ValueDate"],
            values="Value",
            aggregate_function="first",
        )
        parts.append(pivoted)

    if not parts:
        return pl.DataFrame()

    out = parts[0]
    for part in parts[1:]:
        out = out.join(part, on=["SecurityId", "ValueDate"], how="full", coalesce=True)
    return out


def _apply_cdh_fx(
    store: Store,
    dat: pl.DataFrame,
    flds: list[str],
    fx: str,
    start_dt: date,
    end_dt: date,
) -> pl.DataFrame:
    sec_ids = dat["SecurityId"].unique().to_list()
    ids_str = ", ".join(str(i) for i in sec_ids)

    sec_ccy = store._backend.query(
        f"SELECT Id AS SecurityId, Currency FROM SecurityMaster WHERE Id IN ({ids_str})"
    )
    unique_ccys = sec_ccy["Currency"].unique().to_list()
    ccys_need_rate = [c for c in unique_ccys if c != fx]
    if not ccys_need_rate:
        return dat

    fx_tickers = [f"{c}USD" for c in ccys_need_rate]
    if fx != "USD":
        fx_tickers.append(f"{fx}USD")

    tickers_str = ", ".join(f"'{t}'" for t in fx_tickers)
    fx_rates = store._backend.query(f"""
        SELECT sm.Ticker, p.ValueDate, p.PxClose AS FxRate
        FROM Pricing p
        JOIN SecurityMaster sm ON p.SecurityId = sm.Id
        WHERE sm.SecurityType = 'FX'
          AND sm.Ticker IN ({tickers_str})
          AND p.ValueDate BETWEEN '{start_dt}' AND '{end_dt}'
    """)

    if fx_rates.is_empty():
        return dat

    fx_rates = fx_rates.with_columns(
        pl.col("Ticker").str.replace("USD$", "", literal=False).alias("Ccy")
    )

    if fx == "USD":
        cross = fx_rates.select(
            ["Ccy", "ValueDate", pl.col("FxRate").alias("CrossRate")]
        )
    else:
        local_rates = fx_rates.filter(pl.col("Ccy") == fx).select(
            ["ValueDate", pl.col("FxRate").alias("LocalPerUsd")]
        )
        fx_rates = fx_rates.join(local_rates, on="ValueDate", how="left")
        fx_rates = fx_rates.with_columns(
            (pl.col("FxRate") / pl.col("LocalPerUsd")).alias("CrossRate")
        )
        cross = fx_rates.select(["Ccy", "ValueDate", "CrossRate"])

    dat = dat.join(sec_ccy, on="SecurityId", how="left")
    dat = dat.join(
        cross,
        left_on=["Currency", "ValueDate"],
        right_on=["Ccy", "ValueDate"],
        how="left",
    )
    dat = dat.with_columns(
        pl.when(pl.col("Currency") == fx)
        .then(pl.lit(1.0))
        .otherwise(pl.col("CrossRate"))
        .alias("CrossRate")
    )

    fld_defs = store._fields.all.filter(
        pl.col("FieldMnemonic").is_in(flds) & pl.col("FxMode").is_not_null()
    )
    for row in fld_defs.to_dicts():
        col = row["FieldMnemonic"]
        fx_mode = row["FxMode"]
        if col not in dat.columns:
            continue
        if fx_mode == "money":
            dat = dat.with_columns(
                pl.when(pl.col("CrossRate").is_not_null())
                .then(pl.col(col) / pl.col("CrossRate"))
                .otherwise(pl.col(col))
                .alias(col)
            )
        elif fx_mode == "return":
            dat = dat.with_columns(
                pl.when(pl.col("CrossRate").is_not_null())
                .then((pl.col(col) + 1) / (pl.col("CrossRate") + 1) - 1)
                .otherwise(pl.col(col))
                .alias(col)
            )

    dat = dat.drop(["Currency", "CrossRate"])
    return dat


def _scale_tseries(
    df: pl.DataFrame,
    period: str,
    exch_code: str,
    days: str,
    partial: bool,
) -> pl.DataFrame:
    """Aggregate daily data to the requested periodicity, keeping last value per period."""
    if df.is_empty():
        return df

    all_dates = df["ValueDate"].to_list()
    min_dt = min(all_dates)
    max_dt = max(all_dates)

    period_dates = seq_date(
        min_dt, max_dt, exch_code=exch_code, period=period, days=days, partial=partial
    )
    if not period_dates:
        return df

    period_dates_sorted = sorted(period_dates)

    def find_period_end(d: date) -> date:
        idx = bisect.bisect_left(period_dates_sorted, d)
        if idx < len(period_dates_sorted):
            return period_dates_sorted[idx]
        return period_dates_sorted[-1]

    period_end_col = [find_period_end(d) for d in df["ValueDate"].to_list()]
    df = df.with_columns(pl.Series("_PeriodEnd", period_end_col, dtype=pl.Date))

    skip_cols = {"SecurityId", "ValueDate", "_PeriodEnd"}
    value_cols = [c for c in df.columns if c not in skip_cols]

    df = df.sort(["SecurityId", "ValueDate"])
    agg_exprs = [pl.col(c).drop_nulls().last().alias(c) for c in value_cols]
    result = df.group_by(["SecurityId", "_PeriodEnd"]).agg(agg_exprs)
    result = result.rename({"_PeriodEnd": "ValueDate"})
    result = result.sort(["SecurityId", "ValueDate"])
    return result


def _fill_cdh_previous(df: pl.DataFrame) -> pl.DataFrame:
    """Forward-fill null values within each security's time series."""
    if df.is_empty():
        return df

    skip_cols = {"SecurityId", "ValueDate"}
    value_cols = [c for c in df.columns if c not in skip_cols]

    df = df.sort(["SecurityId", "ValueDate"])
    fill_exprs = [
        pl.col(c).forward_fill().over("SecurityId").alias(c) for c in value_cols
    ]
    df = df.with_columns(fill_exprs)
    return df
