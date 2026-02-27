"""Point-in-time data access."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import polars as pl

from dtk._constants import WIDE_COLUMN_MAP
from dtk.convert import apply_type_map
from dtk.date_utils import prev_date
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
        Snapshot date. Defaults to previous business day.
    date_mode:
        "as_of" (latest available up to dt) or "as_seen" (exactly dt, LastFlag=TRUE).
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
    if dt is None:
        dt = prev_date(date.today())

    secs = lookup_securities(store, x, id_type=id_type, dt=dt)
    if secs.is_empty():
        return pl.DataFrame()

    sec_ids = secs["Id"].to_list()
    fld_map = store._fields.map_to_storage(flds)

    results: dict[str, pl.DataFrame] = {}

    if "wide" in fld_map:
        wide_result = _raw_data_wide(store, sec_ids, fld_map["wide"], dt)
        if not wide_result.is_empty():
            results["wide"] = wide_result

    if "long" in fld_map:
        long_result = _raw_data_long(store, sec_ids, fld_map["long"], dt, date_mode)
        if not long_result.is_empty():
            results["long"] = long_result

    out = pl.DataFrame({"SecurityId": sec_ids})
    for part in results.values():
        out = out.join(part, on="SecurityId", how="left")

    if overrides:
        out = _apply_overrides(store, out, flds, dt)

    if fx is not None:
        out = _apply_fx_conversion(store, out, flds, fx, dt)

    if fill == "P":
        out = _fill_previous(store, out, sec_ids, flds, dt)

    type_map = store._fields.build_type_map(flds)
    out = apply_type_map(out, type_map)

    ticker_df = secs.select(["Id", "Ticker"])
    out = out.join(ticker_df, left_on="SecurityId", right_on="Id", how="left")

    ordered_cols = ["SecurityId", "Ticker"] + [f for f in flds if f in out.columns]
    extra = [c for c in out.columns if c not in ordered_cols]
    out = out.select(ordered_cols + extra)
    return out


def _raw_data_wide(
    store: Store,
    sec_ids: list[int],
    fld_spec: pl.DataFrame,
    dt: date,
) -> pl.DataFrame:
    """Query wide-format tables for a list of securities at a single date."""
    ids_str = ", ".join(str(i) for i in sec_ids)
    tables = fld_spec["StorageTable"].unique().to_list()
    parts: list[pl.DataFrame] = []

    for tbl in tables:
        tbl_flds = fld_spec.filter(pl.col("StorageTable") == tbl)
        mnemonics = tbl_flds["FieldMnemonic"].to_list()

        sql = f"SELECT * FROM {tbl} WHERE SecurityId IN ({ids_str}) AND ValueDate = '{dt}'"
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

        keep = ["SecurityId"] + [m for m in mnemonics if m in result.columns]
        result = result.select(keep)
        parts.append(result)

    if not parts:
        return pl.DataFrame()

    out = parts[0]
    for part in parts[1:]:
        out = out.join(part, on="SecurityId", how="full", coalesce=True)
    return out


def _raw_data_long(
    store: Store,
    sec_ids: list[int],
    fld_spec: pl.DataFrame,
    dt: date,
    date_mode: str,
) -> pl.DataFrame:
    """Query long-format tables (FieldSnapshot/SecuritySnapshot) at a single date."""
    ids_str = ", ".join(str(i) for i in sec_ids)
    tables = fld_spec["StorageTable"].unique().to_list()
    parts: list[pl.DataFrame] = []

    for tbl in tables:
        tbl_flds = fld_spec.filter(pl.col("StorageTable") == tbl)

        if tbl == "FieldSnapshot":
            fld_ids_str = ", ".join(str(i) for i in tbl_flds["FieldId"].to_list())
            if date_mode == "as_of":
                sql = f"""
                    WITH ranked AS (
                        SELECT SecurityId, FieldId, ValChr, ValDbl, ValInt, ValDate,
                               ROW_NUMBER() OVER (
                                   PARTITION BY SecurityId, FieldId ORDER BY AsOfDate DESC
                               ) AS rn
                        FROM FieldSnapshot
                        WHERE SecurityId IN ({ids_str})
                          AND FieldId IN ({fld_ids_str})
                          AND ValueDate <= '{dt}'
                          AND AsOfDate <= '{dt}'
                    )
                    SELECT SecurityId, FieldId, ValChr, ValDbl, ValInt, ValDate
                    FROM ranked WHERE rn = 1
                """
            else:
                sql = f"""
                    SELECT SecurityId, FieldId, ValChr, ValDbl, ValInt, ValDate
                    FROM FieldSnapshot
                    WHERE SecurityId IN ({ids_str})
                      AND FieldId IN ({fld_ids_str})
                      AND ValueDate = '{dt}'
                      AND LastFlag = TRUE
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
                index="SecurityId",
                values="Value",
                aggregate_function="first",
            )
            parts.append(pivoted)

        elif tbl == "SecuritySnapshot":
            attr_names = tbl_flds["FieldMnemonic"].to_list()
            attrs_str = ", ".join(f"'{a}'" for a in attr_names)
            if date_mode == "as_of":
                sql = f"""
                    WITH ranked AS (
                        SELECT SecurityId, AttrName, ValChr, ValDbl, ValInt, ValDate, ValJson,
                               ROW_NUMBER() OVER (
                                   PARTITION BY SecurityId, AttrName ORDER BY AsOfDate DESC
                               ) AS rn
                        FROM SecuritySnapshot
                        WHERE SecurityId IN ({ids_str})
                          AND AttrName IN ({attrs_str})
                          AND ValueDate <= '{dt}'
                          AND AsOfDate <= '{dt}'
                    )
                    SELECT SecurityId, AttrName, ValChr, ValDbl, ValInt, ValDate, ValJson
                    FROM ranked WHERE rn = 1
                """
            else:
                sql = f"""
                    SELECT SecurityId, AttrName, ValChr, ValDbl, ValInt, ValDate, ValJson
                    FROM SecuritySnapshot
                    WHERE SecurityId IN ({ids_str})
                      AND AttrName IN ({attrs_str})
                      AND ValueDate = '{dt}'
                      AND LastFlag = TRUE
                """
            result = store._backend.query(sql)
            if result.is_empty():
                continue

            result = result.with_columns(
                pl.coalesce(
                    [
                        pl.col("ValDbl").cast(pl.String),
                        pl.col("ValInt").cast(pl.String),
                        pl.col("ValDate").cast(pl.String),
                        pl.col("ValJson"),
                        pl.col("ValChr"),
                    ]
                ).alias("Value")
            )
            pivoted = result.pivot(
                on="AttrName",
                index="SecurityId",
                values="Value",
                aggregate_function="first",
            )
            parts.append(pivoted)

    if not parts:
        return pl.DataFrame()

    out = parts[0]
    for part in parts[1:]:
        out = out.join(part, on="SecurityId", how="full", coalesce=True)
    return out


def _apply_overrides(
    store: Store,
    df: pl.DataFrame,
    flds: list[str],
    dt: date,
) -> pl.DataFrame:
    """Apply FieldOverride values to the output DataFrame."""
    fld_ids = [
        store._fields.get_id(f) for f in flds if store._fields.get(f) is not None
    ]
    if not fld_ids:
        return df

    sec_ids = df["SecurityId"].to_list()
    ids_str = ", ".join(str(i) for i in sec_ids)
    fld_ids_str = ", ".join(str(i) for i in fld_ids)

    sql = f"""
        SELECT SecurityId, FieldId, ValChr, ValDbl, ValInt, ValDate
        FROM FieldOverride
        WHERE SecurityId IN ({ids_str})
          AND FieldId IN ({fld_ids_str})
          AND ValueDate = '{dt}'
    """
    overrides = store._backend.query(sql)
    if overrides.is_empty():
        return df

    fld_defs = store._fields.all.select(["FieldId", "FieldMnemonic", "DataType"])
    overrides = overrides.join(fld_defs, on="FieldId", how="left")

    out = df.clone()
    for row in overrides.to_dicts():
        col = row["FieldMnemonic"]
        if col not in out.columns:
            continue
        dtype = row["DataType"]
        if dtype == "chr":
            val = row["ValChr"]
        elif dtype == "dbl":
            val = row["ValDbl"]
        elif dtype == "int":
            val = row["ValInt"]
        elif dtype == "date":
            val = row["ValDate"]
        else:
            val = row["ValChr"]
        sid = row["SecurityId"]
        out = out.with_columns(
            pl.when(pl.col("SecurityId") == sid)
            .then(pl.lit(val))
            .otherwise(pl.col(col))
            .alias(col)
        )
    return out


def _apply_fx_conversion(
    store: Store,
    df: pl.DataFrame,
    flds: list[str],
    fx: str,
    dt: date,
) -> pl.DataFrame:
    """Convert field values from security currency to target FX."""
    sec_ids = df["SecurityId"].to_list()
    ids_str = ", ".join(str(i) for i in sec_ids)

    sec_ccy = store._backend.query(
        f"SELECT Id AS SecurityId, Currency FROM SecurityMaster WHERE Id IN ({ids_str})"
    )
    unique_ccys = sec_ccy["Currency"].unique().to_list()
    ccys_need_rate = [c for c in unique_ccys if c != fx]
    if not ccys_need_rate:
        return df

    fx_tickers = [f"{c}USD" for c in ccys_need_rate]
    if fx != "USD":
        fx_tickers.append(f"{fx}USD")

    tickers_str = ", ".join(f"'{t}'" for t in fx_tickers)
    fx_rates = store._backend.query(f"""
        SELECT sm.Ticker, p.PxClose AS FxRate
        FROM Pricing p
        JOIN SecurityMaster sm ON p.SecurityId = sm.Id
        WHERE sm.SecurityType = 'FX'
          AND sm.Ticker IN ({tickers_str})
          AND p.ValueDate = '{dt}'
    """)

    if fx_rates.is_empty():
        return df

    fx_rates = fx_rates.with_columns(
        pl.col("Ticker").str.replace("USD$", "", literal=False).alias("Ccy")
    )

    if fx == "USD":
        fx_rates = fx_rates.with_columns(pl.col("FxRate").alias("CrossRate"))
    else:
        local_rows = fx_rates.filter(pl.col("Ccy") == fx)
        if local_rows.is_empty():
            return df
        local_per_usd = local_rows["FxRate"][0]
        fx_rates = fx_rates.with_columns(
            (pl.col("FxRate") / local_per_usd).alias("CrossRate")
        )

    out = df.join(sec_ccy, on="SecurityId", how="left")
    cross = fx_rates.select(["Ccy", "CrossRate"])
    out = out.join(cross, left_on="Currency", right_on="Ccy", how="left")
    out = out.with_columns(
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
        if col not in out.columns:
            continue
        if fx_mode == "money":
            out = out.with_columns(
                pl.when(pl.col("CrossRate").is_not_null())
                .then(pl.col(col) / pl.col("CrossRate"))
                .otherwise(pl.col(col))
                .alias(col)
            )
        elif fx_mode == "return":
            out = out.with_columns(
                pl.when(pl.col("CrossRate").is_not_null())
                .then((pl.col(col) + 1) / (pl.col("CrossRate") + 1) - 1)
                .otherwise(pl.col(col))
                .alias(col)
            )

    out = out.drop(["Currency", "CrossRate"])
    return out


def _fill_previous(
    store: Store,
    df: pl.DataFrame,
    sec_ids: list[int],
    flds: list[str],
    dt: date,
) -> pl.DataFrame:
    """Forward-fill null field values from previous date (one step back)."""
    fld_cols_present = [f for f in flds if f in df.columns]
    if not fld_cols_present:
        return df

    null_mask = pl.lit(False)
    for col in fld_cols_present:
        null_mask = null_mask | pl.col(col).is_null()

    na_secs = df.filter(null_mask)["SecurityId"].to_list()
    if not na_secs:
        return df

    prev_dt = prev_date(dt)
    prev_df = cdp(store, na_secs, flds, dt=prev_dt, fill="NA", id_type="id")
    if prev_df.is_empty():
        return df

    prev_vals = prev_df.select(["SecurityId"] + fld_cols_present)
    out = df.clone()
    for col in fld_cols_present:
        fill_col = prev_vals.select(["SecurityId", pl.col(col).alias(f"_fill_{col}")])
        out = out.join(fill_col, on="SecurityId", how="left")
        out = out.with_columns(
            pl.when(pl.col(col).is_null() & pl.col(f"_fill_{col}").is_not_null())
            .then(pl.col(f"_fill_{col}"))
            .otherwise(pl.col(col))
            .alias(col)
        )
        out = out.drop(f"_fill_{col}")
    return out
