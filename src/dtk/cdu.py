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
        If True, don't set LastFlag=False on existing rows before inserting.
    check_diff:
        If True, skip rows identical to existing data.

    Returns
    -------
    dict with keys "wide" and "long" showing rows uploaded to each storage type.
    """
    df = _validate_input(store, df)

    if flds is None:
        skip = {"SecurityId", "Ticker", "ValueDate", "AsOfDate"}
        flds = [c for c in df.columns if c not in skip]

    fld_map = store._fields.map_to_storage(flds)
    results = {"wide": 0, "long": 0}

    if "wide" in fld_map:
        results["wide"] = _save_wide(store, df, fld_map["wide"])

    if "long" in fld_map:
        results["long"] = _save_long(
            store, df, fld_map["long"], ignore_older, check_diff
        )

    return results


def _validate_input(store: Store, df: pl.DataFrame) -> pl.DataFrame:
    if "SecurityId" not in df.columns:
        if "Ticker" not in df.columns:
            raise DtkValueError("Input must have SecurityId or Ticker column")
        secs = lookup_securities(
            store, df["Ticker"].to_list(), id_type="ticker", include_inactive=True
        )
        ticker_to_id = dict(zip(secs["Ticker"].to_list(), secs["Id"].to_list()))
        id_values = [ticker_to_id.get(t) for t in df["Ticker"].to_list()]
        df = df.with_columns(pl.Series("SecurityId", id_values, dtype=pl.Int64))

    if "ValueDate" not in df.columns:
        raise DtkValueError("Input must have ValueDate column")

    if df["ValueDate"].dtype != pl.Date:
        df = df.with_columns(pl.col("ValueDate").cast(pl.Date))

    if "AsOfDate" not in df.columns:
        df = df.with_columns(pl.lit(date.today()).alias("AsOfDate").cast(pl.Date))
    elif df["AsOfDate"].dtype != pl.Date:
        df = df.with_columns(pl.col("AsOfDate").cast(pl.Date))

    return df


def _save_wide(store: Store, df: pl.DataFrame, fld_spec: pl.DataFrame) -> int:
    tables = fld_spec["StorageTable"].unique().to_list()
    total = 0

    for tbl in tables:
        tbl_flds = fld_spec.filter(pl.col("StorageTable") == tbl)
        mnemonics = [m for m in tbl_flds["FieldMnemonic"].to_list() if m in df.columns]
        if not mnemonics:
            continue

        upload = df.select(["SecurityId", "ValueDate"] + mnemonics)
        rename = {m: WIDE_COLUMN_MAP[m] for m in mnemonics if m in WIDE_COLUMN_MAP}
        if rename:
            upload = upload.rename(rename)

        total += _upsert_wide(store, upload, tbl)

    return total


def _upsert_wide(store: Store, df: pl.DataFrame, table_name: str) -> int:
    """Bulk upsert a DataFrame into a wide-format table."""
    if df.is_empty():
        return 0

    cols = df.columns
    cols_quoted = ", ".join(f'"{c}"' for c in cols)
    update_cols = [c for c in cols if c not in ("SecurityId", "ValueDate")]
    placeholders = ", ".join(["?"] * len(cols))

    if update_cols:
        set_clause = ", ".join(f'"{c}" = excluded."{c}"' for c in update_cols)
        sql = (
            f'INSERT INTO "{table_name}" ({cols_quoted}) VALUES ({placeholders}) '
            f"ON CONFLICT (SecurityId, ValueDate) DO UPDATE SET {set_clause}"
        )
    else:
        sql = (
            f'INSERT INTO "{table_name}" ({cols_quoted}) VALUES ({placeholders}) '
            f"ON CONFLICT (SecurityId, ValueDate) DO NOTHING"
        )

    rows = [list(row.values()) for row in df.to_dicts()]
    store._backend._conn.executemany(sql, rows)
    return len(df)


def _save_long(
    store: Store,
    df: pl.DataFrame,
    fld_spec: pl.DataFrame,
    ignore_older: bool,
    check_diff: bool,
) -> int:
    melted = _melt_to_long(df, fld_spec)
    if melted.is_empty():
        return 0

    if check_diff:
        melted = _remove_duplicates(store, melted)

    if melted.is_empty():
        return 0

    return _upsert_long(store, melted, ignore_older)


def _melt_to_long(df: pl.DataFrame, fld_spec: pl.DataFrame) -> pl.DataFrame:
    measure_vars = [m for m in fld_spec["FieldMnemonic"].to_list() if m in df.columns]
    if not measure_vars:
        return pl.DataFrame()

    id_vars = ["SecurityId", "ValueDate", "AsOfDate"]
    upload = df.select(id_vars + measure_vars)
    upload = upload.with_columns([pl.col(m).cast(pl.String) for m in measure_vars])

    melted = upload.unpivot(
        on=measure_vars,
        index=id_vars,
        variable_name="FieldMnemonic",
        value_name="Value",
    )

    fld_info = fld_spec.select(["FieldMnemonic", "FieldId", "DataType"])
    melted = melted.join(fld_info, on="FieldMnemonic", how="left")

    melted = melted.with_columns(
        [
            pl.when(pl.col("DataType") == "chr").then(pl.col("Value")).alias("ValChr"),
            pl.when(pl.col("DataType") == "dbl")
            .then(pl.col("Value").cast(pl.Float64, strict=False))
            .alias("ValDbl"),
            pl.when(pl.col("DataType") == "int")
            .then(pl.col("Value").cast(pl.Int64, strict=False))
            .alias("ValInt"),
            pl.when(pl.col("DataType") == "date")
            .then(pl.col("Value").str.to_date("%Y-%m-%d", strict=False))
            .alias("ValDate"),
            pl.lit(True).alias("LastFlag"),
        ]
    )

    return melted.select(
        [
            "SecurityId",
            "FieldId",
            "ValueDate",
            "AsOfDate",
            "LastFlag",
            "ValChr",
            "ValDbl",
            "ValInt",
            "ValDate",
        ]
    )


def _remove_duplicates(store: Store, melted: pl.DataFrame) -> pl.DataFrame:
    """Drop rows whose values are identical to what's already stored (LastFlag=TRUE)."""
    sec_ids = melted["SecurityId"].unique().to_list()
    fld_ids = melted["FieldId"].unique().to_list()
    value_dates = melted["ValueDate"].unique().to_list()

    ids_str = ", ".join(str(i) for i in sec_ids)
    fld_ids_str = ", ".join(str(i) for i in fld_ids)
    dates_str = ", ".join(f"'{d}'" for d in value_dates)

    sql = f"""
        SELECT SecurityId, FieldId, ValueDate, ValChr, ValDbl, ValInt, ValDate
        FROM FieldSnapshot
        WHERE SecurityId IN ({ids_str})
          AND FieldId IN ({fld_ids_str})
          AND ValueDate IN ({dates_str})
          AND LastFlag = TRUE
    """
    existing = store._backend.query(sql)
    if existing.is_empty():
        return melted

    existing = existing.with_columns(
        pl.coalesce(
            [
                pl.col("ValDbl").cast(pl.String),
                pl.col("ValInt").cast(pl.String),
                pl.col("ValDate").cast(pl.String),
                pl.col("ValChr"),
            ]
        ).alias("ExistingVal")
    )
    melted = melted.with_columns(
        pl.coalesce(
            [
                pl.col("ValDbl").cast(pl.String),
                pl.col("ValInt").cast(pl.String),
                pl.col("ValDate").cast(pl.String),
                pl.col("ValChr"),
            ]
        ).alias("NewVal")
    )

    merged = melted.join(
        existing.select(["SecurityId", "FieldId", "ValueDate", "ExistingVal"]),
        on=["SecurityId", "FieldId", "ValueDate"],
        how="left",
    )
    changed = merged.filter(
        pl.col("ExistingVal").is_null() | (pl.col("ExistingVal") != pl.col("NewVal"))
    )
    return changed.drop(["ExistingVal", "NewVal"])


def _upsert_long(store: Store, df: pl.DataFrame, ignore_older: bool) -> int:
    """Insert new long-format rows, marking prior revisions LastFlag=FALSE first."""
    if not ignore_older:
        sec_ids = df["SecurityId"].unique().to_list()
        fld_ids = df["FieldId"].unique().to_list()
        ids_str = ", ".join(str(i) for i in sec_ids)
        fld_ids_str = ", ".join(str(i) for i in fld_ids)
        store._backend.execute(f"""
            UPDATE FieldSnapshot SET LastFlag = FALSE
            WHERE SecurityId IN ({ids_str})
              AND FieldId IN ({fld_ids_str})
              AND LastFlag = TRUE
        """)

    cols = [
        "SecurityId",
        "FieldId",
        "ValueDate",
        "AsOfDate",
        "LastFlag",
        "ValChr",
        "ValDbl",
        "ValInt",
        "ValDate",
    ]
    upload = df.select(cols)
    cols_quoted = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join(["?"] * len(cols))

    sql = (
        f"INSERT INTO FieldSnapshot ({cols_quoted}) VALUES ({placeholders}) "
        f"ON CONFLICT (SecurityId, FieldId, ValueDate, AsOfDate) DO UPDATE SET "
        f'"LastFlag" = excluded."LastFlag", '
        f'"ValChr" = excluded."ValChr", '
        f'"ValDbl" = excluded."ValDbl", '
        f'"ValInt" = excluded."ValInt", '
        f'"ValDate" = excluded."ValDate"'
    )
    rows = [list(row.values()) for row in upload.to_dicts()]
    store._backend._conn.executemany(sql, rows)
    return len(df)


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
    pricing_cols = [
        "SecurityId",
        "ValueDate",
        "PxClose",
        "PxHigh",
        "PxLow",
        "PxOpen",
        "PxLast",
        "Volume",
        "NavClose",
        "NavLast",
        "TotalReturn",
        "DividendAmount",
        "AdjFactor",
    ]

    if "SecurityId" not in df.columns and "Ticker" in df.columns:
        secs = lookup_securities(
            store, df["Ticker"].to_list(), id_type="ticker", include_inactive=True
        )
        ticker_to_id = dict(zip(secs["Ticker"].to_list(), secs["Id"].to_list()))
        id_values = [ticker_to_id.get(t) for t in df["Ticker"].to_list()]
        df = df.with_columns(pl.Series("SecurityId", id_values, dtype=pl.Int64))

    available = [c for c in pricing_cols if c in df.columns]
    if len(available) <= 2:
        raise DtkValueError("No pricing columns found in input")

    upload = df.select(available)
    return _upsert_wide(store, upload, "Pricing")


def cdu_security(store: Store, df: pl.DataFrame) -> int:
    """Upload security master records (upsert to SecurityMaster table).

    Parameters
    ----------
    store:
        The Store instance.
    df:
        Must contain Ticker and SecurityType columns. Include Id to upsert
        existing records; omit Id to insert new ones.

    Returns
    -------
    Number of rows upserted.
    """
    required = {"Ticker", "SecurityType"}
    missing = required - set(df.columns)
    if missing:
        raise DtkValueError(f"Missing required columns: {', '.join(sorted(missing))}")

    cols = df.columns
    cols_quoted = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join(["?"] * len(cols))

    if "Id" in cols:
        update_cols = [c for c in cols if c != "Id"]
        set_clause = ", ".join(f'"{c}" = excluded."{c}"' for c in update_cols)
        sql = (
            f"INSERT INTO SecurityMaster ({cols_quoted}) VALUES ({placeholders}) "
            f"ON CONFLICT (Id) DO UPDATE SET {set_clause}"
        )
    else:
        sql = f"INSERT INTO SecurityMaster ({cols_quoted}) VALUES ({placeholders})"

    rows = [list(row.values()) for row in df.to_dicts()]
    store._backend._conn.executemany(sql, rows)
    return len(df)


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

    set_def = DATA_SETS[dataset]

    if "SecurityId" not in df.columns:
        if "Ticker" not in df.columns:
            raise DtkValueError("Input must have SecurityId or Ticker column")
        secs = lookup_securities(
            store, df["Ticker"].to_list(), id_type="ticker", include_inactive=True
        )
        ticker_to_id = dict(zip(secs["Ticker"].to_list(), secs["Id"].to_list()))
        id_values = [ticker_to_id.get(t) for t in df["Ticker"].to_list()]
        df = df.with_columns(pl.Series("SecurityId", id_values, dtype=pl.Int64))

    pk_map = {"dividend": "DividendId", "event": "EventId", "adjfactor": "AdjFactorId"}
    pk_col = pk_map.get(dataset)

    cols = df.columns
    cols_quoted = ", ".join(f'"{c}"' for c in cols)
    placeholders = ", ".join(["?"] * len(cols))

    if pk_col and pk_col in cols:
        update_cols = [c for c in cols if c != pk_col]
        set_clause = ", ".join(f'"{c}" = excluded."{c}"' for c in update_cols)
        sql = (
            f'INSERT INTO "{set_def.table}" ({cols_quoted}) VALUES ({placeholders}) '
            f"ON CONFLICT ({pk_col}) DO UPDATE SET {set_clause}"
        )
    else:
        sql = f'INSERT INTO "{set_def.table}" ({cols_quoted}) VALUES ({placeholders})'

    rows = [list(row.values()) for row in df.to_dicts()]
    store._backend._conn.executemany(sql, rows)
    return len(df)


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
    fld = store._fields.get(field)
    if fld is None:
        raise DtkFieldNotFoundError(f"Unknown field: {field!r}")

    if value_date is None:
        value_date = date.today()

    fld_id = fld["FieldId"]
    dtype = fld["DataType"]

    val_chr = str(value) if dtype == "chr" else None
    val_dbl = float(value) if dtype == "dbl" else None
    val_int = int(value) if dtype == "int" else None
    val_date = (
        (date.fromisoformat(value) if isinstance(value, str) else value)
        if dtype == "date"
        else None
    )

    sql = """
        INSERT INTO FieldOverride
          (SecurityId, FieldId, ValueDate, ValChr, ValDbl, ValInt, ValDate, Reason, CreatedBy)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (SecurityId, FieldId, ValueDate) DO UPDATE SET
          ValChr = excluded.ValChr,
          ValDbl = excluded.ValDbl,
          ValInt = excluded.ValInt,
          ValDate = excluded.ValDate,
          Reason = excluded.Reason,
          CreatedBy = excluded.CreatedBy
    """
    store._backend.execute(
        sql,
        [
            security_id,
            fld_id,
            value_date,
            val_chr,
            val_dbl,
            val_int,
            val_date,
            reason,
            created_by,
        ],
    )
