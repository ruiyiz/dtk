"""Type conversion utilities: polars <-> storage formats."""

from __future__ import annotations

import json
from datetime import date
from typing import Any

import polars as pl


def to_storage_value(value: Any, dtype: str) -> dict[str, Any]:
    """Convert a Python value to a dict of DuckDB long-format val columns."""
    result = {"ValChr": None, "ValDbl": None, "ValInt": None, "ValDate": None}
    if value is None:
        return result
    if dtype == "chr":
        result["ValChr"] = str(value)
    elif dtype == "dbl":
        result["ValDbl"] = float(value)
    elif dtype == "int":
        result["ValInt"] = int(value)
    elif dtype == "date":
        if isinstance(value, str):
            result["ValDate"] = date.fromisoformat(value)
        else:
            result["ValDate"] = value
    elif dtype == "json":
        result["ValChr"] = json.dumps(value) if not isinstance(value, str) else value
    return result


def from_storage_row(row: dict[str, Any], dtype: str) -> Any:
    """Extract the typed value from a long-format row dict."""
    if dtype == "chr":
        return row.get("ValChr")
    elif dtype == "dbl":
        return row.get("ValDbl")
    elif dtype == "int":
        return row.get("ValInt")
    elif dtype == "date":
        return row.get("ValDate")
    elif dtype == "json":
        val = row.get("ValJson") or row.get("ValChr")
        if val is None:
            return None
        try:
            return json.loads(val)
        except (json.JSONDecodeError, TypeError):
            return val
    return row.get("ValChr")


def cast_series(s: pl.Series, dtype: str) -> pl.Series:
    """Cast a Polars series to the target dtk dtype."""
    if dtype == "chr":
        return s.cast(pl.String)
    elif dtype == "dbl":
        return s.cast(pl.Float64)
    elif dtype == "int":
        return s.cast(pl.Int64)
    elif dtype == "date":
        if s.dtype == pl.String:
            return s.str.to_date()
        return s.cast(pl.Date)
    elif dtype == "lgl":
        return s.cast(pl.Boolean)
    return s


def apply_type_map(df: pl.DataFrame, type_map: dict[str, str]) -> pl.DataFrame:
    """Cast DataFrame columns according to a {mnemonic: dtype} map."""
    casts = []
    for col, dtype in type_map.items():
        if col in df.columns:
            casts.append(cast_series(df[col], dtype).alias(col))
    if casts:
        return df.with_columns(casts)
    return df


def build_type_map(field_defs: pl.DataFrame) -> dict[str, str]:
    """Build {FieldMnemonic: DataType} map from a FieldDef slice."""
    return dict(
        zip(
            field_defs["FieldMnemonic"].to_list(),
            field_defs["DataType"].to_list(),
        )
    )


def dates_to_str(df: pl.DataFrame) -> pl.DataFrame:
    """Convert all Date columns to ISO string for DuckDB insertion."""
    casts = [
        pl.col(c).cast(pl.String).alias(c) for c in df.columns if df[c].dtype == pl.Date
    ]
    return df.with_columns(casts) if casts else df
