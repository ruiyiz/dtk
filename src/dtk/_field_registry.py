from __future__ import annotations

from typing import TYPE_CHECKING

import polars as pl

from dtk.errors import DtkFieldNotFoundError

if TYPE_CHECKING:
    from dtk._backend import Backend


class FieldRegistry:
    def __init__(self, backend: Backend) -> None:
        self._defs: pl.DataFrame = backend.query("SELECT * FROM FieldDef")
        self._by_mnemonic: dict[str, dict] = {
            row["FieldMnemonic"]: row for row in self._defs.to_dicts()
        }

    def get(self, mnemonic: str) -> dict | None:
        return self._by_mnemonic.get(mnemonic)

    def get_id(self, mnemonic: str) -> int:
        rec = self._by_mnemonic.get(mnemonic)
        if rec is None:
            raise DtkFieldNotFoundError(f"Unknown field: {mnemonic!r}")
        return rec["FieldId"]

    def for_function(self, fn: str) -> pl.DataFrame:
        col_map = {"cdp": "IsCdp", "cdh": "IsCdh", "cds": "IsCds", "cdu": "IsCdu"}
        col = col_map.get(fn.lower())
        if col is None:
            raise DtkFieldNotFoundError(f"Unknown function: {fn!r}")
        return self._defs.filter(pl.col(col))

    def map_to_storage(self, flds: list[str]) -> dict[str, pl.DataFrame]:
        sub = self._defs.filter(pl.col("FieldMnemonic").is_in(flds))
        result: dict[str, pl.DataFrame] = {}
        for mode, group in sub.group_by("StorageMode"):
            result[mode[0]] = group
        return result

    def build_type_map(self, flds: list[str]) -> dict[str, str]:
        sub = self._defs.filter(pl.col("FieldMnemonic").is_in(flds))
        return dict(zip(sub["FieldMnemonic"].to_list(), sub["DataType"].to_list()))

    @property
    def all(self) -> pl.DataFrame:
        return self._defs
