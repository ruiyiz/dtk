"""Store: main entry point for dtk."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import polars as pl

from dtk._backend import Backend
from dtk._duckdb_backend import DuckDBBackend
from dtk._field_registry import FieldRegistry

if TYPE_CHECKING:
    from dtk._types import DateMode, Days, ExchangeCode, FillMode, IdType, Periodicity


class Store:
    def __init__(self, backend: Backend) -> None:
        self._backend = backend
        self._fields = FieldRegistry(backend)

    @classmethod
    def from_duckdb(cls, path: str, init: bool = True) -> Store:
        backend = DuckDBBackend(path)
        if init:
            backend.init_schema()
        return cls(backend)

    # ------------------------------------------------------------------
    # Point-in-time queries
    # ------------------------------------------------------------------

    def cdp(
        self,
        x: list[str] | list[int],
        flds: list[str],
        dt: date | None = None,
        date_mode: str = "as_of",
        fill: str = "NA",
        fx: str | None = None,
        id_type: str = "ticker",
        overrides: bool = False,
    ) -> pl.DataFrame:
        from dtk.cdp import cdp

        return cdp(
            self,
            x,
            flds,
            dt=dt,
            date_mode=date_mode,
            fill=fill,
            fx=fx,
            id_type=id_type,
            overrides=overrides,
        )

    # ------------------------------------------------------------------
    # Historical time series queries
    # ------------------------------------------------------------------

    def cdh(
        self,
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
        from dtk.cdh import cdh

        return cdh(
            self,
            x,
            flds,
            start_dt=start_dt,
            end_dt=end_dt,
            date_mode=date_mode,
            exch_code=exch_code,
            period=period,
            days=days,
            fill=fill,
            fx=fx,
            id_type=id_type,
            overrides=overrides,
        )

    # ------------------------------------------------------------------
    # Data set queries
    # ------------------------------------------------------------------

    def cds(
        self,
        x: list[str] | list[int],
        dataset: str,
        start_dt: date | None = None,
        end_dt: date | None = None,
        id_type: str = "ticker",
        **kwargs,
    ) -> pl.DataFrame:
        from dtk.cds import cds

        return cds(
            self,
            x,
            dataset,
            start_dt=start_dt,
            end_dt=end_dt,
            id_type=id_type,
            **kwargs,
        )

    def cds_dividend(
        self,
        x: list[str] | list[int],
        start_dt: date | None = None,
        end_dt: date | None = None,
        dividend_type: str | None = None,
        special_only: bool = False,
        id_type: str = "ticker",
    ) -> pl.DataFrame:
        from dtk.cds import cds_dividend

        return cds_dividend(
            self,
            x,
            start_dt=start_dt,
            end_dt=end_dt,
            dividend_type=dividend_type,
            special_only=special_only,
            id_type=id_type,
        )

    def cds_event(
        self,
        x: list[str] | list[int],
        start_dt: date | None = None,
        end_dt: date | None = None,
        event_type: str | int | None = None,
        status: str = "Active",
        id_type: str = "ticker",
    ) -> pl.DataFrame:
        from dtk.cds import cds_event

        return cds_event(
            self,
            x,
            start_dt=start_dt,
            end_dt=end_dt,
            event_type=event_type,
            status=status,
            id_type=id_type,
        )

    def cds_adjfactor(
        self,
        x: list[str] | list[int],
        start_dt: date | None = None,
        end_dt: date | None = None,
        adj_type: str | None = None,
        id_type: str = "ticker",
    ) -> pl.DataFrame:
        from dtk.cds import cds_adjfactor

        return cds_adjfactor(
            self,
            x,
            start_dt=start_dt,
            end_dt=end_dt,
            adj_type=adj_type,
            id_type=id_type,
        )

    # ------------------------------------------------------------------
    # Data upload
    # ------------------------------------------------------------------

    def cdu(
        self,
        df: pl.DataFrame,
        flds: list[str] | None = None,
        ignore_older: bool = False,
        check_diff: bool = True,
    ) -> dict[str, int]:
        from dtk.cdu import cdu

        return cdu(
            self, df, flds=flds, ignore_older=ignore_older, check_diff=check_diff
        )

    def cdu_pricing(self, df: pl.DataFrame) -> int:
        from dtk.cdu import cdu_pricing

        return cdu_pricing(self, df)

    def cdu_security(self, df: pl.DataFrame) -> int:
        from dtk.cdu import cdu_security

        return cdu_security(self, df)

    def cdu_dataset(self, df: pl.DataFrame, dataset: str) -> int:
        from dtk.cdu import cdu_dataset

        return cdu_dataset(self, df, dataset)

    def cdu_override(
        self,
        security_id: int,
        field: str,
        value,
        value_date: date | None = None,
        reason: str | None = None,
        created_by: str | None = None,
    ) -> None:
        from dtk.cdu import cdu_override

        cdu_override(
            self,
            security_id,
            field,
            value,
            value_date=value_date,
            reason=reason,
            created_by=created_by,
        )
