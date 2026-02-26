from __future__ import annotations

import importlib.resources
from typing import Any

import duckdb
import polars as pl

from dtk.errors import DtkConnectionError, DtkSchemaError


class DuckDBBackend:
    def __init__(self, path: str) -> None:
        try:
            self._conn = duckdb.connect(path)
        except Exception as e:
            raise DtkConnectionError(
                f"Failed to connect to DuckDB at {path!r}: {e}"
            ) from e

    def query(self, sql: str, params: Any = None) -> pl.DataFrame:
        try:
            if params is not None:
                cur = self._conn.execute(sql, params)
            else:
                cur = self._conn.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            if not rows:
                return pl.DataFrame({c: [] for c in cols})
            return pl.DataFrame(dict(zip(cols, zip(*rows))))
        except Exception as e:
            raise DtkSchemaError(f"Query failed: {e}\nSQL: {sql}") from e

    def execute(self, sql: str, params: Any = None) -> int:
        try:
            if params is not None:
                result = self._conn.execute(sql, params)
            else:
                result = self._conn.execute(sql)
            return result.rowcount if result.rowcount is not None else 0
        except Exception as e:
            raise DtkSchemaError(f"Execute failed: {e}\nSQL: {sql}") from e

    def init_schema(self) -> None:
        sql_path = importlib.resources.files("dtk.sql").joinpath("init.sql")
        sql_text = sql_path.read_text()
        for raw_stmt in sql_text.split(";"):
            # Strip comment lines, leaving only SQL lines
            lines = [
                line
                for line in raw_stmt.splitlines()
                if line.strip() and not line.strip().startswith("--")
            ]
            stmt = "\n".join(lines).strip()
            if not stmt:
                continue
            try:
                self._conn.execute(stmt)
            except Exception as e:
                msg = str(e).lower()
                if "already exists" not in msg and "duplicate" not in msg:
                    raise DtkSchemaError(
                        f"Schema init failed on: {stmt[:60]}...\n{e}"
                    ) from e

    def table_exists(self, name: str) -> bool:
        sql = (
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_schema = 'main' AND table_name = ?"
        )
        result = self._conn.execute(sql, [name]).fetchone()
        return result is not None

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> DuckDBBackend:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
