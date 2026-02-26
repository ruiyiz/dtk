import polars as pl
import pytest

from dtk._duckdb_backend import DuckDBBackend
from dtk.errors import DtkConnectionError


def test_connect_memory():
    backend = DuckDBBackend(":memory:")
    result = backend.query("SELECT 42 AS n")
    assert result["n"][0] == 42


def test_execute_returns_int():
    backend = DuckDBBackend(":memory:")
    backend.execute("CREATE TABLE t (x INTEGER)")
    n = backend.execute("INSERT INTO t VALUES (1), (2)")
    assert isinstance(n, int)


def test_table_exists_false():
    backend = DuckDBBackend(":memory:")
    assert not backend.table_exists("NonExistentTable")


def test_table_exists_after_create():
    backend = DuckDBBackend(":memory:")
    backend.execute("CREATE TABLE foo (id INTEGER)")
    assert backend.table_exists("foo")


def test_init_schema_creates_tables():
    backend = DuckDBBackend(":memory:")
    backend.init_schema()
    assert backend.table_exists("SecurityMaster")
    assert backend.table_exists("Pricing")
    assert backend.table_exists("FieldDef")


def test_query_returns_polars_df():
    backend = DuckDBBackend(":memory:")
    result = backend.query("SELECT 1 AS x, 'hello' AS y")
    assert isinstance(result, pl.DataFrame)
    assert list(result.columns) == ["x", "y"]
