import pytest

from dtk import Store


def test_from_duckdb_memory():
    store = Store.from_duckdb(":memory:")
    assert store._backend is not None
    assert store._fields is not None


def test_schema_initialized(store):
    assert store._backend.table_exists("SecurityMaster")
    assert store._backend.table_exists("FieldDef")
    assert store._backend.table_exists("Pricing")
    assert store._backend.table_exists("Dividend")
    assert store._backend.table_exists("CorpEvent")
    assert store._backend.table_exists("AdjFactor")
    assert store._backend.table_exists("FieldSnapshot")
    assert store._backend.table_exists("FieldOverride")


def test_backend_query_returns_polars(store):
    import polars as pl

    result = store._backend.query("SELECT 1 AS val")
    assert isinstance(result, pl.DataFrame)
    assert result["val"][0] == 1
