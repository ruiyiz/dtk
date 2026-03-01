import polars as pl
import pytest

from dtk.cds import cds, cds_dividend, cds_position, cds_transaction
from dtk.errors import DtkValueError


def test_cds_unknown_dataset(seeded_store):
    with pytest.raises(DtkValueError):
        cds(seeded_store, ["SPY"], "unknown_dataset")


def test_cds_dividend_empty(seeded_store):
    # No dividends seeded; should return empty DataFrame
    result = cds_dividend(seeded_store, ["SPY"])
    assert isinstance(result, pl.DataFrame)
    assert result.is_empty()


def test_cds_position_empty(seeded_store):
    result = cds_position(seeded_store, ["SPY"])
    assert isinstance(result, pl.DataFrame)
    assert result.is_empty()


def test_cds_position_roundtrip(seeded_store):
    seeded_store._backend.execute(
        "INSERT INTO Position (SecurityId, PortfolioId, ValueDate, Shares) "
        "VALUES (1, 'test', '2024-01-02', 100.0)"
    )
    result = cds_position(seeded_store, ["SPY"])
    assert len(result) == 1
    assert result["Shares"][0] == 100.0
    assert result["Ticker"][0] == "SPY"


def test_cds_position_filter_portfolio(seeded_store):
    seeded_store._backend.execute(
        "INSERT INTO Position (SecurityId, PortfolioId, ValueDate, Shares) VALUES "
        "(1, 'port1', '2024-01-02', 100.0), (1, 'port2', '2024-01-02', 200.0)"
    )
    result = cds_position(seeded_store, ["SPY"], portfolio_id="port1")
    assert len(result) == 1
    assert float(result["Shares"][0]) == 100.0


def test_cds_transaction_roundtrip(seeded_store):
    seeded_store._backend.execute(
        "INSERT INTO Transaction "
        "(TransactionId, SecurityId, PortfolioId, TradeDate, "
        "TransactionType, Shares, Price) "
        "VALUES (1, 1, 'test', '2024-01-02', 'BUY', 100.0, 450.0)"
    )
    result = cds_transaction(seeded_store, ["SPY"])
    assert len(result) == 1
    assert result["TransactionType"][0] == "BUY"
    assert result["Ticker"][0] == "SPY"


def test_cds_transaction_filter_type(seeded_store):
    seeded_store._backend.execute(
        "INSERT INTO Transaction "
        "(TransactionId, SecurityId, PortfolioId, TradeDate, "
        "TransactionType, Shares, Price) "
        "VALUES (1, 1, 'test', '2024-01-02', 'BUY', 100.0, 450.0), "
        "(2, 1, 'test', '2024-01-03', 'SELL', 50.0, 452.0)"
    )
    result = cds_transaction(seeded_store, ["SPY"], transaction_type="BUY")
    assert len(result) == 1
    assert result["TransactionType"][0] == "BUY"
