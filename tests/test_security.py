import pytest

from dtk.security import get_security_ids, get_security_tickers, lookup_securities


def test_lookup_by_ticker(seeded_store):
    result = lookup_securities(seeded_store, ["SPY"], id_type="ticker")
    assert result.shape[0] == 1
    assert result["Ticker"][0] == "SPY"


def test_lookup_by_id(seeded_store):
    result = lookup_securities(seeded_store, [1], id_type="id")
    assert result.shape[0] == 1
    assert result["Id"][0] == 1


def test_lookup_multiple(seeded_store):
    result = lookup_securities(seeded_store, ["SPY", "AAPL"], id_type="ticker")
    assert result.shape[0] == 2


def test_lookup_unknown_returns_empty(seeded_store):
    result = lookup_securities(seeded_store, ["UNKNOWN"], id_type="ticker")
    assert result.is_empty()


def test_get_security_ids(seeded_store):
    ids = get_security_ids(seeded_store, ["SPY", "AAPL"])
    assert set(ids) == {1, 2}


def test_get_security_tickers(seeded_store):
    tickers = get_security_tickers(seeded_store, [1, 2])
    assert set(tickers) == {"SPY", "AAPL"}
