from datetime import date

import polars as pl
import pytest

from dtk.cdu import cdu, cdu_dataset, cdu_position, cdu_pricing, cdu_security
from dtk.errors import DtkValueError


def test_cdu_wide_upload(seeded_store):
    df = pl.DataFrame(
        {
            "SecurityId": [1],
            "ValueDate": [date(2024, 1, 4)],
            "PX_CLOSE": [455.0],
        }
    )
    result = cdu(seeded_store, df)
    assert result["wide"] == 1
    assert result["long"] == 0


def test_cdu_pricing_upload(seeded_store):
    df = pl.DataFrame(
        {
            "SecurityId": [1],
            "ValueDate": [date(2024, 1, 4)],
            "PxClose": [455.0],
            "PxOpen": [453.0],
        }
    )
    n = cdu_pricing(seeded_store, df)
    assert n == 1


def test_cdu_pricing_roundtrip(seeded_store):
    df = pl.DataFrame(
        {
            "SecurityId": [1],
            "ValueDate": [date(2024, 1, 4)],
            "PxClose": [999.0],
        }
    )
    cdu_pricing(seeded_store, df)
    result = seeded_store._backend.query(
        "SELECT PxClose FROM Pricing WHERE SecurityId = 1 AND ValueDate = '2024-01-04'"
    )
    assert result["PxClose"][0] == 999.0


def test_cdu_pricing_upsert_updates_existing(seeded_store):
    df = pl.DataFrame(
        {
            "SecurityId": [1],
            "ValueDate": [date(2024, 1, 2)],
            "PxClose": [999.0],
        }
    )
    cdu_pricing(seeded_store, df)
    result = seeded_store._backend.query(
        "SELECT PxClose FROM Pricing WHERE SecurityId = 1 AND ValueDate = '2024-01-02'"
    )
    assert result["PxClose"][0] == 999.0


def test_cdu_dataset_unknown(seeded_store):
    df = pl.DataFrame({"SecurityId": [1]})
    with pytest.raises(DtkValueError):
        cdu_dataset(seeded_store, df, "unknown")


def test_cdu_position_basic(seeded_store):
    df = pl.DataFrame(
        {
            "Ticker": ["SPY"],
            "PortfolioId": ["test"],
            "ValueDate": [date(2024, 1, 2)],
            "Shares": [100.0],
        }
    )
    n = cdu_position(seeded_store, df)
    assert n == 1
    result = seeded_store._backend.query(
        "SELECT Shares FROM Position "
        "WHERE PortfolioId = 'test' AND ValueDate = '2024-01-02'"
    )
    assert result["Shares"][0] == 100.0


def test_cdu_position_upsert(seeded_store):
    df1 = pl.DataFrame(
        {
            "Ticker": ["SPY"],
            "PortfolioId": ["test"],
            "ValueDate": [date(2024, 1, 2)],
            "Shares": [100.0],
        }
    )
    cdu_position(seeded_store, df1)

    df2 = pl.DataFrame(
        {
            "Ticker": ["SPY"],
            "PortfolioId": ["test"],
            "ValueDate": [date(2024, 1, 2)],
            "Shares": [150.0],
        }
    )
    cdu_position(seeded_store, df2)

    result = seeded_store._backend.query(
        "SELECT COUNT(*) AS n FROM Position WHERE PortfolioId = 'test'"
    )
    assert result["n"][0] == 1
    val = seeded_store._backend.query(
        "SELECT Shares FROM Position WHERE PortfolioId = 'test'"
    )
    assert val["Shares"][0] == 150.0


def test_cdu_position_missing_columns(seeded_store):
    df = pl.DataFrame(
        {
            "Ticker": ["SPY"],
            "ValueDate": [date(2024, 1, 2)],
        }
    )
    with pytest.raises(DtkValueError):
        cdu_position(seeded_store, df)


def test_cdu_security_insert(seeded_store):
    df = pl.DataFrame(
        {
            "Id": [99],
            "Ticker": ["IVV"],
            "SecurityType": ["ETF"],
            "Currency": ["USD"],
            "ExchangeCode": ["US"],
            "IsActive": [True],
        }
    )
    n = cdu_security(seeded_store, df)
    assert n == 1
    result = seeded_store._backend.query(
        "SELECT Ticker FROM SecurityMaster WHERE Id = 99"
    )
    assert result["Ticker"][0] == "IVV"
