import pytest

from dtk.cdu import cdu, cdu_dataset, cdu_pricing, cdu_security
from dtk.errors import DtkValueError


def test_cdu_not_implemented(seeded_store):
    import polars as pl

    df = pl.DataFrame(
        {"SecurityId": [1], "ValueDate": ["2024-01-02"], "PX_CLOSE": [450.0]}
    )
    with pytest.raises(NotImplementedError):
        cdu(seeded_store, df)


def test_cdu_pricing_not_implemented(seeded_store):
    import polars as pl

    df = pl.DataFrame(
        {"SecurityId": [1], "ValueDate": ["2024-01-02"], "PxClose": [450.0]}
    )
    with pytest.raises(NotImplementedError):
        cdu_pricing(seeded_store, df)


def test_cdu_dataset_unknown(seeded_store):
    import polars as pl

    df = pl.DataFrame({"SecurityId": [1]})
    with pytest.raises(DtkValueError):
        cdu_dataset(seeded_store, df, "unknown")
