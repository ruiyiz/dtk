from datetime import date

import polars as pl
import pytest

from dtk.cds import cds, cds_dividend
from dtk.errors import DtkValueError


def test_cds_unknown_dataset(seeded_store):
    with pytest.raises(DtkValueError):
        cds(seeded_store, ["SPY"], "unknown_dataset")


def test_cds_dividend_empty(seeded_store):
    # No dividends seeded; should return empty DataFrame
    result = cds_dividend(seeded_store, ["SPY"])
    assert isinstance(result, pl.DataFrame)
    assert result.is_empty()
