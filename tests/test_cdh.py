from datetime import date

import pytest

from dtk.cdh import cdh


def test_cdh_not_implemented(seeded_store):
    with pytest.raises(NotImplementedError):
        cdh(seeded_store, ["SPY"], ["PX_CLOSE"], start_dt=date(2024, 1, 1))
