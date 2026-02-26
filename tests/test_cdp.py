import pytest

from dtk.cdp import cdp


def test_cdp_not_implemented(seeded_store):
    with pytest.raises(NotImplementedError):
        cdp(seeded_store, ["SPY"], ["PX_CLOSE"])
