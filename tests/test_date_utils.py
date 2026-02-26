from datetime import date

import pytest

from dtk.date_utils import (
    dt_adjust,
    get_exchange_calendar,
    is_trading_day,
    prev_date,
    seq_date,
)
from dtk.errors import DtkValueError


def test_get_exchange_calendar_us():
    cal = get_exchange_calendar("US")
    assert cal is not None


def test_get_exchange_calendar_unknown():
    with pytest.raises(DtkValueError):
        get_exchange_calendar("ZZ")


def test_is_trading_day_weekday():
    # 2024-01-02 is a Tuesday (NYSE is open)
    assert is_trading_day(date(2024, 1, 2), "US") is True


def test_is_trading_day_weekend():
    # 2024-01-06 is Saturday
    assert is_trading_day(date(2024, 1, 6), "US") is False


def test_seq_date_daily():
    dates = seq_date(date(2024, 1, 2), date(2024, 1, 5), period="D", days="C")
    assert date(2024, 1, 2) in dates
    assert date(2024, 1, 5) in dates
    assert len(dates) == 4


def test_prev_date_daily():
    # 2024-01-03 is Wednesday; previous weekday is 2024-01-02
    result = prev_date(date(2024, 1, 3), period="D", days="N")
    assert result < date(2024, 1, 3)
