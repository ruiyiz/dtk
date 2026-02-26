"""Date utilities: prev_date, next_date, seq_date, dt_adjust, exchange_calendars."""

from __future__ import annotations

from datetime import date, timedelta

import exchange_calendars as xcals

from dtk._constants import EXCHANGE_CALENDAR_MAP
from dtk._types import BDC, Days, Endpoint, Periodicity
from dtk.errors import DtkValueError


def get_exchange_calendar(exch_code: str):
    """Return an exchange_calendars Calendar object for the given exchange code."""
    mic = EXCHANGE_CALENDAR_MAP.get(exch_code)
    if mic is None:
        raise DtkValueError(f"Unknown exchange code: {exch_code!r}")
    return xcals.get_calendar(mic)


def is_trading_day(dt: date, exch_code: str = "US") -> bool:
    cal = get_exchange_calendar(exch_code)
    return cal.is_session(dt)


def _prev_trading_day(dt: date, cal) -> date:
    """Return the previous trading day strictly before dt."""
    # exchange_calendars uses pandas Timestamps; we convert back to date
    import pandas as pd

    ts = pd.Timestamp(dt)
    prev = cal.previous_session(ts)
    return prev.date()


def _next_trading_day(dt: date, cal) -> date:
    import pandas as pd

    ts = pd.Timestamp(dt)
    nxt = cal.next_session(ts)
    return nxt.date()


def _adjust_date(dt: date, cal, bdc: str) -> date:
    """Apply a BDC adjustment to a single date using the given calendar."""
    import pandas as pd

    ts = pd.Timestamp(dt)

    def is_session(d: date) -> bool:
        return cal.is_session(pd.Timestamp(d))

    if is_session(dt):
        return dt

    if bdc == BDC.Unadjusted:
        return dt
    elif bdc == BDC.Following:
        while not is_session(dt):
            dt = dt + timedelta(days=1)
    elif bdc == BDC.Preceding:
        while not is_session(dt):
            dt = dt - timedelta(days=1)
    elif bdc == BDC.ModifiedFollowing:
        orig_month = dt.month
        candidate = dt
        while not is_session(candidate):
            candidate = candidate + timedelta(days=1)
        if candidate.month != orig_month:
            candidate = dt
            while not is_session(candidate):
                candidate = candidate - timedelta(days=1)
        dt = candidate
    elif bdc == BDC.ModifiedPreceding:
        orig_month = dt.month
        candidate = dt
        while not is_session(candidate):
            candidate = candidate - timedelta(days=1)
        if candidate.month != orig_month:
            candidate = dt
            while not is_session(candidate):
                candidate = candidate + timedelta(days=1)
        dt = candidate
    return dt


def dt_adjust(
    dates: list[date], exch_code: str, bdc: str = BDC.Following
) -> list[date]:
    """Adjust a list of dates to business days according to bdc."""
    cal = get_exchange_calendar(exch_code)
    return [_adjust_date(d, cal, bdc) for d in dates]


def prev_date(
    dt: date,
    exch_code: str = "US",
    period: str = "D",
    days: str = "N",
    endpoint: str = "last_of",
    n: int = 1,
) -> date:
    """Return the nth previous date per period/days/endpoint logic."""
    if n == 0:
        return dt
    return _nth_period_date(dt, exch_code, period, days, endpoint, n=-n)


def next_date(
    dt: date,
    exch_code: str = "US",
    period: str = "D",
    days: str = "N",
    endpoint: str = "last_of",
    n: int = 1,
) -> date:
    """Return the nth next date per period/days/endpoint logic."""
    if n == 0:
        return dt
    return _nth_period_date(dt, exch_code, period, days, endpoint, n=n)


def _nth_period_date(
    dt: date, exch_code: str, period: str, days: str, endpoint: str, n: int
) -> date:
    """Helper: generate a sequence and return the nth element."""
    n_abs = abs(n)
    span_days = {
        "D": 1 * n_abs + 7,
        "W": 7 * (n_abs + 1),
        "M": 31 * (n_abs + 1),
        "Q": 92 * (n_abs + 1),
        "HY": 183 * (n_abs + 1),
        "Y": 366 * (n_abs + 1),
    }[period]

    start = dt + timedelta(days=int(span_days) * (-1 if n < 0 else 1))
    if n < 0:
        from_d, to_d = start, dt - timedelta(days=1)  # exclusive of dt
    else:
        from_d, to_d = dt + timedelta(days=1), start  # exclusive of dt

    ds = seq_date(
        from_d, to_d, exch_code=exch_code, period=period, days=days, endpoint=endpoint
    )
    if n < 0:
        ds = list(reversed(ds))

    if len(ds) < n_abs:
        raise DtkValueError(f"Not enough dates in sequence for n={n}")
    return ds[n_abs - 1]


def seq_date(
    from_dt: date,
    to_dt: date,
    exch_code: str = "US",
    period: str = "D",
    days: str = "N",
    endpoint: str = "last_of",
    partial: bool = False,
) -> list[date]:
    """Generate a sequence of period-end (or period-start) dates."""
    if from_dt > to_dt:
        from_dt, to_dt = to_dt, from_dt
        reversed_order = True
    else:
        reversed_order = False

    all_days = [from_dt + timedelta(days=i) for i in range((to_dt - from_dt).days + 1)]

    def adjust(dates: list[date], bdc: str) -> list[date]:
        if days == Days.CalendarDay:
            return dates
        adj_exch = "WD" if days == Days.NonTradingDay else exch_code
        return dt_adjust(dates, adj_exch, bdc)

    if period == Periodicity.D:
        out = adjust(all_days, BDC.Preceding)
    elif period == Periodicity.W:
        if endpoint == Endpoint.LastOf:
            week_ends = _unique_week_ends(all_days)
            out = adjust(week_ends, BDC.Preceding)
        else:
            week_starts = _unique_week_starts(all_days)
            out = adjust(week_starts, BDC.Following)
    elif period == Periodicity.M:
        if endpoint == Endpoint.LastOf:
            month_ends = _unique_month_ends(all_days)
            out = adjust(month_ends, BDC.Preceding)
        else:
            month_starts = _unique_month_starts(all_days)
            out = adjust(month_starts, BDC.Following)
    elif period == Periodicity.Q:
        if endpoint == Endpoint.LastOf:
            q_ends = _unique_quarter_ends(all_days)
            out = adjust(q_ends, BDC.Preceding)
        else:
            q_starts = _unique_quarter_starts(all_days)
            out = adjust(q_starts, BDC.Following)
    elif period == Periodicity.Y:
        if endpoint == Endpoint.LastOf:
            y_ends = _unique_year_ends(all_days)
            out = adjust(y_ends, BDC.Preceding)
        else:
            y_starts = _unique_year_starts(all_days)
            out = adjust(y_starts, BDC.Following)
    else:
        raise DtkValueError(f"Unsupported period: {period!r}")

    seen: set[date] = set()
    unique_out: list[date] = []
    for d in out:
        if from_dt <= d <= to_dt and d not in seen:
            seen.add(d)
            unique_out.append(d)

    if partial:
        if endpoint == Endpoint.LastOf:
            if not unique_out or unique_out[-1] < to_dt:
                if to_dt not in seen:
                    unique_out.append(to_dt)
        else:
            if not unique_out or unique_out[0] > from_dt:
                unique_out.insert(0, from_dt)

    out = sorted(unique_out, reverse=reversed_order)
    return out


# --- Period boundary helpers ---


def _unique_week_ends(dates: list[date]) -> list[date]:
    seen: set[date] = set()
    result = []
    for d in dates:
        # ISO weekday: Monday=1, Sunday=7
        week_end = d + timedelta(days=7 - d.isoweekday())
        if week_end not in seen:
            seen.add(week_end)
            result.append(week_end)
    return result


def _unique_week_starts(dates: list[date]) -> list[date]:
    seen: set[date] = set()
    result = []
    for d in dates:
        week_start = d - timedelta(days=d.isoweekday() - 1)
        if week_start not in seen:
            seen.add(week_start)
            result.append(week_start)
    return result


def _unique_month_ends(dates: list[date]) -> list[date]:
    import calendar

    seen: set[date] = set()
    result = []
    for d in dates:
        last = calendar.monthrange(d.year, d.month)[1]
        me = date(d.year, d.month, last)
        if me not in seen:
            seen.add(me)
            result.append(me)
    return result


def _unique_month_starts(dates: list[date]) -> list[date]:
    seen: set[date] = set()
    result = []
    for d in dates:
        ms = date(d.year, d.month, 1)
        if ms not in seen:
            seen.add(ms)
            result.append(ms)
    return result


def _unique_quarter_ends(dates: list[date]) -> list[date]:
    import calendar

    seen: set[date] = set()
    result = []
    for d in dates:
        q_month = ((d.month - 1) // 3 + 1) * 3
        last = calendar.monthrange(d.year, q_month)[1]
        qe = date(d.year, q_month, last)
        if qe not in seen:
            seen.add(qe)
            result.append(qe)
    return result


def _unique_quarter_starts(dates: list[date]) -> list[date]:
    seen: set[date] = set()
    result = []
    for d in dates:
        q_month = ((d.month - 1) // 3) * 3 + 1
        qs = date(d.year, q_month, 1)
        if qs not in seen:
            seen.add(qs)
            result.append(qs)
    return result


def _unique_year_ends(dates: list[date]) -> list[date]:
    seen: set[int] = set()
    result = []
    for d in dates:
        if d.year not in seen:
            seen.add(d.year)
            result.append(date(d.year, 12, 31))
    return result


def _unique_year_starts(dates: list[date]) -> list[date]:
    seen: set[int] = set()
    result = []
    for d in dates:
        if d.year not in seen:
            seen.add(d.year)
            result.append(date(d.year, 1, 1))
    return result
