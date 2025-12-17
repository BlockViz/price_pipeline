import datetime as dt
from datetime import timezone
from time import perf_counter
from typing import overload

from dq_config import LOG_HEARTBEAT_SEC

__all__ = [
    "tdur",
    "utcnow",
    "to_utc",
    "ensure_utc_dt",
    "floor_to_hour_utc",
    "_to_pydate",
    "day_bounds_utc",
    "ym_tag",
    "month_start",
    "next_month_start",
    "date_seq",
    "hour_seq",
    "_now_str",
    "_maybe_heartbeat",
]

def tdur(t0): return f"{(perf_counter()-t0):.2f}s"
def utcnow(): return dt.datetime.now(timezone.utc)

@overload
def to_utc(x: dt.datetime) -> dt.datetime: ...
@overload
def to_utc(x: None) -> None: ...
def to_utc(x: dt.datetime | None) -> dt.datetime | None:
    if x is None: return None
    if x.tzinfo is None: return x.replace(tzinfo=timezone.utc)
    return x.astimezone(timezone.utc)

def ensure_utc_dt(x: dt.datetime | dt.date | None, name="dt") -> dt.datetime:
    if x is None: raise TypeError(f"{name} is None; expected datetime/date")
    if isinstance(x, dt.datetime): return to_utc(x)
    if isinstance(x, dt.date): return dt.datetime(x.year, x.month, x.day, tzinfo=timezone.utc)
    raise TypeError(f"{name}={x!r} is not date/datetime")

def floor_to_hour_utc(x: dt.datetime):
    x = ensure_utc_dt(x, "x")
    return x.replace(minute=0, second=0, microsecond=0, tzinfo=timezone.utc)

def _to_pydate(x) -> dt.date:
    if x is None: raise TypeError("Cannot coerce None to date")
    if isinstance(x, dt.date) and not isinstance(x, dt.datetime): return x
    if isinstance(x, dt.datetime): return to_utc(x).date()
    try: return dt.date.fromisoformat(str(x)[:10])
    except Exception: pass
    try: return dt.date(1970,1,1) + dt.timedelta(days=int(x))
    except Exception: pass
    raise TypeError(f"Cannot interpret date value {x!r}")

def day_bounds_utc(d: dt.date):
    start = dt.datetime(d.year,d.month,d.day,tzinfo=timezone.utc)
    return start, start + dt.timedelta(days=1)

def ym_tag(d: dt.date) -> str:
    return f"{d.year:04d}-{d.month:02d}"

def month_start(d: dt.date) -> dt.date:
    return dt.date(d.year, d.month, 1)

def next_month_start(d: dt.date) -> dt.date:
    if d.month == 12:
        return dt.date(d.year + 1, 1, 1)
    return dt.date(d.year, d.month + 1, 1)

def date_seq(last_inclusive: dt.date, days: int):
    start = last_inclusive - dt.timedelta(days=days-1)
    return [start + dt.timedelta(days=i) for i in range(days)]

def hour_seq(start_dt: dt.datetime, end_excl: dt.datetime):
    cur = floor_to_hour_utc(start_dt); end_excl = to_utc(end_excl)
    while cur < end_excl:
        yield cur; cur += dt.timedelta(hours=1)

def _now_str():  # fast ts string for logs
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

_last_log_time = [0.0]
def _maybe_heartbeat(tag: str):
    now = perf_counter()
    if (now - _last_log_time[0]) >= LOG_HEARTBEAT_SEC:
        _last_log_time[0] = now
        print(f"[{_now_str()}] [hb] {tag} still working")
