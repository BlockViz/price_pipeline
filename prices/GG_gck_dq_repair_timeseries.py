#!/usr/bin/env python3
# GG_gck_dq_repair_timeseries.py
# Optimized to plan exclusively from coverage tables:
#   - coin_daily_coverage_ranges  → daily gaps
#   - coin_intraday_coverage      → hourly & 10m gaps
# Repair + API backfill + aggregate recompute remain.
# Optional quality sweep for hourly rows is off by default (FILL_HOURLY_BADSCAN=0).

import os, time, random, requests, datetime as dt
from datetime import timezone
from collections import defaultdict, deque
from time import perf_counter, sleep
from typing import overload
import traceback

# ───────────────────── Connector ─────────────────────
from astra_connect.connect import get_session, AstraConfig
AstraConfig.from_env()  # ← required to load creds/secure bundle/keyspace
session, cluster = get_session(return_cluster=True)

# ---------- Config ----------
KEYSPACE     = os.getenv("ASTRA_KEYSPACE", "default_keyspace")

TABLE_LIVE      = os.getenv("TABLE_LIVE", "gecko_prices_live")
TEN_MIN_TABLE   = os.getenv("TEN_MIN_TABLE", "gecko_prices_10m_7d")
DAILY_TABLE     = os.getenv("DAILY_TABLE", "gecko_candles_daily_contin")
HOURLY_TABLE    = os.getenv("HOURLY_TABLE", "gecko_candles_hourly_30d")

# Coverage tables
COIN_DAILY_RANGES   = os.getenv("COIN_DAILY_RANGES",   "coin_daily_coverage_ranges")
COIN_INTRADAY_BITS  = os.getenv("COIN_INTRADAY_BITS",  "coin_intraday_coverage")

# Aggregate targets
MCAP_10M_TABLE   = os.getenv("MCAP_10M_TABLE",   "gecko_market_cap_10m_7d")
MCAP_HOURLY_TABLE= os.getenv("MCAP_HOURLY_TABLE","gecko_market_cap_hourly_30d")
MCAP_DAILY_TABLE = os.getenv("MCAP_DAILY_TABLE", "gecko_market_cap_daily_contin")

# Repair windows
DAYS_10M    = int(os.getenv("DQ_WINDOW_10M_DAYS", "7"))
DAYS_DAILY  = int(os.getenv("DQ_WINDOW_DAILY_DAYS", "365"))
DAYS_HOURLY = int(os.getenv("DQ_WINDOW_HOURLY_DAYS", "30"))

# Nightly vs Intraday (convenience)
DQ_MODE = (os.getenv("DQ_MODE", "NIGHTLY").strip().upper())  # "NIGHTLY" | "INTRADAY"
# DQ_MODE = "NIGHTLY"   ## manual run for daily repair
# DQ_MODE = "INTRADAY"  ## manua run for 10m and hourly repair

# What to run
FIX_DAILY_FROM_10M      = os.getenv("FIX_DAILY_FROM_10M", "1") == "1"
SEED_10M_FROM_DAILY     = os.getenv("SEED_10M_FROM_DAILY", "0") == "1"
FILL_HOURLY             = os.getenv("FILL_HOURLY", "1") == "1"
FILL_HOURLY_FROM_API    = os.getenv("FILL_HOURLY_FROM_API", "1") == "1"
INTERPOLATE_IF_API_MISS = os.getenv("INTERPOLATE_IF_API_MISS", "1") == "1"
BACKFILL_DAILY_FROM_API = os.getenv("BACKFILL_DAILY_FROM_API", "1") == "1"
FULL_DAILY_ALL          = os.getenv("FULL_DAILY_ALL", "1") == "1"  # daily agg across entire history
FILL_HOURLY_BADSCAN     = os.getenv("FILL_HOURLY_BADSCAN", "0") == "1"  # optional & off by default

# Aggregate recompute toggles
FULL_MODE = os.getenv("FULL_MODE", "1") == "1"
TRUNCATE_AGGREGATES_IN_FULL = os.getenv("TRUNCATE_AGGREGATES_IN_FULL", "1") == "1"
RECOMPUTE_MCAP_10M    = os.getenv("RECOMPUTE_MCAP_10M", "1") == "1"
RECOMPUTE_MCAP_HOURLY = os.getenv("RECOMPUTE_MCAP_HOURLY", "1") == "1"
RECOMPUTE_MCAP_DAILY  = os.getenv("RECOMPUTE_MCAP_DAILY", "1") == "1"

# Universe selection
TOP_N_DQ              = int(os.getenv("TOP_N_DQ", "210"))
DQ_MAX_COINS          = int(os.getenv("DQ_MAX_COINS", "100000"))
INCLUDE_ALL_DAILY_IDS = os.getenv("INCLUDE_ALL_DAILY_IDS", "1") == "1"
# New, to steer intraday behavior
INTRADAY_TOP_N             = int(os.getenv("INTRADAY_TOP_N", str(TOP_N_DQ)))
INCLUDE_UNRANKED_INTRADAY  = os.getenv("INCLUDE_UNRANKED_INTRADAY", "0") == "1"
SKIP_UNCOVERED_COINS = os.getenv("SKIP_UNCOVERED_COINS", "1") == "1"

# Performance / logging
REQUEST_TIMEOUT = int(os.getenv("DQ_REQUEST_TIMEOUT_SEC", "30"))
FETCH_SIZE      = int(os.getenv("DQ_FETCH_SIZE", "500"))
RETRIES         = int(os.getenv("DQ_RETRIES", "3"))
BACKOFF_S       = int(os.getenv("DQ_BACKOFF_SEC", "4"))
PAUSE_S         = float(os.getenv("DQ_PAUSE_PER_COIN", "0.04"))
LOG_HEARTBEAT_SEC       = float(os.getenv("DQ_LOG_HEARTBEAT_SEC", "15"))
LOG_EVERY               = int(os.getenv("DQ_LOG_EVERY", "10"))
VERBOSE                 = os.getenv("DQ_VERBOSE", "0") == "1"
TIME_API                = os.getenv("DQ_TIME_API", "1") == "1"
DRY_RUN                 = os.getenv("DQ_DRY_RUN", "0") == "1"

# Soft run budget
SOFT_BUDGET_SEC = int(os.getenv("DQ_SOFT_BUDGET_SEC", str(24*60)))

# CoinGecko API
API_TIER = (os.getenv("COINGECKO_API_TIER") or "pro").strip().lower()
API_KEY  = (os.getenv("COINGECKO_API_KEY") or "").strip()
if API_KEY.lower().startswith("api key:"):
    API_KEY = API_KEY.split(":", 1)[1].strip()
BASE = os.getenv(
    "COINGECKO_BASE_URL",
    "https://api.coingecko.com/api/v3" if API_TIER == "demo" else "https://pro-api.coingecko.com/api/v3"
)

# Daily API backfill request budget
DAILY_API_REQ_BUDGET = int(os.getenv("DAILY_API_REQ_BUDGET", "150"))
PAD_DAYS             = int(os.getenv("DAILY_API_PAD_DAYS", "1"))
MAX_RANGE_DAYS       = int(os.getenv("DAILY_API_MAX_RANGE_DAYS", "90"))

# Interpolation knobs
INTERPOLATE_MCAP_VOL_DAILY  = os.getenv("INTERPOLATE_MCAP_VOL_DAILY", "1") == "1"
INTERPOLATE_MCAP_VOL_HOURLY = os.getenv("INTERPOLATE_MCAP_VOL_HOURLY", "1") == "1"
INTERP_MAX_DAYS  = int(os.getenv("INTERP_MAX_DAYS", "60"))
INTERP_MAX_HOURS = int(os.getenv("INTERP_MAX_HOURS", "168"))

# Rate limiting
CG_REQ_INTERVAL_S = float(os.getenv("CG_REQUEST_INTERVAL_S", "1.0" if API_TIER == "demo" else "0.25"))
CG_MAX_RPM        = int(os.getenv("CG_MAX_RPM", "50" if API_TIER == "demo" else "120"))
_req_times = deque()

RUNS_ANY_DAILY = BACKFILL_DAILY_FROM_API or FIX_DAILY_FROM_10M
RUNS_INTRADAY  = FILL_HOURLY or SEED_10M_FROM_DAILY  # seeding is intraday-side, optional

# ---------- Metrics ----------
metrics = {
    "cg_calls_total": 0,
    "cg_calls_ok": 0,
    "cg_calls_429": 0,
    "cg_calls_deferred": 0,
    "cg_calls_5xx": 0,
    "cg_calls_other_http": 0,
    "points_daily": 0,
    "points_hourly": 0,
    "points_10m": 0,
    "agg_rows_10m": 0,
    "agg_rows_hourly": 0,
    "agg_rows_daily": 0,
}
from collections import defaultdict as _dd
coin_fix_counts = _dd(int)

# Negative cache
NEG_CACHE_PATH = os.getenv("COINGECKO_NEG_CACHE_PATH", "tmp/coingecko_no_market.ids")
os.makedirs(os.path.dirname(NEG_CACHE_PATH), exist_ok=True)

# ---------- Small utils ----------
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
        print(f"[{_now_str()}] [hb] {tag} … still working")

# ---------- HTTP with rate limiting ----------
class RateLimitDefer(Exception):
    def __init__(self, status_code: int, retry_after: float | None, url: str):
        super().__init__(f"Rate limited {status_code}, retry_after={retry_after}, url={url}")
        self.status_code = status_code
        self.retry_after = retry_after
        self.url = url

def _throttle():
    now = time.time()
    if _req_times and (now - _req_times[-1]) < CG_REQ_INTERVAL_S:
        time.sleep(CG_REQ_INTERVAL_S - (now - _req_times[-1])); now = time.time()
    cutoff = now - 60.0
    while _req_times and _req_times[0] < cutoff: _req_times.popleft()
    if len(_req_times) >= CG_MAX_RPM:
        sleep_for = 60.0 - (now - _req_times[0]) + 0.01
        time.sleep(max(0.0, sleep_for))

def http_get(path, params=None):
    url = f"{BASE}{path}"; params = dict(params or {})
    headers = {}
    if API_TIER == "demo":
        if not API_KEY: raise RuntimeError("COINGECKO_API_KEY missing for demo tier.")
        params["x_cg_demo_api_key"] = API_KEY
    else:
        if not API_KEY: raise RuntimeError("COINGECKO_API_KEY missing for pro tier.")
        headers["x-cg-pro-api-key"] = API_KEY

    last = None
    for i in range(RETRIES):
        _throttle()
        _req_times.append(time.time())
        metrics["cg_calls_total"] += 1
        t0 = perf_counter()
        try:
            r = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            if r.status_code in (401, 403):
                metrics["cg_calls_other_http"] += 1
                raise RuntimeError(f"CoinGecko auth failed ({r.status_code}).")
            if r.status_code == 429:
                metrics["cg_calls_429"] += 1
                ra = r.headers.get("Retry-After")
                retry_after = float(ra) if ra is not None else None
                raise RateLimitDefer(429, retry_after, url)
            if r.status_code in (500,502,503,504):
                metrics["cg_calls_5xx"] += 1
                wait_s = min(5.0, BACKOFF_S*(i+1))
                print(f"[{_now_str()}] API {path} {r.status_code} — backoff {wait_s:.1f}s (retry {i+1}/{RETRIES})")
                time.sleep(wait_s + random.uniform(0,0.5)); last = requests.HTTPError(f"{r.status_code}", response=r); continue
            r.raise_for_status()
            if TIME_API: print(f"[{_now_str()}] API OK {path} took {tdur(t0)}")
            metrics["cg_calls_ok"] += 1
            return r.json()
        except RateLimitDefer:
            raise
        except (requests.ConnectionError, requests.Timeout) as e:
            last = e
            wait_s = min(5.0, BACKOFF_S*(i+1))
            print(f"[{_now_str()}] API {path} error: {e} — backoff {wait_s:.1f}s (retry {i+1}/{RETRIES})")
            time.sleep(wait_s + random.uniform(0,0.5))
        except requests.HTTPError as e:
            sc = e.response.status_code if e.response is not None else None
            if sc and sc != 429 and sc < 500: metrics["cg_calls_other_http"] += 1
            last = e
    raise RuntimeError(f"CoinGecko failed: {url} :: {last}")

# ---------- Bucket helpers ----------
def bucket_daily_ohlc(prices_ms_values, start_d: dt.date, end_d: dt.date):
    from collections import defaultdict as _dd
    per_day = _dd(list)
    for ms, price in prices_ms_values or []:
        ts_ = dt.datetime.fromtimestamp(ms/1000.0, tz=timezone.utc)
        per_day[ts_.date()].append((ts_, float(price)))
    out = {}
    d = start_d
    while d <= end_d:
        pts = sorted(per_day.get(d, []), key=lambda x: x[0])
        if pts:
            vals = [p for _,p in pts]
            out[d] = {"open": vals[0], "high": max(vals), "low": min(vals), "close": vals[-1], "last_ts": pts[-1][0], "is_true_ohlc": len(vals)>1}
        d += dt.timedelta(days=1)
    return out

def bucket_daily_last(values_ms_values, start_d: dt.date, end_d: dt.date):
    from collections import defaultdict as _dd
    per_day = _dd(list)
    for ms, val in values_ms_values or []:
        ts_ = dt.datetime.fromtimestamp(ms/1000.0, tz=timezone.utc)
        v = float(val) if val is not None else None
        per_day[ts_.date()].append((ts_, v))
    out = {}
    d = start_d
    while d <= end_d:
        pts = sorted(per_day.get(d, []), key=lambda x: x[0])
        if pts: out[d] = {"val": pts[-1][1], "ts": pts[-1][0]}
        d += dt.timedelta(days=1)
    return out

# ---------- Interpolation ----------
def _interp_linear(x0,y0,x1,y1,x):
    if x1 == x0: return y0
    w = (x - x0)/(x1 - x0); return y0 + (y1 - y0)*w

def interpolate_daily_series(dates, values_map, max_gap_days=None):
    out = {}; known = sorted([d for d in dates if values_map.get(d) is not None])
    if not known: return {d: None for d in dates}
    for d in dates:
        v = values_map.get(d)
        if v is not None: out[d] = float(v); continue
        left = max([k for k in known if k < d], default=None)
        right = min([k for k in known if k > d], default=None)
        if left is None and right is None: out[d] = None
        elif left is None: out[d] = float(values_map[right])
        elif right is None: out[d] = float(values_map[left])
        else:
            gap = (right - left).days
            if (max_gap_days is not None) and (gap > max_gap_days):
                out[d] = float(values_map[left])
            else:
                y = _interp_linear(left.toordinal(), float(values_map[left]), right.toordinal(), float(values_map[right]), d.toordinal())
                out[d] = float(y)
    return out

def interpolate_hourly_series(hours, values_map, max_gap_hours=None):
    out = {}; known = sorted([h for h in hours if values_map.get(h) is not None])
    if not known: return {h: None for h in hours}
    for h in hours:
        v = values_map.get(h)
        if v is not None: out[h] = float(v); continue
        left = max([k for k in known if k < h], default=None)
        right = min([k for k in known if k > h], default=None)
        if left is None and right is None: out[h] = None
        elif left is None: out[h] = float(values_map[right])
        elif right is None: out[h] = float(values_map[left])
        else:
            gap = int((right - left).total_seconds() // 3600)
            if (max_gap_hours is not None) and (gap > max_gap_hours):
                out[h] = float(values_map[left])
            else:
                y = _interp_linear(left.timestamp(), float(values_map[left]), right.timestamp(), float(values_map[right]), h.timestamp())
                out[h] = float(y)
    return out

# ---------- Prepared statements ----------
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement, BatchStatement

# include category from live
SEL_LIVE = SimpleStatement(
    f"SELECT id, symbol, name, market_cap_rank, category FROM {TABLE_LIVE}",
    fetch_size=FETCH_SIZE
)

SEL_DAILY_DISTINCT_IDS = SimpleStatement(
    f"SELECT DISTINCT id FROM {DAILY_TABLE}",
    fetch_size=FETCH_SIZE
)

SEL_10M_RANGE_FULL = session.prepare(f"""
  SELECT ts, price_usd, market_cap, volume_24h,
         market_cap_rank, circulating_supply, total_supply, last_updated
  FROM {TEN_MIN_TABLE}
  WHERE id = ? AND ts >= ? AND ts < ? ORDER BY ts ASC
""")
SEL_10M_ONE = session.prepare(f"""
  SELECT last_updated, market_cap, volume_24h
  FROM {TEN_MIN_TABLE}
  WHERE id = ? AND ts = ? LIMIT 1
""")

SEL_DAILY_ONE = session.prepare(f"""
  SELECT date, symbol, name,
         price_usd, market_cap, volume_24h, last_updated,
         open, high, low, close, candle_source,
         market_cap_rank, circulating_supply, total_supply
  FROM {DAILY_TABLE}
  WHERE id = ? AND date = ? LIMIT 1
""")
SEL_DAILY_READ_FOR_AGG = session.prepare(f"""
  SELECT last_updated, market_cap, volume_24h
  FROM {DAILY_TABLE}
  WHERE id = ? AND date = ? LIMIT 1
""")

SEL_DAILY_FIRST = session.prepare(f"""
  SELECT date FROM {DAILY_TABLE}
  WHERE id = ? ORDER BY date ASC LIMIT 1
""")
SEL_DAILY_LAST = session.prepare(f"""
  SELECT date FROM {DAILY_TABLE}
  WHERE id = ? ORDER BY date DESC LIMIT 1
""")
SEL_DAILY_FOR_ID_RANGE_ALL = session.prepare(f"""
  SELECT date, last_updated, market_cap, volume_24h
  FROM {DAILY_TABLE}
  WHERE id = ? AND date >= ? AND date <= ?
""")

SEL_HOURLY_META_RANGE = session.prepare(f"""
  SELECT ts, price_usd, market_cap, volume_24h, candle_source, last_updated
  FROM {HOURLY_TABLE}
  WHERE id = ? AND ts >= ? AND ts < ?
""")
SEL_HOURLY_ONE = session.prepare(f"""
  SELECT last_updated, market_cap, volume_24h
  FROM {HOURLY_TABLE}
  WHERE id = ? AND ts = ? LIMIT 1
""")
SEL_PREV_CLOSE = session.prepare(f"""
  SELECT ts, close FROM {HOURLY_TABLE}
  WHERE id = ? AND ts < ? ORDER BY ts DESC LIMIT 1
""")

INS_10M = session.prepare(f"""
  INSERT INTO {TEN_MIN_TABLE}
    (id, ts, symbol, name, price_usd, market_cap, volume_24h,
     market_cap_rank, circulating_supply, total_supply, last_updated)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")
INS_DAY = session.prepare(f"""
  INSERT INTO {DAILY_TABLE}
    (id, date, symbol, name,
     open, high, low, close, price_usd,
     market_cap, volume_24h,
     market_cap_rank, circulating_supply, total_supply,
     candle_source, last_updated)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")
INS_HOURLY = session.prepare(f"""
  INSERT INTO {HOURLY_TABLE}
    (id, ts, symbol, name,
     open, high, low, close, price_usd,
     market_cap, volume_24h,
     market_cap_rank, circulating_supply, total_supply,
     candle_source, last_updated)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# Aggregate inserts
INS_MCAP_10M = session.prepare(f"""
  INSERT INTO {MCAP_10M_TABLE}
    (category, ts, last_updated, market_cap, market_cap_rank, volume_24h)
  VALUES (?, ?, ?, ?, ?, ?)
""")
INS_MCAP_HOURLY = session.prepare(f"""
  INSERT INTO {MCAP_HOURLY_TABLE}
    (category, ts, last_updated, market_cap, market_cap_rank, volume_24h)
  VALUES (?, ?, ?, ?, ?, ?)
""")
INS_MCAP_DAILY = session.prepare(f"""
  INSERT INTO {MCAP_DAILY_TABLE}
    (category, date, last_updated, market_cap, market_cap_rank, volume_24h)
  VALUES (?, ?, ?, ?, ?, ?)
""")

# Optional truncates for FULL mode
TRUNC_10M = SimpleStatement(f"TRUNCATE {MCAP_10M_TABLE}")
TRUNC_H   = SimpleStatement(f"TRUNCATE {MCAP_HOURLY_TABLE}")
TRUNC_D   = SimpleStatement(f"TRUNCATE {MCAP_DAILY_TABLE}")

# ---- Coverage prepared statements ----
PS_RANGES_FOR_ID = session.prepare(f"""
  SELECT start_date, end_date
  FROM {COIN_DAILY_RANGES}
  WHERE id = ?
""")
PS_BITS_RANGE_10M = session.prepare(f"""
  SELECT day, bitmap, set_count
  FROM {COIN_INTRADAY_BITS}
  WHERE id = ? AND granularity = 1 AND day >= ? AND day <= ?
""")
PS_BITS_RANGE_1H = session.prepare(f"""
  SELECT day, bitmap, set_count
  FROM {COIN_INTRADAY_BITS}
  WHERE id = ? AND granularity = 2 AND day >= ? AND day <= ?
""")

# ---------- Universe selection ----------
def top_assets(limit: int):
    rows = list(session.execute(SEL_LIVE, timeout=REQUEST_TIMEOUT))
    ranked = [r for r in rows if isinstance(getattr(r, "market_cap_rank", None), int) and r.market_cap_rank > 0]
    ranked.sort(key=lambda r: r.market_cap_rank)
    ranked = ranked[:limit]
    by_id = {r.id: r for r in ranked}

    id_to_cat = {}
    for r in rows:
        id_to_cat[r.id] = (getattr(r, "category", None) or "Other").strip() or "Other"

    # Optional: widen universe with anything that has daily history in our table
    avail_ids = set()
    if INCLUDE_ALL_DAILY_IDS:
        ids = [r.id for r in session.execute(SEL_DAILY_DISTINCT_IDS, timeout=REQUEST_TIMEOUT)]
        avail_ids = set(ids)

    extra_ids = set(x.strip().lower() for x in (os.getenv("INCLUDE_IDS") or "").split(",") if x.strip())
    want_ids = set(by_id.keys()) | set(avail_ids) | extra_ids

    def _mk_stub(cid: str):
        rec = type("Coin", (), {"id": cid, "symbol": (cid or "").upper(), "name": cid, "market_cap_rank": None})()
        return rec

    for cid in want_ids:
        if cid not in by_id:
            by_id[cid] = _mk_stub(cid)

    out = list(by_id.values())
    out.sort(key=lambda r: (0 if (getattr(r, "market_cap_rank", None) is not None) else 1,
                            getattr(r, "market_cap_rank", 10**9),
                            r.id))
    print(f"[{_now_str()}] Loaded {len(out)} assets for DQ (live_top={len(ranked)}, extras={len(want_ids)-len(ranked)})")
    return out, id_to_cat

def static_meta_for_coin(coin):
    """Return (rank, circ, tot) using what we know now. No time series."""
    rnk  = coin.get("market_cap_rank")
    # If you have these columns in TABLE_LIVE and want them, you can extend top_assets to fetch them too.
    circ = None
    tot  = None
    return rnk, circ, tot

def lookup_meta_for_id(coin_id: str, live_index: dict):
    live = live_index.get(coin_id)
    if live:
        return live.symbol, live.name, live.market_cap_rank
    return None, None, None

# ---------- Coverage presence helpers ----------
def dates_between(start_d: dt.date, end_d: dt.date) -> list[dt.date]:
    d = start_d; out = []
    while d <= end_d:
        out.append(d); d += dt.timedelta(days=1)
    return out

def covered_days_from_ranges(coin_id: str, start_d: dt.date, end_d: dt.date) -> tuple[set[dt.date], bool]:
    have, seen = set(), False
    rows = session.execute(PS_RANGES_FOR_ID, [coin_id], timeout=REQUEST_TIMEOUT)
    for r in rows:
        seen = True
        s = _to_pydate(r.start_date); e = _to_pydate(r.end_date)
        if e < start_d or s > end_d: 
            continue
        s2 = max(s, start_d); e2 = min(e, end_d)
        have.update(dates_between(s2, e2))
    return have, seen

def _decode_bitmap_hours(bitmap: bytes) -> list[bool]:
    ba = bytearray(bitmap or b"\x00\x00\x00")
    out = [False]*24
    for i in range(24):
        bidx = i // 8; off = i % 8
        out[i] = bool(ba[bidx] & (1 << off))
    return out

def missing_hours_from_coverage(coin_id: str, start_dt: dt.datetime, end_dt: dt.datetime) -> set[dt.datetime]:
    start_dt = ensure_utc_dt(start_dt, "start_dt")
    end_dt   = ensure_utc_dt(end_dt,   "end_dt")
    start_d, end_d = start_dt.date(), (end_dt - dt.timedelta(seconds=1)).date()
    want = set(hour_seq(start_dt, end_dt))
    rows = session.execute(PS_BITS_RANGE_1H, [coin_id, start_d, end_d], timeout=REQUEST_TIMEOUT)
    covered = set()
    by_day = {_to_pydate(r.day): _decode_bitmap_hours(getattr(r, "bitmap", b"")) for r in rows}
    cur = start_dt
    while cur < end_dt:
        d = cur.date()
        h = cur.hour
        if d in by_day and by_day[d][h]:
            covered.add(cur)
        cur += dt.timedelta(hours=1)
    return want - covered

def missing_days_10m_from_coverage(coin_id: str, start_d: dt.date, end_d: dt.date) -> set[dt.date]:
    want = set(dates_between(start_d, end_d))
    rows = session.execute(PS_BITS_RANGE_10M, [coin_id, start_d, end_d], timeout=REQUEST_TIMEOUT)
    have = set()
    for r in rows:
        d = _to_pydate(r.day)
        sc = int(getattr(r, "set_count", 0) or 0)
        if sc > 0:
            have.add(d)
    return want - have

# ---------- Optional quality scan for existing hourly ----------
def _any_row(iterator) -> bool:
    # Efficiently detect if a result set has at least one row (without buffering all)
    for _ in iterator:
        return True
    return False

def has_any_coverage_for_windows(coin_id: str,
                                 start_10m: dt.date,
                                 last_incl: dt.date,
                                 days_hourly: int) -> bool:
    """True if we have ANY coverage rows for this id (daily ranges OR intraday bitmaps) in our windows."""
    # Daily coverage ranges (any run for this id anywhere)
    if _any_row(session.execute(PS_RANGES_FOR_ID, [coin_id], timeout=REQUEST_TIMEOUT)):
        return True

    # 10m window
    if _any_row(session.execute(PS_BITS_RANGE_10M, [coin_id, start_10m, last_incl], timeout=REQUEST_TIMEOUT)):
        return True

    # 1h window
    start_1h = last_incl - dt.timedelta(days=days_hourly - 1)
    if _any_row(session.execute(PS_BITS_RANGE_1H, [coin_id, start_1h, last_incl], timeout=REQUEST_TIMEOUT)):
        return True

    return False

def bad_hours_hourly(coin_id: str, start_dt: dt.datetime, end_dt: dt.datetime):
    bad = set()
    rows = session.execute(SEL_HOURLY_META_RANGE, [coin_id, to_utc(start_dt), to_utc(end_dt)], timeout=REQUEST_TIMEOUT)
    for r in rows:
        ts_raw = getattr(r, "ts", None)
        ts = floor_to_hour_utc(ts_raw) if isinstance(ts_raw, dt.datetime) else None
        if ts is None: continue
        mcap = r.market_cap
        price = r.price_usd
        csrc = getattr(r, "candle_source", None) or ""
        needs_mcap = (mcap is None) or (isinstance(mcap, (int, float)) and mcap <= 0.0)
        is_interpolated = (csrc in ("hourly_interp",))
        inconsistent = (price is not None) and (mcap is None or mcap <= 0.0)
        if needs_mcap or is_interpolated or inconsistent:
            bad.add(ts)
    return bad

# ---------- Equality ----------
def _daily_row_equal(existing, o,h,l,c,mcap,vol,source):
    if not existing: return False
    def eq(a,b,eps=1e-12):
        if a is None and b is None: return True
        if a is None or b is None:  return False
        try: return abs(float(a)-float(b)) <= eps
        except Exception: return False
    return (
        eq(getattr(existing,"open",None),  o) and
        eq(getattr(existing,"high",None),  h) and
        eq(getattr(existing,"low",None),   l) and
        eq(getattr(existing,"close",None), c) and
        eq(getattr(existing,"market_cap",None), mcap) and
        eq(getattr(existing,"volume_24h",None), vol) and
        (getattr(existing,"candle_source",None) == source)
    )

# ---------- Repairs ----------
def repair_daily_from_10m(coin, missing_days: set[dt.date]) -> int:
    if not (FIX_DAILY_FROM_10M and missing_days): return 0
    print(f"[{_now_str()}]    [repair_daily_from_10m] {coin['symbol']}: {len(missing_days)} day(s)")
    t0 = perf_counter(); cnt = 0
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for day in sorted(missing_days):
        day_start, day_end_excl = day_bounds_utc(day)
        pts = list(session.execute(SEL_10M_RANGE_FULL, [coin["id"], day_start, day_end_excl], timeout=REQUEST_TIMEOUT))
        prices, mcaps, vols, ranks, circs, tots = [], [], [], [], [], []
        last_ts = None
        for p in pts:
            last_ts = to_utc(p.ts) or last_ts
            if p.price_usd is not None: prices.append(float(p.price_usd))
            if p.market_cap is not None: mcaps.append(float(p.market_cap))
            if p.volume_24h is not None: vols.append(float(p.volume_24h))
            if getattr(p,"market_cap_rank",None) is not None: ranks.append(int(p.market_cap_rank))
            if getattr(p,"circulating_supply",None) is not None: circs.append(float(p.circulating_supply))
            if getattr(p,"total_supply",None) is not None: tots.append(float(p.total_supply))
        if not prices: continue
        o,h,l,c = prices[0], max(prices), min(prices), prices[-1]
        mcap = mcaps[-1] if mcaps else None
        vol  = vols[-1]  if vols  else None
        rnk  = ranks[-1] if ranks else None
        circ = circs[-1] if circs else None
        tot  = tots[-1]  if tots  else None
        last_upd = last_ts or (day_end_excl - dt.timedelta(seconds=1))

        existing = session.execute(SEL_DAILY_ONE, [coin["id"], day], timeout=REQUEST_TIMEOUT).one()
        if _daily_row_equal(existing, o,h,l,c,mcap,vol,"10m_final"): continue

        if not DRY_RUN:
            batch.add(INS_DAY, (
                coin["id"], day, coin["symbol"], coin["name"],
                o,h,l,c,c,
                mcap, vol,
                rnk, circ, tot,
                "10m_final", last_upd
            ))
        cnt += 1
        if (cnt % 100 == 0) and not DRY_RUN:
            session.execute(batch); batch.clear()
    if not DRY_RUN and len(batch): session.execute(batch)
    print(f"[{_now_str()}]    [repair_daily_from_10m] {coin['symbol']} wrote={cnt} ({tdur(t0)})")
    return cnt

def seed_10m_from_daily(coin, missing_days: set[dt.date]) -> int:
    if not (SEED_10M_FROM_DAILY and missing_days): return 0
    print(f"[{_now_str()}]    [seed_10m] {coin['symbol']}: {len(missing_days)} day(s)")
    t0 = perf_counter(); cnt = 0
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for day in sorted(missing_days):
        row = session.execute(SEL_DAILY_ONE, [coin["id"], day], timeout=REQUEST_TIMEOUT).one()
        if not row: continue
        close = float(row.close if row.close is not None else (row.price_usd or 0.0))
        ts_ = dt.datetime(day.year,day.month,day.day,23,59,59,tzinfo=timezone.utc)
        mcap = float(row.market_cap or 0.0)
        vol  = float(row.volume_24h or 0.0)
        rnk  = getattr(row,"market_cap_rank",None)
        circ = float(row.circulating_supply) if row.circulating_supply is not None else None
        tot  = float(row.total_supply)       if row.total_supply       is not None else None
        last_updated = to_utc(getattr(row,"last_updated",None)) or ts_
        if not DRY_RUN:
            batch.add(INS_10M, (
                coin["id"], ts_, coin["symbol"], coin["name"], close, mcap, vol,
                rnk, circ, tot, last_updated
            ))
        cnt += 1
        if (cnt % 200 == 0) and not DRY_RUN:
            session.execute(batch); batch.clear()
    if not DRY_RUN and len(batch)>0: session.execute(batch)
    print(f"[{_now_str()}]    [seed_10m] {coin['symbol']} wrote={cnt} ({tdur(t0)})")
    return cnt

def backfill_daily_from_api_ranges(coin, need_daily: set[dt.date], dt_ranges: list[tuple[dt.datetime, dt.datetime]]) -> int:
    if not (BACKFILL_DAILY_FROM_API and need_daily and dt_ranges): return 0
    all_prices, all_mcaps, all_vols = [], [], []
    for (start_dt, end_dt) in dt_ranges:
        try:
            sd = ensure_utc_dt(start_dt, "start_dt")
            ed = ensure_utc_dt(end_dt, "end_dt")
            data = http_get(
                f"/coins/{coin['id']}/market_chart/range",
                params={"vs_currency":"usd","from":int(sd.timestamp()),"to":int(ed.timestamp())}
            )
        except RateLimitDefer as e:
            metrics["cg_calls_deferred"] += 1
            wait_s = (e.retry_after or BACKOFF_S) + random.uniform(0.0, 0.25)
            print(f"[{_now_str()}]      · window {start_dt.date()}→{end_dt.date()} rate-limited; sleeping {wait_s:.2f}s then deferring")
            time.sleep(wait_s); continue
        except Exception as e:
            print(f"[{_now_str()}]      · window {start_dt.date()}→{end_dt.date()} failed: {e}")
            continue
        prices = data.get("prices", []) or []
        mcaps  = data.get("market_caps", []) or []
        vols   = data.get("total_volumes", []) or []
        print(f"[{_now_str()}]      · window {start_dt.date()}→{end_dt.date()} sizes: prices={len(prices)} mcaps={len(mcaps)} vols={len(vols)}")
        all_prices.extend(prices); all_mcaps.extend(mcaps); all_vols.extend(vols)
        time.sleep(PAUSE_S + random.uniform(0.0,0.25))

    if not all_prices:
        print(f"[{_now_str()}]    [api] no payload for {coin['symbol']} → skip (wrote=0)")
        add_to_neg_cache(coin["id"]); return 0

    days = sorted(need_daily)
    start_day, end_day = days[0], days[-1]
    ohlc   = bucket_daily_ohlc(all_prices, start_day, end_day)
    m_last = bucket_daily_last(all_mcaps,  start_day, end_day)
    v_last = bucket_daily_last(all_vols,   start_day, end_day)
    print(f"[{_now_str()}]    [api] bucketed days price={len(ohlc)}")

    if INTERPOLATE_MCAP_VOL_DAILY:
        m_series = interpolate_daily_series(days, {d: (m_last.get(d,{}).get("val")) for d in days}, max_gap_days=INTERP_MAX_DAYS)
        v_series = interpolate_daily_series(days, {d: (v_last.get(d,{}).get("val")) for d in days}, max_gap_days=INTERP_MAX_DAYS)
    else:
        m_series = {d: (m_last.get(d,{}).get("val")) for d in days}
        v_series = {d: (v_last.get(d,{}).get("val")) for d in days}

    cur_rank, cur_circ, cur_tot = static_meta_for_coin(coin)

    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    cnt_daily = 0; prev_close = None
    for d in days:
        row  = ohlc.get(d); mcap = m_series.get(d); vol = v_series.get(d)
        if not row: continue
        o,h,l,c,last_ts = row["open"],row["high"],row["low"],row["close"],row["last_ts"]
        if row.get("is_true_ohlc"): csrc = "hourly"
        else:
            if prev_close is None: o = h = l = c; csrc = "flat"
            else:
                o = float(prev_close); h = max(o,c); l = min(o,c); csrc = "prev_close"
        day_end = day_bounds_utc(d)[1] - dt.timedelta(seconds=1)

        existing = session.execute(SEL_DAILY_ONE, [coin["id"], d], timeout=REQUEST_TIMEOUT).one()
        if _daily_row_equal(existing, o,h,l,c,mcap,vol,csrc):
            prev_close = c; continue

        batch.add(INS_DAY, (
            coin["id"], d, coin["symbol"], coin["name"],
            float(o),float(h),float(l),float(c),float(c),
            float(mcap) if mcap is not None else None,
            float(vol)  if vol  is not None else None,
            cur_rank, cur_circ, cur_tot,
            csrc, last_ts if row.get("is_true_ohlc") else day_end
        ))
        cnt_daily += 1; prev_close = c
        if (cnt_daily % 100 == 0): session.execute(batch); batch.clear()
    if len(batch): session.execute(batch)
    print(f"[{_now_str()}]    [api] {coin['symbol']} wrote={cnt_daily}")
    return cnt_daily

def fetch_hourly_from_api(coin_id: str, start_dt: dt.datetime, end_dt: dt.datetime):
    step_s = 90 * 3600
    sd = ensure_utc_dt(start_dt, "start_dt")
    ed = ensure_utc_dt(end_dt, "end_dt")
    t0 = int(sd.timestamp()); t1 = int(ed.timestamp())
    prices_all, mcaps_all, vols_all = [], [], []
    cur = t0
    while cur < t1:
        nxt = min(cur + step_s, t1)
        try:
            data = http_get(
                f"/coins/{coin_id}/market_chart/range",
                params={"vs_currency":"usd","from":cur,"to":nxt}
            )
            prices_all.extend(data.get("prices", []) or [])
            mcaps_all.extend(data.get("market_caps", []) or [])
            vols_all.extend(data.get("total_volumes", []) or [])
        except RateLimitDefer as e:
            metrics["cg_calls_deferred"] += 1
            wait_s = (e.retry_after or BACKOFF_S) + random.uniform(0.0, 0.25)
            print(f"[{_now_str()}]  · hourly chunk {dt.datetime.fromtimestamp(cur, tz=timezone.utc)}→{dt.datetime.fromtimestamp(nxt, tz=timezone.utc)} 429; sleep {wait_s:.2f}s")
            time.sleep(wait_s)
        except Exception as ex:
            print(f"[{_now_str()}]  · hourly chunk fetch failed: {ex}")
        time.sleep(PAUSE_S + random.uniform(0.0, 0.15))
        cur = nxt

    def to_hour_map(arr):
        m = {}
        for ms, v in (arr or []):
            ts_s = (ms/1000.0) if (ms and ms > 10_000_000_000) else float(ms or 0.0)
            ts_ = dt.datetime.fromtimestamp(ts_s, tz=timezone.utc)
            hr  = floor_to_hour_utc(ts_); m[hr] = (float(v) if v is not None else None, ts_)
        return m
    return to_hour_map(prices_all), to_hour_map(mcaps_all), to_hour_map(vols_all)

def repair_hourly_from_api_or_interp(coin, missing_hours: set[dt.datetime]) -> int:
    if not (FILL_HOURLY and missing_hours): return 0
    hours_sorted = sorted([floor_to_hour_utc(h) for h in missing_hours])
    start_dt = floor_to_hour_utc(hours_sorted[0] - dt.timedelta(hours=6))
    end_dt   = floor_to_hour_utc(hours_sorted[-1] + dt.timedelta(hours=7))
    p_map = mc_map = v_map = {}
    if FILL_HOURLY_FROM_API:
        try:
            p_map, mc_map, v_map = fetch_hourly_from_api(coin["id"], start_dt, end_dt)
        except RateLimitDefer as e:
            metrics["cg_calls_deferred"] += 1
            wait_s = (getattr(e, "retry_after", None) or BACKOFF_S) + random.uniform(0.0, 0.25)
            print(f"[{_now_str()}]  · hourly API fetch rate-limited for {coin['symbol']} — sleeping {wait_s:.2f}s then deferring")
            time.sleep(wait_s)
            if not INTERPOLATE_IF_API_MISS: return 0
        except Exception as e:
            print(f"[{_now_str()}]  · hourly API fetch failed for {coin['symbol']}: {e}")
            if not INTERPOLATE_IF_API_MISS: return 0
    api_hours = sorted(p_map.keys())

    def interp_price(h):
        if not api_hours: return (None, None)
        left = right = None
        for ah in reversed([a for a in api_hours if a < h]):
            if p_map.get(ah,(None,None))[0] is not None: left = ah; break
        for ah in [a for a in api_hours if a > h]:
            if p_map.get(ah,(None,None))[0] is not None: right = ah; break
        if left is None or right is None: return (None, None)
        pl,_ = p_map[left]; pr,_ = p_map[right]
        if pl is None or pr is None: return (None, None)
        span = (right-left).total_seconds(); w = (h-left).total_seconds()/span if span>0 else 0.0
        price = pl + (pr-pl)*w; return (float(price), to_utc(right))

    def interp_generic(h, amap, max_gap_hours):
        if not amap: return None
        known_hours = sorted([k for k,(v,_) in amap.items() if v is not None])
        if not known_hours: return None
        if h in amap and amap[h][0] is not None: return float(amap[h][0])
        left = max([k for k in known_hours if k < h], default=None)
        right = min([k for k in known_hours if k > h], default=None)
        if left is None and right is None: return None
        if left is None: return float(amap[right][0])
        if right is None: return float(amap[left][0])
        gap = int((right-left).total_seconds()//3600)
        if (max_gap_hours is not None) and (gap>max_gap_hours): return float(amap[left][0])
        y = _interp_linear(left.timestamp(), float(amap[left][0]), right.timestamp(), float(amap[right][0]), h.timestamp())
        return float(y)

    prev_row = session.execute(SEL_PREV_CLOSE, [coin["id"], hours_sorted[0]], timeout=REQUEST_TIMEOUT).one()
    prev_close = float(prev_row.close) if (prev_row and prev_row.close is not None) else None

    cur_rank, cur_circ, cur_tot = static_meta_for_coin(coin)


    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    wrote = 0
    for h in hours_sorted:
        h = floor_to_hour_utc(h)
        if h in p_map and p_map[h][0] is not None:
            price, ts_src = p_map[h][0], p_map[h][1]; source = "hourly_api"
        else:
            price, ts_src = interp_price(h); source = "hourly_interp" if price is not None else None
        if price is None: continue

        if prev_close is None: o=hih=lo=c=float(price)
        else:
            o=float(prev_close); c=float(price); hih=max(o,c); lo=min(o,c)

        if mc_map and (h in mc_map) and (mc_map[h][0] is not None): mcap = float(mc_map[h][0])
        elif INTERPOLATE_MCAP_VOL_HOURLY: mcap = interp_generic(h, mc_map, INTERP_MAX_HOURS)
        else: mcap = None

        if v_map and (h in v_map) and (v_map[h][0] is not None): vol = float(v_map[h][0])
        elif INTERPOLATE_MCAP_VOL_HOURLY: vol = interp_generic(h, v_map, INTERP_MAX_HOURS)
        else: vol = None

        hr_end = h + dt.timedelta(hours=1) - dt.timedelta(seconds=1)
        last_upd = ts_src or hr_end

        if not DRY_RUN:
            batch.add(INS_HOURLY, (
                coin["id"], h, coin["symbol"], coin["name"],
                o, hih, lo, c, c,
                float(mcap) if mcap is not None else None,
                float(vol)  if vol  is not None else None,
                cur_rank, cur_circ, cur_tot,
                source, last_upd
            ))
        wrote += 1; prev_close = c
        if (wrote % 128 == 0) and not DRY_RUN:
            session.execute(batch); batch.clear()
    if not DRY_RUN and len(batch): session.execute(batch)
    print(f"[{_now_str()}]    [hourly] {coin['symbol']} wrote={wrote}")
    return wrote

# ---------- Neg cache ----------
def load_neg_cache():
    try:
        with open(NEG_CACHE_PATH, "r", encoding="utf-8") as f:
            return {line.strip() for line in f if line.strip()}
    except FileNotFoundError:
        return set()
def add_to_neg_cache(coin_id: str):
    try:
        with open(NEG_CACHE_PATH, "a", encoding="utf-8") as f:
            f.write(coin_id + "\n")
    except Exception as e:
        print(f"[{_now_str()}] [WARN] could not update neg cache: {e}")

# ---------- Helpers for contiguous ranges ----------
def group_contiguous_dates(dates: set[dt.date]) -> list[tuple[dt.date, dt.date]]:
    if not dates: return []
    sdates = sorted(dates); runs = []; run_start = run_end = sdates[0]
    for d in sdates[1:]:
        if d == run_end + dt.timedelta(days=1): run_end = d
        else: runs.append((run_start, run_end)); run_start = run_end = d
    runs.append((run_start, run_end)); return runs

def clamp_date_range(s: dt.date, e: dt.date, max_days: int):
    out = []; cur = s
    while cur <= e:
        end = min(e, cur + dt.timedelta(days=max_days-1))
        out.append((cur, end)); cur = end + dt.timedelta(days=1)
    return out

# ---------- Aggregate helpers ----------
def compute_ranks(entries):
    non_all = [(cat, m) for (cat, m) in entries if cat != "ALL"]
    non_all.sort(key=lambda x: (x[1] or 0.0), reverse=True)
    ranks = {cat: i+1 for i,(cat,_m) in enumerate(non_all)}
    ranks["ALL"] = 0
    return ranks

def write_hourly_aggregates_from_map(hour_totals_map):
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    total = 0
    for ts_hr, catmap in sorted(hour_totals_map.items()):
        entries = [(c, catmap[c][1]) for c in catmap]
        ranks = compute_ranks(entries)
        for cat, (lu, mc, vol) in catmap.items():
            rk = ranks.get(cat, None)
            if DRY_RUN: continue
            batch.add(INS_MCAP_HOURLY, [cat, ts_hr, lu or (ts_hr + dt.timedelta(hours=1) - dt.timedelta(seconds=1)), float(mc or 0.0), rk, float(vol or 0.0)])
            total += 1
            if (total % 100) == 0: session.execute(batch); batch.clear()
    if not DRY_RUN and len(batch): session.execute(batch)
    metrics["agg_rows_hourly"] += total
    print(f"[{_now_str()}] [mcap_hourly] wrote rows={total}")

def write_daily_aggregates_from_map(day_totals_map):
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    total = 0
    for d, catmap in sorted(day_totals_map.items()):
        entries = [(c, catmap[c][1]) for c in catmap]
        ranks = compute_ranks(entries)
        for cat, (lu, mc, vol) in catmap.items():
            rk = ranks.get(cat, None)
            if DRY_RUN: continue
            batch.add(INS_MCAP_DAILY, [cat, d, lu or (dt.datetime(d.year,d.month,d.day,23,59,59,tzinfo=timezone.utc)), float(mc or 0.0), rk, float(vol or 0.0)])
            total += 1
            if (total % 100) == 0: session.execute(batch); batch.clear()
    if not DRY_RUN and len(batch): session.execute(batch)
    metrics["agg_rows_daily"] += total
    print(f"[{_now_str()}] [mcap_daily] wrote rows={total}")

def write_10m_aggregates_from_map(min10_totals_map):
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    total = 0
    for ts10, catmap in sorted(min10_totals_map.items()):
        entries = [(c, catmap[c][1]) for c in catmap]
        ranks = compute_ranks(entries)
        for cat, (lu, mc, vol) in catmap.items():
            rk = ranks.get(cat, None)
            if DRY_RUN: continue
            batch.add(INS_MCAP_10M, [cat, ts10, lu or ts10, float(mc or 0.0), rk, float(vol or 0.0)])
            total += 1
            if (total % 200) == 0: session.execute(batch); batch.clear()
    if not DRY_RUN and len(batch): session.execute(batch)
    metrics["agg_rows_10m"] += total
    print(f"[{_now_str()}] [mcap_10m] wrote rows={total}")

def recompute_daily_full_all(coins, cat_for_id):
    print(f"[{_now_str()}] [FULL][daily_all] begin full-table recompute across {len(coins)} coins")
    acc = {}
    def add_point(d, cat, lu, mcap, vol):
        catmap = acc.setdefault(d, {})
        lu0, m0, v0 = catmap.get(cat, (None, 0.0, 0.0))
        lu_new = lu if (lu0 is None or (lu and lu > lu0)) else lu0
        catmap[cat] = (lu_new, m0 + mcap, v0 + vol)
        alu0, am0, av0 = catmap.get("ALL", (None, 0.0, 0.0))
        alu_new = lu if (alu0 is None or (lu and lu > alu0)) else alu0
        catmap["ALL"] = (alu_new, am0 + mcap, av0 + vol)

    for i, c in enumerate(coins, 1):
        if (i == 1) or (i % 25 == 0) or (i == len(coins)):
            print(f"[{_now_str()}] [FULL][daily_all] {i}/{len(coins)}: {c['id']}")
            _maybe_heartbeat(f"FULL daily_all coin {i}/{len(coins)}")
        srow = session.execute(SEL_DAILY_FIRST, [c["id"]], timeout=REQUEST_TIMEOUT).one()
        erow = session.execute(SEL_DAILY_LAST,  [c["id"]], timeout=REQUEST_TIMEOUT).one()
        if not srow or not erow: continue
        sdate = _to_pydate(srow.date); edate = _to_pydate(erow.date)
        rows = session.execute(SEL_DAILY_FOR_ID_RANGE_ALL, [c["id"], sdate, edate], timeout=REQUEST_TIMEOUT)
        for r in rows:
            _maybe_heartbeat("FULL daily_all streaming rows")
            d = _to_pydate(r.date)
            lu = to_utc(getattr(r, "last_updated", None)) or dt.datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=timezone.utc)
            mcap = float(getattr(r, "market_cap", 0.0) or 0.0)
            vol  = float(getattr(r, "volume_24h", 0.0) or 0.0)
            cat  = cat_for_id(c["id"])
            add_point(d, cat, lu, mcap, vol)
    write_daily_aggregates_from_map(acc)

# ---------- Main ----------
def main():
    # Light ping to fail fast on auth/network — only if we plan to call CG
    needs_api = (FILL_HOURLY and FILL_HOURLY_FROM_API) or BACKFILL_DAILY_FROM_API
    if needs_api:
        # Light ping to fail fast on auth/network
        try: http_get("/ping")
        except Exception as e: raise SystemExit(f"[FATAL] CoinGecko preflight failed: {e}")

    now = utcnow()
    end_excl = dt.datetime(now.year,now.month,now.day,tzinfo=timezone.utc) + dt.timedelta(days=1)
    global start_daily_date, last_inclusive
    last_inclusive   = end_excl.date() - dt.timedelta(days=1)
    start_10m_date   = last_inclusive - dt.timedelta(days=DAYS_10M-1)
    start_daily_date = last_inclusive - dt.timedelta(days=DAYS_DAILY-1)

    if DQ_MODE == "NIGHTLY":
        start_daily_date = last_inclusive  # only yesterday for daily

    want_10m_days   = set(date_seq(last_inclusive, DAYS_10M))
    want_daily_days = set(date_seq(last_inclusive, DAYS_DAILY))

    # Universe
    live, id_to_cat_map = top_assets(TOP_N_DQ)
    live_index = {r.id: r for r in live}

    if DQ_MODE == "INTRADAY":
        # Use only live universe for intraday to avoid long-tail API churn
        if INCLUDE_UNRANKED_INTRADAY:
            intraday_live = list(live)  # includes items without rank if top_assets returned them
        else:
            intraday_live = [r for r in live if isinstance(getattr(r, "market_cap_rank", None), int)]

        intraday_live.sort(key=lambda r: r.market_cap_rank if r.market_cap_rank is not None else 10**9)
        intraday_live = intraday_live[:INTRADAY_TOP_N]
        universe_ids = {r.id for r in intraday_live}

        print(f"[{_now_str()}] Universe (INTRADAY): live_only={len(universe_ids)} (top_n={INTRADAY_TOP_N}, include_unranked={INCLUDE_UNRANKED_INTRADAY})")

    else:
        # NIGHTLY: keep the wide daily universe so we can fix daily gaps everywhere
        universe_ids = {r.id for r in live}
        if INCLUDE_ALL_DAILY_IDS:
            ids = [r.id for r in session.execute(SEL_DAILY_DISTINCT_IDS, timeout=REQUEST_TIMEOUT)]
            universe_ids |= set(ids)
        print(f"[{_now_str()}] Universe (NIGHTLY): live={len(live)} all_daily_ids={'on' if INCLUDE_ALL_DAILY_IDS else 'off'} → total_ids={len(universe_ids)}")

    # Optional global cap, still applied deterministically
    if DQ_MAX_COINS and len(universe_ids) > DQ_MAX_COINS:
        print(f"[{_now_str()}] Universe {len(universe_ids)} > cap {DQ_MAX_COINS}; trimming deterministically.")
        universe_ids = set(sorted(universe_ids)[:DQ_MAX_COINS])

    coins = []
    for cid in sorted(universe_ids):
        symbol, name, rank = lookup_meta_for_id(cid, live_index)
        coins.append({"id": cid, "symbol": symbol or cid, "name": name or cid, "market_cap_rank": rank})

    def cat_for_id(cid: str) -> str:
        return (id_to_cat_map.get(cid) or "Other").strip() or "Other"

    print(f"[{_now_str()}] DQ windows → 10m: {start_10m_date}→{last_inclusive} | daily: {start_daily_date}→{last_inclusive}")
    print(f"[{_now_str()}] Universe size: {len(coins)}")

    # Hard guard by mode: we only *plan* the work we’re allowed to do
    if DQ_MODE == "INTRADAY":
        # Nightly-only tasks off
        # (daily window is still computed above but will remain unused)
        pass
    elif DQ_MODE == "NIGHTLY":
        # Intraday repair off (unless explicitly turned on via env)
        # The env already controls these, this is just a sanity echo:
        if FILL_HOURLY:
            print(f"[{_now_str()}] [NOTE] NIGHTLY with FILL_HOURLY=1 — hourly repair will run (allowed by env).")

    fixedD_local = fixed10_seeded = fixedD_api = 0
    fixedH_total = 0
    t_all = perf_counter()
    neg_cache = load_neg_cache()
    api_plans = []
    deadline = perf_counter() + SOFT_BUDGET_SEC

    affected_hours = set()
    affected_days  = set()
    affected_10m_ts = set()

    for i, coin in enumerate(coins, 1):
        t_coin = perf_counter()
        try:
            # ── Neg-cache fast skip
            if coin["id"] in neg_cache:
                print(f"[{_now_str()}] → Coin {i}/{len(coins)}: {coin['symbol']} in negative cache — skip")
                continue

            print(f"[{_now_str()}] → Coin {i}/{len(coins)}: {coin['symbol']} ({coin['id']}) rank={coin.get('market_cap_rank')}")

            # ── Optional guard: brand-new / no-coverage coins
            if SKIP_UNCOVERED_COINS:
                if not has_any_coverage_for_windows(coin["id"], start_10m_date, last_inclusive, DAYS_HOURLY):
                    print(f"[{_now_str()}]    no coverage rows yet → skip (new/unseen coin)")
                    continue

            # DAILY via coverage ranges
            _res_have_daily = covered_days_from_ranges(coin["id"], start_daily_date, last_inclusive)
            if isinstance(_res_have_daily, tuple):
                have_daily, _ = _res_have_daily          # (set[date], bool)
            else:
                have_daily = _res_have_daily             # set[date]

            need_daily_all = want_daily_days - have_daily

            # Of missing daily, which have any 10m coverage that day?
            need_daily_local: set[dt.date] = set()
            need_daily_api:   set[dt.date] = set()
            if RUNS_ANY_DAILY:
                have_daily, _ = covered_days_from_ranges(coin["id"], start_daily_date, last_inclusive)
                need_daily_all = want_daily_days - have_daily

                if need_daily_all:
                    missing_10m_days = missing_days_10m_from_coverage(coin["id"], min(need_daily_all), max(need_daily_all))
                    have_10m_on_day = need_daily_all - missing_10m_days
                    need_daily_local = have_10m_on_day
                    need_daily_api   = need_daily_all - need_daily_local
            else:
                need_daily_all = set()

            # 10m seeding: days in 10m window with no 10m coverage
            _need_10m = missing_days_10m_from_coverage(coin["id"], start_10m_date, last_inclusive)
            if isinstance(_need_10m, tuple):
                need_10m, _ = _need_10m
            else:
                need_10m = _need_10m

            if RUNS_INTRADAY and not RUNS_ANY_DAILY:
                print(f"[{_now_str()}]    Missing → 10m:{len(need_10m)}")
            else:
                print(f"[{_now_str()}]    Missing → 10m:{len(need_10m)} "
                    f"daily_total:{len(need_daily_all)} (10m_eligible:{len(need_daily_local)}, api:{len(need_daily_api)})")

            # Local daily repair from 10m
            if need_daily_local and FIX_DAILY_FROM_10M:
                fixed = repair_daily_from_10m(coin, need_daily_local)
                affected_days |= set(sorted(need_daily_local))
                fixedD_local += fixed
                if fixed > 0:
                    metrics["points_daily"] += fixed
                    coin_fix_counts[coin["id"]] += fixed

            # 10m seed from daily (optional)
            if need_10m and SEED_10M_FROM_DAILY:
                fixed = seed_10m_from_daily(coin, need_10m)
                for d in need_10m:
                    affected_10m_ts.add(dt.datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=timezone.utc))
                fixed10_seeded += fixed
                if fixed > 0:
                    metrics["points_10m"] += fixed
                    coin_fix_counts[coin["id"]] += fixed

            # HOURLY via coverage (+ optional quality sweep)
            if FILL_HOURLY:
                start_hour_dt = dt.datetime.combine(
                    last_inclusive - dt.timedelta(days=DAYS_HOURLY - 1),
                    dt.time.min,
                    tzinfo=timezone.utc,
                )
                end_hour_dt = dt.datetime.combine(
                    last_inclusive + dt.timedelta(days=1),
                    dt.time.min,
                    tzinfo=timezone.utc,
                )

                _missing_hours = missing_hours_from_coverage(coin["id"], start_hour_dt, end_hour_dt)
                if isinstance(_missing_hours, tuple):
                    missing_hours, _ = _missing_hours
                else:
                    missing_hours = _missing_hours

                bad_existing = bad_hours_hourly(coin["id"], start_hour_dt, end_hour_dt) if FILL_HOURLY_BADSCAN else set()
                need_hours = missing_hours | bad_existing

                print(
                    f"[{_now_str()}]    Hourly window: missing={len(missing_hours)} "
                    f"bad_existing={len(bad_existing)} → to_repair={len(need_hours)} over {DAYS_HOURLY}d"
                )

                if need_hours:
                    try:
                        wroteH = repair_hourly_from_api_or_interp(coin, need_hours)
                        affected_hours |= set(need_hours)
                        print(f"[{_now_str()}]    hourly_repair → wrote {wroteH} rows")
                        fixedH_total += wroteH
                        if wroteH > 0:
                            metrics["points_hourly"] += wroteH
                            coin_fix_counts[coin["id"]] += wroteH
                    except RateLimitDefer:
                        print(f"[{_now_str()}]    [WARN] hourly repair deferred due to rate limit for {coin['symbol']}")
                    except Exception as e:
                        print(f"[{_now_str()}]    [WARN] hourly repair failed for {coin['symbol']}: {e}")

            # Plan API ranges for remaining daily gaps
            remaining_daily = need_daily_api
            if remaining_daily and BACKFILL_DAILY_FROM_API:
                runs = group_contiguous_dates(remaining_daily)
                padded: list[tuple[dt.date, dt.date]] = []
                for s, e in runs:
                    s_pad = max(start_daily_date, s - dt.timedelta(days=PAD_DAYS))
                    e_pad = min(last_inclusive,   e + dt.timedelta(days=PAD_DAYS))
                    padded.extend(clamp_date_range(s_pad, e_pad, MAX_RANGE_DAYS))

                dt_ranges: list[tuple[dt.datetime, dt.datetime]] = []
                for s, e in padded:
                    start_dt3 = dt.datetime(s.year, s.month, s.day, 0, 0, 0, tzinfo=timezone.utc)
                    end_dt3   = dt.datetime(e.year, e.month, e.day, 23, 59, 59, tzinfo=timezone.utc)
                    dt_ranges.append((start_dt3, end_dt3))

                if dt_ranges:
                    api_plans.append({"coin": coin, "ranges": dt_ranges, "need_days": remaining_daily})
                    affected_days |= set(remaining_daily)

            sleep(PAUSE_S)
            elapsed_coin = perf_counter() - t_coin
            if (i % LOG_EVERY == 0) or VERBOSE:
                print(f"[{_now_str()}] ← Done {coin['symbol']} in {elapsed_coin:.2f}s  (progress {i}/{len(coins)})")

            if perf_counter() >= deadline:
                print(f"[{_now_str()}] [WARN] Time budget exhausted at {i}/{len(coins)}; stopping early.")
                break

        except Exception as e:
            # Avoid referencing possibly-unbound 'coin' if something blew up before the loop body
            _sym = coin['symbol'] if 'coin' in locals() and isinstance(coin, dict) else '?'
            _cid = coin['id']     if 'coin' in locals() and isinstance(coin, dict) else '?'
            print(f"[{_now_str()}] [WARN] coin {_sym} ({_cid}) failed: {e}")
            continue



    # Execute API plan within request budget
    if BACKFILL_DAILY_FROM_API and api_plans:
        def _plan_reqs(p): return len(p["ranges"])
        api_plans.sort(key=_plan_reqs)
        reqs_left = DAILY_API_REQ_BUDGET; coins_done = 0
        for plan in api_plans:
            if reqs_left <= 0: break
            coin = plan["coin"]; ranges = []
            for r in plan["ranges"]:
                if reqs_left <= 0: break
                ranges.append(r); reqs_left -= 1
            if not ranges: break
            try:
                wrote_api = backfill_daily_from_api_ranges(coin, plan["need_days"], ranges)
                fixedD_api += wrote_api; coins_done += 1
                if wrote_api > 0:
                    metrics["points_daily"] += wrote_api
                    coin_fix_counts[coin["id"]] += wrote_api
            except RateLimitDefer as e:
                metrics["cg_calls_deferred"] += 1
                wait_s = (getattr(e, "retry_after", None) or BACKOFF_S) + random.uniform(0.0, 0.25)
                print(f"[{_now_str()}]  · API repair deferred for {coin['symbol']} — sleeping {wait_s:.2f}s")
                time.sleep(wait_s)
            except Exception as e:
                print(f"[{_now_str()}]  · API repair failed for {coin['symbol']}: {e}")
            sleep(PAUSE_S)
        remaining = max(0, len(api_plans) - coins_done)
        print(f"[{_now_str()}] API pass done: coins={coins_done} daily_rows={fixedD_api} remaining_coins={remaining} (req_budget_left={reqs_left})")
    else:
        print(f"[{_now_str()}] No API pass needed or disabled.")

    # ───────────────────── Aggregate recompute ─────────────────────
    print(f"[{_now_str()}] Aggregate recompute: FULL_MODE={FULL_MODE} | recompute 10m={RECOMPUTE_MCAP_10M} hourly={RECOMPUTE_MCAP_HOURLY} daily={RECOMPUTE_MCAP_DAILY}")

    def recompute_hourly_incremental(hours_set):
        if not hours_set: return
        print(f"[{_now_str()}] [mcap_hourly] incremental timestamps={len(hours_set)} (summing across {len(coins)} coins)")
        hour_map = {}
        for h in sorted(hours_set):
            catmap = defaultdict(lambda: (None, 0.0, 0.0))
            for c in coins:
                row = session.execute(SEL_HOURLY_ONE, [c["id"], h], timeout=REQUEST_TIMEOUT).one()
                if not row: continue
                lu = to_utc(getattr(row, "last_updated", None))
                mcap = float(getattr(row, "market_cap", 0.0) or 0.0)
                vol  = float(getattr(row, "volume_24h", 0.0) or 0.0)
                cat = cat_for_id(c["id"])
                old_lu, old_m, old_v = catmap[cat]
                new_lu = lu if (old_lu is None or (lu and lu > old_lu)) else old_lu
                catmap[cat] = (new_lu, old_m + mcap, old_v + vol)
                a_lu, a_m, a_v = catmap["ALL"]
                a_new_lu = lu if (a_lu is None or (lu and lu > a_lu)) else a_lu
                catmap["ALL"] = (a_new_lu, a_m + mcap, a_v + vol)
            hour_map[h] = catmap
        write_hourly_aggregates_from_map(hour_map)

    def recompute_daily_incremental(days_set):
        if not days_set: return
        print(f"[{_now_str()}] [mcap_daily] incremental dates={len(days_set)} (summing across {len(coins)} coins)")
        day_map = {}
        for d in sorted(days_set):
            catmap = defaultdict(lambda: (None, 0.0, 0.0))
            for c in coins:
                row = session.execute(SEL_DAILY_READ_FOR_AGG, [c["id"], d], timeout=REQUEST_TIMEOUT).one()
                if not row: continue
                lu = to_utc(getattr(row, "last_updated", None))
                mcap = float(getattr(row, "market_cap", 0.0) or 0.0)
                vol  = float(getattr(row, "volume_24h", 0.0) or 0.0)
                cat = cat_for_id(c["id"])
                old_lu, old_m, old_v = catmap[cat]
                new_lu = lu if (old_lu is None or (lu and lu > old_lu)) else old_lu
                catmap[cat] = (new_lu, old_m + mcap, old_v + vol)
                a_lu, a_m, a_v = catmap["ALL"]
                a_new_lu = lu if (a_lu is None or (lu and lu > a_lu)) else a_lu
                catmap["ALL"] = (a_new_lu, a_m + mcap, a_v + vol)
            day_map[d] = catmap
        write_daily_aggregates_from_map(day_map)

    def recompute_10m_incremental(ts_set):
        if not ts_set: return
        print(f"[{_now_str()}] [mcap_10m] incremental timestamps={len(ts_set)} (summing across {len(coins)} coins)")
        min10_map = {}
        for t10 in sorted(ts_set):
            catmap = defaultdict(lambda: (None, 0.0, 0.0))
            for c in coins:
                row = session.execute(SEL_10M_ONE, [c["id"], t10], timeout=REQUEST_TIMEOUT).one()
                if not row: continue
                lu = to_utc(getattr(row, "last_updated", None))
                mcap = float(getattr(row, "market_cap", 0.0) or 0.0)
                vol  = float(getattr(row, "volume_24h", 0.0) or 0.0)
                cat = cat_for_id(c["id"])
                old_lu, old_m, old_v = catmap[cat]
                new_lu = lu if (old_lu is None or (lu and lu > old_lu)) else old_lu
                catmap[cat] = (new_lu, old_m + mcap, old_v + vol)
                a_lu, a_m, a_v = catmap["ALL"]
                a_new_lu = lu if (a_lu is None or (lu and lu > a_lu)) else a_lu
                catmap["ALL"] = (a_new_lu, a_m + mcap, a_v + vol)
            min10_map[t10] = catmap
        write_10m_aggregates_from_map(min10_map)

    if FULL_MODE:
        print(f"[{_now_str()}] [FULL] recomputing aggregates over full windows 10m={DAYS_10M}d hourly={DAYS_HOURLY}d daily={DAYS_DAILY}d")
        if TRUNCATE_AGGREGATES_IN_FULL:
            if RECOMPUTE_MCAP_10M:    session.execute(TRUNC_10M)
            if RECOMPUTE_MCAP_HOURLY: session.execute(TRUNC_H)
            if RECOMPUTE_MCAP_DAILY:  session.execute(TRUNC_D)
            print(f"[{_now_str()}] [FULL] truncated aggregate tables (as enabled)")

        if RECOMPUTE_MCAP_10M:
            win_start = dt.datetime.combine(last_inclusive - dt.timedelta(days=DAYS_10M-1), dt.time.min, tzinfo=timezone.utc)
            win_end   = dt.datetime.combine(last_inclusive + dt.timedelta(days=1),        dt.time.min, tzinfo=timezone.utc)
            print(f"[{_now_str()}] [FULL][10m] window {win_start} → {win_end}")
            acc = {}
            for idx,c in enumerate(coins,1):
                if (idx==1) or (idx%25==0) or (idx==len(coins)):
                    print(f"[{_now_str()}] [FULL][10m] id {idx}/{len(coins)}: {c['id']}")
                rows = session.execute(SEL_10M_RANGE_FULL, [c["id"], win_start, win_end], timeout=REQUEST_TIMEOUT)
                for r in rows:
                    ts = to_utc(getattr(r, "ts", None))
                    if ts is None: continue
                    mcap = float(getattr(r, "market_cap", 0.0) or 0.0)
                    vol  = float(getattr(r, "volume_24h", 0.0) or 0.0)
                    lu   = to_utc(getattr(r, "last_updated", None)) or (ts + dt.timedelta(hours=1) - dt.timedelta(seconds=1))
                    cat  = cat_for_id(c["id"])
                    catmap = acc.setdefault(ts, {})
                    lu0, m0, v0 = catmap.get(cat, (None, 0.0, 0.0))
                    lu_new = lu if (lu0 is None or (lu and lu > lu0)) else lu0
                    catmap[cat] = (lu_new, m0 + mcap, v0 + vol)
                    alu0, am0, av0 = catmap.get("ALL", (None, 0.0, 0.0))
                    alu_new = lu if (alu0 is None or (lu and lu > alu0)) else alu0
                    catmap["ALL"] = (alu_new, am0 + mcap, av0 + vol)
            write_10m_aggregates_from_map(acc)

        if RECOMPUTE_MCAP_HOURLY:
            win_start = dt.datetime.combine(last_inclusive - dt.timedelta(days=DAYS_HOURLY-1), dt.time.min, tzinfo=timezone.utc)
            win_end   = dt.datetime.combine(last_inclusive + dt.timedelta(days=1),          dt.time.min, tzinfo=timezone.utc)
            print(f"[{_now_str()}] [FULL][hourly] window {win_start} → {win_end}")
            acc = {}
            for idx,c in enumerate(coins,1):
                if (idx==1) or (idx%25==0) or (idx==len(coins)):
                    print(f"[{_now_str()}] [FULL][hourly] id {idx}/{len(coins)}: {c['id']}")
                rows = session.execute(SEL_HOURLY_META_RANGE, [c["id"], win_start, win_end], timeout=REQUEST_TIMEOUT)
                for r in rows:
                    ts_raw = getattr(r, "ts", None)
                    ts = floor_to_hour_utc(ts_raw) if isinstance(ts_raw, dt.datetime) else None
                    if ts is None: continue
                    mcap = float(getattr(r, "market_cap", 0.0) or 0.0)
                    vol  = float(getattr(r, "volume_24h", 0.0) or 0.0)
                    lu   = to_utc(getattr(r, "last_updated", None)) or (ts + dt.timedelta(hours=1) - dt.timedelta(seconds=1))
                    cat  = cat_for_id(c["id"])
                    catmap = acc.setdefault(ts, {})
                    lu0, m0, v0 = catmap.get(cat, (None, 0.0, 0.0))
                    lu_new = lu if (lu0 is None or (lu and lu > lu0)) else lu0
                    catmap[cat] = (lu_new, m0 + mcap, v0 + vol)
                    alu0, am0, av0 = catmap.get("ALL", (None, 0.0, 0.0))
                    alu_new = lu if (alu0 is None or (lu and lu > alu0)) else lu0
                    catmap["ALL"] = (alu_new, am0 + mcap, av0 + vol)
            write_hourly_aggregates_from_map(acc)

        if RECOMPUTE_MCAP_DAILY:
            if FULL_DAILY_ALL:
                if TRUNCATE_AGGREGATES_IN_FULL:
                    session.execute(TRUNC_D)
                    print(f"[{_now_str()}] [FULL][daily_all] truncated {MCAP_DAILY_TABLE}")
                recompute_daily_full_all(coins, cat_for_id)
            else:
                win_start_d = last_inclusive - dt.timedelta(days=DAYS_DAILY-1)
                win_end_d   = last_inclusive
                print(f"[{_now_str()}] [FULL][daily] window {win_start_d} → {win_end_d}")
                acc = {}
                cur_d = win_start_d; all_days = []
                while cur_d <= win_end_d:
                    all_days.append(cur_d); cur_d += dt.timedelta(days=1)
                for idx,c in enumerate(coins,1):
                    if (idx==1) or (idx%25==0) or (idx==len(coins)):
                        print(f"[{_now_str()}] [FULL][daily] id {idx}/{len(coins)}: {c['id']}")
                    for d in all_days:
                        row = session.execute(SEL_DAILY_READ_FOR_AGG, [c["id"], d], timeout=REQUEST_TIMEOUT).one()
                        if not row: continue
                        mcap = float(getattr(row,"market_cap",0.0) or 0.0)
                        vol  = float(getattr(row,"volume_24h",0.0) or 0.0)
                        lu   = to_utc(getattr(row,"last_updated",None)) or dt.datetime(d.year,d.month,d.day,23,59,59,tzinfo=timezone.utc)
                        cat  = cat_for_id(c["id"])
                        catmap = acc.setdefault(d, {})
                        lu0, m0, v0 = catmap.get(cat, (None, 0.0, 0.0))
                        lu_new = lu if (lu0 is None or (lu and lu > lu0)) else lu0
                        catmap[cat] = (lu_new, m0 + mcap, v0 + vol)
                        alu0, am0, av0 = catmap.get("ALL", (None, 0.0, 0.0))
                        alu_new = lu if (alu0 is None or (lu and lu > alu0)) else lu0
                        catmap["ALL"] = (alu_new, am0 + mcap, av0 + vol)
                write_daily_aggregates_from_map(acc)

    else:
        # INCREMENTAL: only timestamps/days we touched
        if RECOMPUTE_MCAP_HOURLY and affected_hours:
            recompute_hourly_incremental(affected_hours)
        if RECOMPUTE_MCAP_DAILY and affected_days:
            recompute_daily_incremental(affected_days)
        if RECOMPUTE_MCAP_10M and affected_10m_ts:
            recompute_10m_incremental(affected_10m_ts)

    print(f"[{_now_str()}] DONE in {tdur(t_all)} | Local fixes → daily_from_10m:{fixedD_local} "
          f"{'(10m_seeded:'+str(fixed10_seeded)+')' if SEED_10M_FROM_DAILY else ''} "
          f"| API fixes → daily:{fixedD_api} | hourly:{fixedH_total} | Universe size: {len(coins)}")

    # ---------- Final Summary ----------
    coins_fixed_count = sum(1 for _id, n in coin_fix_counts.items() if n > 0)
    points_total = metrics["points_daily"] + metrics["points_hourly"] + metrics["points_10m"]
    print(f"[{_now_str()}] FINAL SUMMARY → "
          f"CG calls: total={metrics['cg_calls_total']} ok={metrics['cg_calls_ok']} "
          f"429={metrics['cg_calls_429']} deferred={metrics['cg_calls_deferred']} "
          f"5xx={metrics['cg_calls_5xx']} other_http={metrics['cg_calls_other_http']} | "
          f"coins_fixed={coins_fixed_count} | "
          f"data_points_fixed: daily={metrics['points_daily']} hourly={metrics['points_hourly']} 10m={metrics['points_10m']} "
          f"→ agg_rows: 10m={metrics['agg_rows_10m']} hourly={metrics['agg_rows_hourly']} daily={metrics['agg_rows_daily']} "
          f"total_points={points_total}")

if __name__ == "__main__":
    try:
        main()
    finally:
        print(f"[{_now_str()}] Shutting down…")
        try:
            if 'cluster' in globals() and cluster:
                cluster.shutdown()
        except Exception:
            print(f"[{_now_str()}] Shutdown error:")
            traceback.print_exc()
        print(f"[{_now_str()}] Done.")
