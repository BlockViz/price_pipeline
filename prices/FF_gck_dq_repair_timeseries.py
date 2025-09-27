#!/usr/bin/env python3
# FF_gck_dq_repair_timeseries.py
# Plan-first API ranges, hourly repair + tz fix + rate limit + neg-cache
# Enhanced: include coins that fall in/out of Top-N and use astra_connect connector.
# Added: final summary of API call counts and fix counts (coins + data points).

import os, time, random, requests, datetime as dt
from datetime import timezone
from collections import defaultdict, deque
from time import perf_counter, sleep

# ───────────────────── Connector (no path/env hacks) ─────────────────────
from astra_connect.connect import get_session
session, cluster = get_session(return_cluster=True)

# ---------- Config ----------
KEYSPACE     = os.getenv("ASTRA_KEYSPACE", "default_keyspace")

TABLE_LIVE      = os.getenv("TABLE_LIVE", "gecko_prices_live")
TEN_MIN_TABLE   = os.getenv("TEN_MIN_TABLE", "gecko_prices_10m_7d")
DAILY_TABLE     = os.getenv("DAILY_TABLE", "gecko_candles_daily_contin")
TABLE_ROLLING   = os.getenv("TABLE_ROLLING", "gecko_prices_live_rolling")
HOURLY_TABLE    = os.getenv("HOURLY_TABLE", "gecko_candles_hourly_30d")

# Repair windows
DAYS_10M   = int(os.getenv("DQ_WINDOW_10M_DAYS", "7"))
DAYS_DAILY = int(os.getenv("DQ_WINDOW_DAILY_DAYS", "365"))
DAYS_HOURLY = int(os.getenv("DQ_WINDOW_HOURLY_DAYS", "30"))

# What to run
FIX_DAILY_FROM_10M      = os.getenv("FIX_DAILY_FROM_10M", "1") == "1"
SEED_10M_FROM_DAILY     = os.getenv("SEED_10M_FROM_DAILY", "0") == "1"
FILL_HOURLY             = os.getenv("FILL_HOURLY", "1") == "1"
FILL_HOURLY_FROM_API    = os.getenv("FILL_HOURLY_FROM_API", "1") == "1"
INTERPOLATE_IF_API_MISS = os.getenv("INTERPOLATE_IF_API_MISS", "1") == "1"
BACKFILL_DAILY_FROM_API = os.getenv("BACKFILL_DAILY_FROM_API", "1") == "1"

# Top-N and universe expansion
TOP_N_DQ              = int(os.getenv("TOP_N_DQ", "210"))
DQ_MAX_COINS          = int(os.getenv("DQ_MAX_COINS", "100000"))
INCLUDE_ALL_DAILY_IDS = os.getenv("INCLUDE_ALL_DAILY_IDS", "1") == "1"  # include all ids seen in DAILY

# Performance / logging
REQUEST_TIMEOUT = int(os.getenv("DQ_REQUEST_TIMEOUT_SEC", "30"))
CONNECT_TIMEOUT = int(os.getenv("DQ_CONNECT_TIMEOUT_SEC", "15"))
FETCH_SIZE      = int(os.getenv("DQ_FETCH_SIZE", "500"))
RETRIES         = int(os.getenv("DQ_RETRIES", "3"))
BACKOFF_S       = int(os.getenv("DQ_BACKOFF_SEC", "4"))
PAUSE_S         = float(os.getenv("DQ_PAUSE_PER_COIN", "0.05"))

LOG_EVERY   = int(os.getenv("DQ_LOG_EVERY", "10"))
VERBOSE     = os.getenv("DQ_VERBOSE", "0") == "1"
TIME_API    = os.getenv("DQ_TIME_API", "1") == "1"
DRY_RUN     = os.getenv("DQ_DRY_RUN", "0") == "1"

# Soft run budget (stop before the platform hard-kills at 25m)
SOFT_BUDGET_SEC = int(os.getenv("DQ_SOFT_BUDGET_SEC", str(24*60)))  # default 24 minutes

# CoinGecko API tier
API_TIER = (os.getenv("COINGECKO_API_TIER") or "demo").strip().lower()
API_KEY  = (os.getenv("COINGECKO_API_KEY") or "pro").strip()
if API_KEY.lower().startswith("api key:"):
    API_KEY = API_KEY.split(":", 1)[1].strip()
BASE = os.getenv(
    "COINGECKO_BASE_URL",
    "https://api.coingecko.com/api/v3" if API_TIER == "demo" else "https://pro-api.coingecko.com/api/v3"
)
HDR = "x-cg-demo-api-key" if API_TIER == "demo" else "x-cg-pro-api-key"
QS  = "x_cg_demo_api_key" if API_TIER == "demo" else "x_cg_pro_api_key"

# Request budget for daily API backfill
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

# ---------- Metrics ----------
metrics = {
    "cg_calls_total": 0,         # total HTTP attempts sent to CoinGecko
    "cg_calls_ok": 0,            # 2xx
    "cg_calls_429": 0,           # rate limited responses
    "cg_calls_deferred": 0,      # times we deferred due to long Retry-After
    "cg_calls_5xx": 0,           # transient server errors
    "cg_calls_other_http": 0,    # other HTTP errors (e.g., 4xx other than 429)
    "points_daily": 0,           # daily rows written (from 10m + API)
    "points_hourly": 0,          # hourly rows written
    "points_10m": 0,             # 10m rows seeded from daily
}
coin_fix_counts = defaultdict(int)  # id -> total rows written across all tables

# Negative cache for coins with no data
NEG_CACHE_PATH = os.getenv("COINGECKO_NEG_CACHE_PATH", "tmp/coingecko_no_market.ids")
os.makedirs(os.path.dirname(NEG_CACHE_PATH), exist_ok=True)

# ---------- Small utils ----------
def ts(): return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
def tdur(t0): return f"{(perf_counter()-t0):.2f}s"
def utcnow(): return dt.datetime.now(timezone.utc)

def to_utc(x: dt.datetime | None) -> dt.datetime | None:
    if x is None: return None
    if x.tzinfo is None: return x.replace(tzinfo=timezone.utc)
    return x.astimezone(timezone.utc)

def floor_to_hour_utc(x: dt.datetime) -> dt.datetime:
    x = to_utc(x)
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

def equalish(a,b,eps=1e-12):
    if a is None and b is None: return True
    if a is None or b is None:  return False
    try: return abs(float(a)-float(b)) <= eps
    except Exception: return False

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

# ---------- HTTP with rate limiting ----------
class RateLimitDefer(Exception):
    """Raised to signal we should defer this request instead of sleeping for a long Retry-After."""
    def __init__(self, status_code: int, retry_after: float | None, url: str):
        super().__init__(f"Rate limited {status_code}, retry_after={retry_after}, url={url}")
        self.status_code = status_code
        self.retry_after = retry_after
        self.url = url

def _throttle():
    now = time.time()
    # simple per-request spacing
    if _req_times and (now - _req_times[-1]) < CG_REQ_INTERVAL_S:
        time.sleep(CG_REQ_INTERVAL_S - (now - _req_times[-1])); now = time.time()
    # sliding window RPM
    cutoff = now - 60.0
    while _req_times and _req_times[0] < cutoff: _req_times.popleft()
    if len(_req_times) >= CG_MAX_RPM:
        sleep_for = 60.0 - (now - _req_times[0]) + 0.01
        time.sleep(max(0.0, sleep_for))

def http_get(path, params=None):
    url = f"{BASE}{path}"; params = dict(params or {})
    headers = {HDR: API_KEY} if API_KEY else {}
    if API_KEY: params[QS] = API_KEY
    last = None
    for i in range(RETRIES):
        _throttle()
        _req_times.append(time.time())                 # count this ATTEMPT
        metrics["cg_calls_total"] += 1
        t0 = perf_counter()
        try:
            r = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            # 429/402 (rate/plan)
            if r.status_code in (429, 402):
                metrics["cg_calls_429"] += 1
                ra = r.headers.get("Retry-After")
                try:
                    ra_val = float(ra) if ra and ra.replace('.','',1).isdigit() else None
                except Exception:
                    ra_val = None
                if (i + 1) < RETRIES and (ra_val is None or ra_val <= 5.0):
                    wait_s = (ra_val if ra_val is not None else BACKOFF_S*(i+1))
                    print(f"[{ts()}] API {path} 429 — short backoff {wait_s:.1f}s (retry {i+1}/{RETRIES})")
                    time.sleep(wait_s + random.uniform(0,0.5)); last = requests.HTTPError(f"{r.status_code}", response=r); continue
                print(f"[{ts()}] API {path} 429 — deferring (Retry-After={ra_val})")
                metrics["cg_calls_deferred"] += 1
                raise RateLimitDefer(r.status_code, ra_val, url)
            # 5xx transient
            if r.status_code in (500,502,503,504):
                metrics["cg_calls_5xx"] += 1
                wait_s = min(5.0, BACKOFF_S*(i+1))
                print(f"[{ts()}] API {path} error {r.status_code} — backoff {wait_s:.1f}s (retry {i+1}/{RETRIES})")
                time.sleep(wait_s + random.uniform(0,0.5)); last = requests.HTTPError(f"{r.status_code}", response=r); continue

            r.raise_for_status()
            if TIME_API: print(f"[{ts()}] API OK {path} took {tdur(t0)}")
            metrics["cg_calls_ok"] += 1
            return r.json()
        except RateLimitDefer:
            raise
        except (requests.ConnectionError, requests.Timeout) as e:
            last = e
            wait_s = min(5.0, BACKOFF_S*(i+1))
            print(f"[{ts()}] API {path} error: {e} — backoff {wait_s:.1f}s (retry {i+1}/{RETRIES})")
            time.sleep(wait_s + random.uniform(0,0.5))
        except requests.HTTPError as e:
            # Other HTTP errors (e.g., 404)
            try:
                sc = e.response.status_code if e.response is not None else None
            except Exception:
                sc = None
            if sc and sc != 429 and sc < 500:
                metrics["cg_calls_other_http"] += 1
            last = e
    raise RuntimeError(f"CoinGecko failed: {url} :: {last}")

# ---------- Bucket helpers ----------
def bucket_daily_ohlc(prices_ms_values, start_d: dt.date, end_d: dt.date):
    per_day = defaultdict(list)
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
    per_day = defaultdict(list)
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

SEL_LIVE = SimpleStatement(
    f"SELECT id, symbol, name, market_cap_rank FROM {TABLE_LIVE}",
    fetch_size=FETCH_SIZE
)

SEL_DAILY_DISTINCT_IDS = SimpleStatement(  # cluster-wide scan; acceptable for maintenance job
    f"SELECT DISTINCT id FROM {DAILY_TABLE}",
    fetch_size=FETCH_SIZE
)

SEL_10M_RANGE_FULL = session.prepare(f"""
  SELECT ts, price_usd, market_cap, volume_24h,
         market_cap_rank, circulating_supply, total_supply, last_updated
  FROM {TEN_MIN_TABLE}
  WHERE id = ? AND ts >= ? AND ts < ? ORDER BY ts ASC
""")
SEL_10M_RANGE_DAYS = session.prepare(f"""
  SELECT ts FROM {TEN_MIN_TABLE}
  WHERE id = ? AND ts >= ? AND ts < ?
""")
SEL_DAILY_ONE = session.prepare(f"""
  SELECT date, symbol, name,
         price_usd, market_cap, volume_24h, last_updated,
         open, high, low, close, candle_source,
         market_cap_rank, circulating_supply, total_supply
  FROM {DAILY_TABLE}
  WHERE id = ? AND date = ? LIMIT 1
""")
SEL_DAILY_RANGE = session.prepare(f"""
  SELECT date FROM {DAILY_TABLE}
  WHERE id = ? AND date >= ? AND date <= ?
""")
SEL_DAILY_LAST_META = session.prepare(f"""
  SELECT date, symbol, name FROM {DAILY_TABLE}
  WHERE id = ? ORDER BY date DESC LIMIT 1
""")
SEL_ROLLING_RANGE = session.prepare(f"""
  SELECT last_updated, market_cap_rank, circulating_supply, total_supply
  FROM {TABLE_ROLLING}
  WHERE id = ? AND last_updated >= ? AND last_updated < ?
""")
SEL_HOURLY_RANGE = session.prepare(f"""
  SELECT ts FROM {HOURLY_TABLE}
  WHERE id = ? AND ts >= ? AND ts < ?
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

# ---------- Universe selection ----------
def top_assets(limit: int):
    """
    Build the asset set as a union of:
      - Top-N ranked from live
      - Any ids present in coin_daily_availability overlapping the DQ window
      - Optional INCLUDE_IDS env (comma-separated ids)
    This avoids DISTINCT-on-clustering errors and still keeps coins that drop out of Top-N.
    """
    # A) ranked coins from live
    rows = list(session.execute(SEL_LIVE, timeout=REQUEST_TIMEOUT))
    ranked = [r for r in rows if isinstance(getattr(r, "market_cap_rank", None), int) and r.market_cap_rank > 0]
    ranked.sort(key=lambda r: r.market_cap_rank)
    ranked = ranked[:limit]
    by_id = {r.id: r for r in ranked}

    # B) ids from availability summary (scan; id is the partition key there)
    avail_ids = set()
    try:
        ss = SimpleStatement("SELECT id, first_day, last_day FROM coin_daily_availability", fetch_size=FETCH_SIZE)
        for row in session.execute(ss, timeout=REQUEST_TIMEOUT):
            fid_raw = getattr(row, "first_day", None)
            lid_raw = getattr(row, "last_day", None)
            if not fid_raw or not lid_raw:
                continue
            # Normalize types to Python date to avoid 'Date' vs 'datetime.date' comparisons
            try:
                fid = _to_pydate(fid_raw)
                lid = _to_pydate(lid_raw)
            except Exception:
                continue
            # include if its span overlaps our daily DQ window
            if (lid >= start_daily_date) and (fid <= last_inclusive):
                avail_ids.add(row.id)
    except Exception as e:
        print(f"[{ts()}] [WARN] availability scan failed (optional): {e}")

    # C) manual includes
    extra_ids = set(x.strip().lower() for x in (os.getenv("INCLUDE_IDS") or "").split(",") if x.strip())

    want_ids = set(by_id.keys()) | set(avail_ids) | extra_ids

    # hydrate minimal records for ids not present in live
    def _mk_stub(cid: str):
        return type("Coin", (), {
            "id": cid,
            "symbol": (cid or "").upper(),
            "name": cid,
            "market_cap_rank": None
        })()

    for cid in want_ids:
        if cid not in by_id:
            by_id[cid] = _mk_stub(cid)

    out = list(by_id.values())
    # stable order: live-ranked first, then stubs
    out.sort(key=lambda r: (0 if (getattr(r, "market_cap_rank", None) is not None) else 1,
                            getattr(r, "market_cap_rank", 10**9),
                            r.id))
    print(f"[{ts()}] Loaded {len(out)} assets for DQ (live_top={len(ranked)}, from_availability={len(avail_ids)}, manual={len(extra_ids)})")
    return out

def all_daily_ids():
    t0 = perf_counter()
    ids = [r.id for r in session.execute(SEL_DAILY_DISTINCT_IDS, timeout=REQUEST_TIMEOUT)]
    print(f"[{ts()}] Loaded {len(ids)} distinct ids from {DAILY_TABLE} in {tdur(t0)}")
    return set(ids)

def lookup_meta_for_id(coin_id: str, live_index: dict):
    """Return (symbol, name, rank|None) using live first, then latest daily row."""
    live = live_index.get(coin_id)
    if live:
        return live.symbol, live.name, live.market_cap_rank
    row = session.execute(SEL_DAILY_LAST_META, [coin_id], timeout=REQUEST_TIMEOUT).one()
    if row and getattr(row, "symbol", None):
        return row.symbol, row.name, None
    return None, None, None

# ---------- Presence helpers ----------
def existing_days_10m(coin_id: str, start_dt: dt.datetime, end_dt: dt.datetime):
    have = set()
    for row in session.execute(SEL_10M_RANGE_DAYS, [coin_id, to_utc(start_dt), to_utc(end_dt)], timeout=REQUEST_TIMEOUT):
        have.add(_to_pydate(row.ts.date()))
    return have

def existing_days_daily(coin_id: str, start_date: dt.date, end_date: dt.date):
    have = set()
    for row in session.execute(SEL_DAILY_RANGE, [coin_id, start_date, end_date], timeout=REQUEST_TIMEOUT):
        have.add(_to_pydate(row.date))
    return have

def existing_hours_hourly(coin_id: str, start_dt: dt.datetime, end_dt: dt.datetime):
    have = set()
    for row in session.execute(SEL_HOURLY_RANGE, [coin_id, to_utc(start_dt), to_utc(end_dt)], timeout=REQUEST_TIMEOUT):
        if getattr(row, "ts", None):
            have.add(floor_to_hour_utc(row.ts))
    return have

def _daily_row_equal(existing, o,h,l,c,mcap,vol,source):
    if not existing: return False
    return (
        equalish(getattr(existing,"open",None),  o) and
        equalish(getattr(existing,"high",None),  h) and
        equalish(getattr(existing,"low",None),   l) and
        equalish(getattr(existing,"close",None), c) and
        equalish(getattr(existing,"market_cap",None), mcap) and
        equalish(getattr(existing,"volume_24h",None), vol) and
        (getattr(existing,"candle_source",None) == source)
    )

# ---------- Repairs (daily from 10m; seed 10m; API backfill; hourly) ----------
def repair_daily_from_10m(coin, missing_days: set[dt.date]) -> int:
    if not (FIX_DAILY_FROM_10M and missing_days): return 0
    print(f"[{ts()}]    [repair_daily_from_10m] {coin['symbol']}: {len(missing_days)} day(s)")
    t0 = perf_counter(); cnt = 0
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for day in sorted(missing_days):
        day_start, day_end_excl = day_bounds_utc(day)
        pts = list(session.execute(SEL_10M_RANGE_FULL, [coin["id"], day_start, day_end_excl], timeout=REQUEST_TIMEOUT))
        if VERBOSE: print(f"[{ts()}]      · {coin['symbol']} {day} → 10m rows={len(pts)}")
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
        if (cnt % 40 == 0) and not DRY_RUN:
            session.execute(batch); batch.clear()
    if (cnt % 40 != 0) and not DRY_RUN and len(batch)>0:
        session.execute(batch)
    print(f"[{ts()}]    [repair_daily_from_10m] {coin['symbol']} wrote={cnt} ({tdur(t0)})")
    return cnt

def seed_10m_from_daily(coin, missing_days: set[dt.date]) -> int:
    if not (SEED_10M_FROM_DAILY and missing_days): return 0
    print(f"[{ts()}]    [seed_10m] {coin['symbol']}: {len(missing_days)} day(s)")
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
        if (cnt % 40 == 0) and not DRY_RUN:
            session.execute(batch); batch.clear()
    if (cnt % 40 != 0) and not DRY_RUN and len(batch)>0:
        session.execute(batch)
    print(f"[{ts()}]    [seed_10m] {coin['symbol']} wrote={cnt} ({tdur(t0)})")
    return cnt

def backfill_daily_from_api_ranges(coin, need_daily: set[dt.date], dt_ranges: list[tuple[dt.datetime, dt.datetime]]) -> int:
    if not (BACKFILL_DAILY_FROM_API and need_daily and dt_ranges): return 0
    all_prices, all_mcaps, all_vols = [], [], []
    for (start_dt, end_dt) in dt_ranges:
        try:
            data = http_get(
                f"/coins/{coin['id']}/market_chart/range",
                params={"vs_currency":"usd","from":int(to_utc(start_dt).timestamp()),"to":int(to_utc(end_dt).timestamp())}
            )
        except RateLimitDefer as e:
            print(f"[{ts()}]      · window {start_dt.date()}→{end_dt.date()} rate-limited; deferring")
            continue
        except Exception as e:
            print(f"[{ts()}]      · window {start_dt.date()}→{end_dt.date()} failed: {e}")
            continue
        prices = data.get("prices", []) or []
        mcaps  = data.get("market_caps", []) or []
        vols   = data.get("total_volumes", []) or []
        print(f"[{ts()}]      · window {start_dt.date()}→{end_dt.date()} sizes: prices={len(prices)} mcaps={len(mcaps)} vols={len(vols)}")
        all_prices.extend(prices); all_mcaps.extend(mcaps); all_vols.extend(vols)
        time.sleep(PAUSE_S + random.uniform(0.0,0.25))

    if not all_prices:
        print(f"[{ts()}]    [api] no payload for {coin['symbol']} → skip (wrote=0)")
        add_to_neg_cache(coin["id"]); return 0

    days = sorted(need_daily)
    start_day, end_day = days[0], days[-1]
    ohlc   = bucket_daily_ohlc(all_prices, start_day, end_day)
    m_last = bucket_daily_last(all_mcaps,  start_day, end_day)
    v_last = bucket_daily_last(all_vols,   start_day, end_day)
    print(f"[{ts()}]    [api] bucketed days price={len(ohlc)}")

    if INTERPOLATE_MCAP_VOL_DAILY:
        m_series = interpolate_daily_series(days, {d: (m_last.get(d,{}).get("val")) for d in days}, max_gap_days=INTERP_MAX_DAYS)
        v_series = interpolate_daily_series(days, {d: (v_last.get(d,{}).get("val")) for d in days}, max_gap_days=INTERP_MAX_DAYS)
    else:
        m_series = {d: (m_last.get(d,{}).get("val")) for d in days}
        v_series = {d: (v_last.get(d,{}).get("val")) for d in days}

    # rolling state
    start_dt2, _ = day_bounds_utc(start_day)
    end_dt2      = dt.datetime(end_day.year,end_day.month,end_day.day,23,59,59,tzinfo=timezone.utc)
    raw = list(session.execute(SEL_ROLLING_RANGE, [coin["id"], start_dt2, end_dt2+dt.timedelta(seconds=1)], timeout=REQUEST_TIMEOUT))
    states = []
    for r in raw:
        states.append((to_utc(r.last_updated), r.market_cap_rank,
                       float(r.circulating_supply) if r.circulating_supply is not None else None,
                       float(r.total_supply) if r.total_supply is not None else None))
    states.sort(key=lambda x: x[0])
    st_i = -1
    cur_rank = coin.get("market_cap_rank")
    cur_circ = cur_tot = None
    def advance_state_until(day_end_dt):
        nonlocal st_i, cur_rank, cur_circ, cur_tot
        while st_i + 1 < len(states) and states[st_i+1][0] <= day_end_dt:
            st_i += 1; _, rnk, circ, tot = states[st_i]
            if rnk  is not None: cur_rank = rnk
            if circ is not None: cur_circ = circ
            if tot  is not None: cur_tot  = tot

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
        advance_state_until(day_end)

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
        if (cnt_daily % 40 == 0): session.execute(batch); batch.clear()
    if len(batch): session.execute(batch)
    print(f"[{ts()}]    [api] {coin['symbol']} wrote={cnt_daily}")
    return cnt_daily

def fetch_hourly_from_api(coin_id: str, start_dt: dt.datetime, end_dt: dt.datetime):
    data = http_get(
        f"/coins/{coin_id}/market_chart/range",
        params={"vs_currency":"usd","from":int(to_utc(start_dt).timestamp()),"to":int(to_utc(end_dt).timestamp())}
    )
    def to_hour_map(arr):
        m = {}
        for ms, v in (arr or []):
            ts_ = dt.datetime.fromtimestamp(ms/1000.0, tz=timezone.utc)
            hr  = floor_to_hour_utc(ts_); m[hr] = (float(v) if v is not None else None, ts_)
        return m
    return to_hour_map(data.get("prices")), to_hour_map(data.get("market_caps")), to_hour_map(data.get("total_volumes"))

def repair_hourly_from_api_or_interp(coin, missing_hours: set[dt.datetime]) -> int:
    if not (FILL_HOURLY and missing_hours): return 0
    hours_sorted = sorted([floor_to_hour_utc(h) for h in missing_hours])
    start_dt = floor_to_hour_utc(hours_sorted[0] - dt.timedelta(hours=6))
    end_dt   = floor_to_hour_utc(hours_sorted[-1] + dt.timedelta(hours=7))
    p_map = mc_map = v_map = {}
    if FILL_HOURLY_FROM_API:
        try:
            p_map, mc_map, v_map = fetch_hourly_from_api(coin["id"], start_dt, end_dt)
        except RateLimitDefer:
            print(f"[{ts()}]  · hourly API fetch rate-limited for {coin['symbol']} — deferring")
            if not INTERPOLATE_IF_API_MISS: return 0
        except Exception as e:
            print(f"[{ts()}]  · hourly API fetch failed for {coin['symbol']}: {e}")
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

    raw = list(session.execute(SEL_ROLLING_RANGE, [coin["id"], start_dt, end_dt+dt.timedelta(seconds=1)], timeout=REQUEST_TIMEOUT))
    states = []
    for r in raw:
        states.append((to_utc(r.last_updated), r.market_cap_rank,
                       float(r.circulating_supply) if r.circulating_supply is not None else None,
                       float(r.total_supply) if r.total_supply is not None else None))
    states.sort(key=lambda x: x[0])
    st_i = -1; cur_rank = coin.get("market_cap_rank"); cur_circ = cur_tot = None
    def advance_state_until(hr_end):
        nonlocal st_i, cur_rank, cur_circ, cur_tot
        while st_i + 1 < len(states) and states[st_i+1][0] <= hr_end:
            st_i += 1; _, rnk, circ, tot = states[st_i]
            if rnk  is not None: cur_rank = rnk
            if circ is not None: cur_circ = circ
            if tot  is not None: cur_tot  = tot

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
        advance_state_until(hr_end)
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
        if (wrote % 64 == 0) and not DRY_RUN:
            session.execute(batch); batch.clear()
    if not DRY_RUN and len(batch): session.execute(batch)
    print(f"[{ts()}]    [hourly] {coin['symbol']} wrote={wrote}")
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
        print(f"[{ts()}] [WARN] could not update neg cache: {e}")

# Helpers for contiguous ranges
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

# ---------- Main ----------
def main():
    now = utcnow()
    end_excl = dt.datetime(now.year,now.month,now.day,tzinfo=timezone.utc) + dt.timedelta(days=1)
    global start_daily_date, last_inclusive  # used by top_assets()
    last_inclusive   = end_excl.date() - dt.timedelta(days=1)
    start_10m_date   = last_inclusive - dt.timedelta(days=DAYS_10M-1)
    start_daily_date = last_inclusive - dt.timedelta(days=DAYS_DAILY-1)

    want_10m_days   = set(date_seq(last_inclusive, DAYS_10M))
    want_daily_days = set(date_seq(last_inclusive, DAYS_DAILY))

    # Build coin universe
    live = top_assets(TOP_N_DQ)
    live_index = {r.id: r for r in live}
    universe_ids = set(r.id for r in live)

    if INCLUDE_ALL_DAILY_IDS:
        universe_ids |= all_daily_ids()

    # Cap if requested
    if DQ_MAX_COINS and len(universe_ids) > DQ_MAX_COINS:
        print(f"[{ts()}] Universe {len(universe_ids)} > cap {DQ_MAX_COINS}; trimming deterministically.")
        universe_ids = set(sorted(universe_ids)[:DQ_MAX_COINS])

    # Build coin meta dicts
    coins = []
    for cid in sorted(universe_ids):
        symbol, name, rank = lookup_meta_for_id(cid, live_index)
        coins.append({"id": cid, "symbol": symbol or cid, "name": name or cid, "market_cap_rank": rank})

    print(f"[{ts()}] DQ windows → 10m: {start_10m_date}→{last_inclusive} | daily: {start_daily_date}→{last_inclusive}")
    print(f"[{ts()}] Universe size: {len(coins)} (top {len(live)} + daily extras {len(universe_ids)-len(live)})")

    fixedD_local = fixed10_seeded = fixedD_api = 0
    fixedH_total = 0
    t_all = perf_counter()
    neg_cache = load_neg_cache()
    api_plans = []
    deadline = perf_counter() + SOFT_BUDGET_SEC

    for i, coin in enumerate(coins, 1):
        t_coin = perf_counter()
        try:
            if coin["id"] in neg_cache:
                print(f"[{ts()}] → Coin {i}/{len(coins)}: {coin['symbol']} in negative cache — skip")
                continue
            print(f"[{ts()}] → Coin {i}/{len(coins)}: {coin['symbol']} ({coin['id']}) rank={coin.get('market_cap_rank')}")

            have_10m = existing_days_10m(
                coin["id"],
                dt.datetime.combine(start_10m_date, dt.time.min, tzinfo=timezone.utc),
                dt.datetime.combine(last_inclusive + dt.timedelta(days=1), dt.time.min, tzinfo=timezone.utc)
            )
            have_daily = existing_days_daily(coin["id"], start_daily_date, last_inclusive)

            need_daily_all   = want_daily_days - have_daily
            need_daily_local = need_daily_all & have_10m
            need_daily_api   = need_daily_all - need_daily_local
            need_10m         = want_10m_days  - have_10m

            print(f"[{ts()}]    Missing → 10m:{len(need_10m)} "
                  f"daily_total:{len(need_daily_all)} (10m_eligible:{len(need_daily_local)}, api:{len(need_daily_api)})")

            if need_daily_local and FIX_DAILY_FROM_10M:
                fixed = repair_daily_from_10m(coin, need_daily_local)
                fixedD_local += fixed
                if fixed > 0:
                    metrics["points_daily"] += fixed
                    coin_fix_counts[coin["id"]] += fixed

            if need_10m and SEED_10M_FROM_DAILY:
                fixed = seed_10m_from_daily(coin, need_10m)
                fixed10_seeded += fixed
                if fixed > 0:
                    metrics["points_10m"] += fixed
                    coin_fix_counts[coin["id"]] += fixed

            # Hourly repair
            if FILL_HOURLY:
                start_hour_dt = dt.datetime.combine(last_inclusive - dt.timedelta(days=DAYS_HOURLY-1), dt.time.min, tzinfo=timezone.utc)
                end_hour_dt   = dt.datetime.combine(last_inclusive + dt.timedelta(days=1), dt.time.min, tzinfo=timezone.utc)
                want_hours = set(hour_seq(start_hour_dt, end_hour_dt))
                have_hours = existing_hours_hourly(coin["id"], start_hour_dt, end_hour_dt)
                need_hours = want_hours - have_hours
                print(f"[{ts()}]    Hourly window: missing_hours={len(need_hours)} over {DAYS_HOURLY}d")
                if need_hours:
                    try:
                        wroteH = repair_hourly_from_api_or_interp(coin, need_hours)
                        print(f"[{ts()}]    hourly_repair → wrote {wroteH} rows")
                        fixedH_total += wroteH
                        if wroteH > 0:
                            metrics["points_hourly"] += wroteH
                            coin_fix_counts[coin["id"]] += wroteH
                    except RateLimitDefer:
                        print(f"[{ts()}]    [WARN] hourly repair deferred due to rate limit for {coin['symbol']}")
                    except Exception as e:
                        print(f"[{ts()}]    [WARN] hourly repair failed for {coin['symbol']}: {e}")

            # Plan API ranges for residual daily gaps
            remaining_daily = (want_daily_days - existing_days_daily(coin["id"], start_daily_date, last_inclusive))
            if remaining_daily and BACKFILL_DAILY_FROM_API:
                runs = group_contiguous_dates(remaining_daily)
                padded = []
                for s,e in runs:
                    s_pad = max(start_daily_date, s - dt.timedelta(days=PAD_DAYS))
                    e_pad = min(last_inclusive,   e + dt.timedelta(days=PAD_DAYS))
                    padded.extend(clamp_date_range(s_pad, e_pad, MAX_RANGE_DAYS))
                dt_ranges = []
                for s,e in padded:
                    start_dt3 = dt.datetime(s.year,s.month,s.day,0,0,0,tzinfo=timezone.utc)
                    end_dt3   = dt.datetime(e.year,e.month,e.day,23,59,59,tzinfo=timezone.utc)
                    dt_ranges.append((start_dt3, end_dt3))
                if dt_ranges:
                    api_plans.append({"coin": coin, "ranges": dt_ranges, "need_days": remaining_daily})

            sleep(PAUSE_S)
            elapsed_coin = perf_counter() - t_coin
            if (i % LOG_EVERY == 0) or VERBOSE:
                print(f"[{ts()}] ← Done {coin['symbol']} in {elapsed_coin:.2f}s  (progress {i}/{len(coins)})")

            # Respect soft budget
            if perf_counter() >= deadline:
                print(f"[{ts()}] [WARN] Time budget exhausted at {i}/{len(coins)}; stopping early.")
                break

        except Exception as e:
            print(f"[{ts()}] [WARN] coin {coin.get('symbol')} ({coin.get('id')}) failed: {e}")
            continue

    # Execute API plan under request budget
    if BACKFILL_DAILY_FROM_API and api_plans:
        def _plan_reqs(p): return len(p["ranges"])
        api_plans.sort(key=_plan_reqs)  # cheapest first
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
            except RateLimitDefer:
                print(f"[{ts()}]  · API repair deferred for {coin['symbol']} due to rate limit")
            except Exception as e:
                print(f"[{ts()}]  · API repair failed for {coin['symbol']}: {e}")
            sleep(PAUSE_S)
        remaining = max(0, len(api_plans) - coins_done)
        print(f"[{ts()}] API pass done: coins={coins_done} daily_rows={fixedD_api} remaining_coins={remaining} (req_budget_left={reqs_left})")
    else:
        print(f"[{ts()}] No API pass needed or disabled.")

    # Existing overall line
    print(f"[{ts()}] DONE in {tdur(t_all)} | Local fixes → daily_from_10m:{fixedD_local} "
          f"{'(10m_seeded:'+str(fixed10_seeded)+')' if SEED_10M_FROM_DAILY else ''} "
          f"| API fixes → daily:{fixedD_api} | hourly:{fixedH_total} | Universe size: {len(coins)}")

    # ---------- Final Summary ----------
    coins_fixed_count = sum(1 for _id, n in coin_fix_counts.items() if n > 0)
    points_total = metrics["points_daily"] + metrics["points_hourly"] + metrics["points_10m"]
    print(f"[{ts()}] FINAL SUMMARY → "
          f"CG calls: total={metrics['cg_calls_total']} ok={metrics['cg_calls_ok']} "
          f"429={metrics['cg_calls_429']} deferred={metrics['cg_calls_deferred']} "
          f"5xx={metrics['cg_calls_5xx']} other_http={metrics['cg_calls_other_http']} | "
          f"coins_fixed={coins_fixed_count}/{len(coins)} | "
          f"data_points_fixed: daily={metrics['points_daily']} "
          f"hourly={metrics['points_hourly']} 10m={metrics['points_10m']} "
          f"total={points_total}")

if __name__ == "__main__":
    try:
        main()
    finally:
        print(f"[{ts()}] Shutting down…")
        try:
            cluster.shutdown()
        except Exception as e:
            print(f"[{ts()}] Shutdown error: {e}")
        print(f"[{ts()}] Done.")
