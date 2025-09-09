#!/usr/bin/env python3
# prices/dq_repair_timeseries.py
import os, time, requests, datetime as dt
from datetime import timedelta, timezone
import sys, pathlib
from collections import defaultdict

# ───────────────────── Repo root & helpers ─────────────────────
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.append(str(_REPO_ROOT))

try:
    from paths import rel, chdir_repo_root
except Exception:
    def rel(*parts: str) -> pathlib.Path:
        return _REPO_ROOT.joinpath(*parts)
    def chdir_repo_root() -> None:
        os.chdir(_REPO_ROOT)

chdir_repo_root()

# ───────────────────────── 3rd-party ─────────────────────────
from cassandra import OperationTimedOut, ReadTimeout, WriteTimeout, DriverException, ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy
from cassandra.query import SimpleStatement, BatchStatement
from dotenv import load_dotenv

# Load .env from repo root explicitly
load_dotenv(dotenv_path=rel(".env"))

# ---------- Config ----------
BUNDLE       = os.getenv("ASTRA_BUNDLE_PATH", "secure-connect.zip")
ASTRA_TOKEN  = os.getenv("ASTRA_TOKEN")
KEYSPACE     = os.getenv("ASTRA_KEYSPACE", "default_keyspace")

# Tables (CoinGecko pipeline)
TABLE_LIVE      = os.getenv("TABLE_LIVE", "gecko_prices_live")
TEN_MIN_TABLE   = os.getenv("TEN_MIN_TABLE", "gecko_prices_10m_7d")
DAILY_TABLE     = os.getenv("DAILY_TABLE", "gecko_candles_daily_contin")
TABLE_ROLLING   = os.getenv("TABLE_ROLLING", "gecko_prices_live_rolling")

TOP_N_DQ               = int(os.getenv("TOP_N_DQ", "210"))
DQ_MAX_COINS           = int(os.getenv("DQ_MAX_COINS", "210"))
DQ_MAX_API_COINS_RUN   = int(os.getenv("DQ_MAX_API_COINS_PER_RUN", "20"))

DAYS_10M     = int(os.getenv("DQ_WINDOW_10M_DAYS", "7"))
DAYS_DAILY   = int(os.getenv("DQ_WINDOW_DAILY_DAYS", "365"))

FIX_DAILY_FROM_10M      = os.getenv("FIX_DAILY_FROM_10M", "1") == "1"
BACKFILL_DAILY_FROM_API = os.getenv("BACKFILL_DAILY_FROM_API", "1") == "1"
SEED_10M_FROM_DAILY     = os.getenv("SEED_10M_FROM_DAILY", "0") == "1"
DRY_RUN                 = os.getenv("DQ_DRY_RUN", "0") == "1"

REQUEST_TIMEOUT = int(os.getenv("DQ_REQUEST_TIMEOUT_SEC", "30"))
CONNECT_TIMEOUT = int(os.getenv("DQ_CONNECT_TIMEOUT_SEC", "15"))
FETCH_SIZE      = int(os.getenv("DQ_FETCH_SIZE", "500"))
RETRIES         = int(os.getenv("DQ_RETRIES", "3"))
BACKOFF_S       = int(os.getenv("DQ_BACKOFF_SEC", "4"))
PAUSE_S         = float(os.getenv("DQ_PAUSE_PER_COIN", "0.1"))

LOG_EVERY   = int(os.getenv("DQ_LOG_EVERY", "10"))
VERBOSE     = os.getenv("DQ_VERBOSE", "0") == "1"
TIME_API    = os.getenv("DQ_TIME_API", "1") == "1"

# CoinGecko API (free/demo or pro)
API_TIER = (os.getenv("COINGECKO_API_TIER") or "demo").strip().lower()
API_KEY  = (os.getenv("COINGECKO_API_KEY") or "").strip()
if API_KEY.lower().startswith("api key:"):
    API_KEY = API_KEY.split(":", 1)[1].strip()
BASE = os.getenv(
    "COINGECKO_BASE_URL",
    "https://api.coingecko.com/api/v3" if API_TIER == "demo" else "https://pro-api.coingecko.com/api/v3"
)
HDR = "x-cg-demo-api-key" if API_TIER == "demo" else "x-cg-pro-api-key"
QS  = "x_cg_demo_api_key" if API_TIER == "demo" else "x_cg_pro_api_key"

if not ASTRA_TOKEN:
    raise SystemExit("Missing ASTRA_TOKEN")

# ---------- Helpers ----------
def ts(): return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
def tdur(t0): return f"{(time.perf_counter()-t0):.2f}s"
def utcnow(): return dt.datetime.now(timezone.utc)

def _to_pydate(x) -> dt.date:
    """
    Coerce various Cassandra/Python types to a plain datetime.date.
    Handles:
      - datetime.date
      - datetime.datetime (uses .date())
      - cassandra.util.Date (string parse fallback)
      - ISO strings like 'YYYY-MM-DD'
      - integer day offsets (Cassandra epoch-days)
    """
    if x is None:
        raise TypeError("Cannot coerce None to date")

    if isinstance(x, dt.date) and not isinstance(x, dt.datetime):
        return x

    if isinstance(x, dt.datetime):
        return x.date()

    s = str(x)
    try:
        return dt.date.fromisoformat(s[:10])
    except Exception:
        pass

    try:
        days = int(x)
        return dt.date(1970, 1, 1) + dt.timedelta(days=days)
    except Exception:
        pass

    raise TypeError(f"Cannot interpret date value {x!r}")

def equalish(a, b, eps=1e-12):
    if a is None and b is None: return True
    if a is None or b is None:  return False
    try:
        return abs(float(a) - float(b)) <= eps
    except Exception:
        return False

def day_bounds_utc(d: dt.date):
    start = dt.datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    end_excl = start + dt.timedelta(days=1)
    return start, end_excl

def date_seq(last_inclusive: dt.date, days: int):
    start = last_inclusive - dt.timedelta(days=days-1)
    return [start + dt.timedelta(days=i) for i in range(days)]

def http_get(path, params=None):
    url = f"{BASE}{path}"
    params = dict(params or {})
    headers = {HDR: API_KEY} if API_KEY else {}
    if API_KEY:
        params[QS] = API_KEY
    last = None
    t0 = time.perf_counter()
    for i in range(RETRIES):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=30)
            if r.status_code in (402, 429, 500, 502, 503, 504):
                raise requests.HTTPError(f"{r.status_code} from CoinGecko", response=r)
            r.raise_for_status()
            if TIME_API:
                print(f"[{ts()}] API OK {path} took {tdur(t0)}")
            return r.json()
        except (requests.HTTPError, requests.ConnectionError, requests.Timeout) as e:
            last = e
            sleep_for = BACKOFF_S * (i + 1)
            print(f"[{ts()}] API {path} error: {e} — backoff {sleep_for}s")
            time.sleep(sleep_for)
    raise RuntimeError(f"CoinGecko failed: {url} :: {last}")

def bucket_daily_ohlc(prices_ms_values, start_d: dt.date, end_d: dt.date):
    per_day = defaultdict(list)
    for ms, price in prices_ms_values or []:
        ts_ = dt.datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        per_day[ts_.date()].append((ts_, float(price)))
    out = {}
    d = start_d
    while d <= end_d:
        pts = sorted(per_day.get(d, []), key=lambda x: x[0])
        if pts:
            vals = [p for _, p in pts]
            out[d] = {
                "open": vals[0],
                "high": max(vals),
                "low":  min(vals),
                "close": vals[-1],
                "last_ts": pts[-1][0],
                "is_true_ohlc": len(vals) > 1,
            }
        d += dt.timedelta(days=1)
    return out

def bucket_daily_last(values_ms_values, start_d: dt.date, end_d: dt.date):
    per_day = defaultdict(list)
    for ms, val in values_ms_values or []:
        ts_ = dt.datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
        v = float(val) if val is not None else None
        per_day[ts_.date()].append((ts_, v))
    out = {}
    d = start_d
    while d <= end_d:
        pts = sorted(per_day.get(d, []), key=lambda x: x[0])
        if pts:
            out[d] = {"val": pts[-1][1], "ts": pts[-1][0]}
        d += dt.timedelta(days=1)
    return out

# ---------- Connect ----------
print(f"[{ts()}] Connecting to Astra (bundle='{BUNDLE}', keyspace='{KEYSPACE}')")
auth = PlainTextAuthProvider("token", ASTRA_TOKEN)
exec_profile = ExecutionProfile(load_balancing_policy=RoundRobinPolicy(),
                                request_timeout=REQUEST_TIMEOUT)
cluster = Cluster(cloud={"secure_connect_bundle": BUNDLE},
                  auth_provider=auth,
                  execution_profiles={EXEC_PROFILE_DEFAULT: exec_profile},
                  connect_timeout=CONNECT_TIMEOUT)
session = cluster.connect(KEYSPACE)
print(f"[{ts()}] Connected.")

# ---------- Prepared statements ----------
SEL_LIVE = SimpleStatement(
    f"SELECT id, symbol, name, market_cap_rank FROM {TABLE_LIVE}",
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

# Enrichment from rolling for API repair
SEL_ROLLING_RANGE = session.prepare(f"""
  SELECT last_updated, market_cap_rank, circulating_supply, total_supply
  FROM {TABLE_ROLLING}
  WHERE id = ? AND last_updated >= ? AND last_updated < ?
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

# ---------- Core functions ----------
def top_assets(limit: int):
    t0 = time.perf_counter()
    rows = list(session.execute(SEL_LIVE, timeout=REQUEST_TIMEOUT))
    rows = [r for r in rows if isinstance(r.market_cap_rank, int) and r.market_cap_rank > 0]
    rows.sort(key=lambda r: r.market_cap_rank)
    rows = rows[:limit]
    print(f"[{ts()}] Loaded {len(rows)} assets from {TABLE_LIVE} in {tdur(t0)}")
    return rows

def existing_days_10m(coin_id: str, start_dt: dt.datetime, end_dt: dt.datetime):
    t0 = time.perf_counter()
    have = set()
    for row in session.execute(SEL_10M_RANGE_DAYS, [coin_id, start_dt, end_dt], timeout=REQUEST_TIMEOUT):
        have.add(_to_pydate(row.ts.date()))
    if VERBOSE:
        print(f"  · fetched 10m days={len(have)} in {tdur(t0)}")
    return have

def existing_days_daily(coin_id: str, start_date: dt.date, end_date: dt.date):
    t0 = time.perf_counter()
    have = set()
    for row in session.execute(SEL_DAILY_RANGE, [coin_id, start_date, end_date], timeout=REQUEST_TIMEOUT):
        have.add(_to_pydate(row.date))
    if VERBOSE:
        print(f"  · fetched daily days={len(have)} in {tdur(t0)}")
    return have

def _daily_row_equal(existing, o, h, l, c, mcap, vol, source):
    if not existing: return False
    return (
        equalish(getattr(existing, "open", None),  o) and
        equalish(getattr(existing, "high", None),  h) and
        equalish(getattr(existing, "low", None),   l) and
        equalish(getattr(existing, "close", None), c) and
        equalish(getattr(existing, "market_cap", None), mcap) and
        equalish(getattr(existing, "volume_24h", None), vol) and
        (getattr(existing, "candle_source", None) == source)
    )

def repair_daily_from_10m(coin, missing_days: set[dt.date]) -> int:
    """Compute daily OHLC from 10m and upsert (candle_source='10m_final')."""
    if not (FIX_DAILY_FROM_10M and missing_days): return 0
    print(f"[{ts()}]    [repair_daily_from_10m] {coin.symbol}: {len(missing_days)} day(s) to fix")
    t0 = time.perf_counter()
    cnt = 0
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for idx, day in enumerate(sorted(missing_days), 1):
        day_start, day_end_excl = day_bounds_utc(day)
        try:
            t_read = time.perf_counter()
            pts = list(session.execute(SEL_10M_RANGE_FULL, [coin.id, day_start, day_end_excl], timeout=REQUEST_TIMEOUT))
            print(f"[{ts()}]      · {coin.symbol} {day} → 10m rows={len(pts)} in {tdur(t_read)}")
        except (OperationTimedOut, ReadTimeout, DriverException) as e:
            print(f"  · [READ-ERR] {coin.symbol} {day}: {e}")
            continue

        prices, mcaps, vols = [], [], []
        ranks, circs, tots = [], [], []
        last_ts = None
        for p in pts:
            last_ts = p.ts or last_ts
            if p.price_usd is not None: prices.append(float(p.price_usd))
            if p.market_cap is not None: mcaps.append(float(p.market_cap))
            if p.volume_24h is not None: vols.append(float(p.volume_24h))
            if getattr(p, "market_cap_rank", None) is not None: ranks.append(int(p.market_cap_rank))
            if getattr(p, "circulating_supply", None) is not None: circs.append(float(p.circulating_supply))
            if getattr(p, "total_supply", None) is not None: tots.append(float(p.total_supply))
        if not prices:
            continue

        o, h, l, c = prices[0], max(prices), min(prices), prices[-1]
        mcap = mcaps[-1] if mcaps else None
        vol  = vols[-1]  if vols  else None
        rnk  = ranks[-1] if ranks else None
        circ = circs[-1] if circs else None
        tot  = tots[-1]  if tots else None
        last_upd = last_ts or (day_end_excl - dt.timedelta(seconds=1))

        existing = session.execute(SEL_DAILY_ONE, [coin.id, day], timeout=REQUEST_TIMEOUT).one()
        if _daily_row_equal(existing, o, h, l, c, mcap, vol, "10m_final"):
            if VERBOSE: print(f"  · skip unchanged daily {coin.symbol} {day}")
            continue

        if not DRY_RUN:
            batch.add(INS_DAY, (
                coin.id, day, coin.symbol, coin.name,
                o, h, l, c, c,
                mcap, vol,
                rnk, circ, tot,
                "10m_final", last_upd
            ))
        cnt += 1
        if (idx % 25 == 0) or (idx == len(missing_days)):
            try:
                bs = len(batch)
            except Exception:
                bs = "n/a"
            print(f"[{ts()}]      · progress {idx}/{len(missing_days)} (batch_size={bs})")
        if (cnt % 40 == 0) and not DRY_RUN:
            print(f"[{ts()}]      · flushing batch at cnt={cnt}")
            session.execute(batch); batch.clear()

    if (cnt % 40 != 0) and not DRY_RUN and len(batch) > 0:
        print(f"[{ts()}]      · final flush at cnt={cnt}")
        session.execute(batch)

    print(f"[{ts()}]    [repair_daily_from_10m] {coin.symbol} wrote={cnt} ({tdur(t0)})")
    return cnt

def seed_10m_from_daily(coin, missing_days: set[dt.date]) -> int:
    """OPTIONAL: seed a synthetic 10m row at 23:59:59Z using the daily close."""
    if not (SEED_10M_FROM_DAILY and missing_days): return 0
    print(f"[{ts()}]    [seed_10m] {coin.symbol}: {len(missing_days)} day(s) to seed")
    t0 = time.perf_counter()
    cnt = 0
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for idx, day in enumerate(sorted(missing_days), 1):
        row = session.execute(SEL_DAILY_ONE, [coin.id, day], timeout=REQUEST_TIMEOUT).one()
        if not row: continue
        close = float(row.close if row.close is not None else (row.price_usd or 0.0))
        ts_ = dt.datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=timezone.utc)
        mcap = float(row.market_cap or 0.0)
        vol  = float(row.volume_24h or 0.0)
        rnk  = getattr(row, "market_cap_rank", None)
        circ = float(row.circulating_supply) if row.circulating_supply is not None else None
        tot  = float(row.total_supply)       if row.total_supply       is not None else None
        last_updated = getattr(row, "last_updated", None) or ts_
        if not DRY_RUN:
            batch.add(INS_10M, (
                coin.id, ts_, coin.symbol, coin.name, close, mcap, vol,
                rnk, circ, tot, last_updated
            ))
        cnt += 1
        if (idx % 25 == 0) or (idx == len(missing_days)):
            try:
                bs = len(batch)
            except Exception:
                bs = "n/a"
            print(f"[{ts()}]      · seed progress {idx}/{len(missing_days)} (batch_size={bs})")
        if (cnt % 40 == 0) and not DRY_RUN:
            print(f"[{ts()}]      · seed flush at cnt={cnt}")
            session.execute(batch); batch.clear()

    if (cnt % 40 != 0) and not DRY_RUN and len(batch) > 0:
        print(f"[{ts()}]      · seed final flush at cnt={cnt}")
        session.execute(batch)
    print(f"[{ts()}]    [seed_10m] {coin.symbol} wrote={cnt} ({tdur(t0)})")
    return cnt

def backfill_daily_from_api(coin, need_daily: set[dt.date]) -> int:
    """
    Fetch daily via CoinGecko /coins/{id}/market_chart/range and upsert.
    If multiple intraday points → true OHLC ('hourly'); else prev-close/flat.
    Enrich rank/supplies per day from rolling states (<= day end).
    """
    if not (BACKFILL_DAILY_FROM_API and need_daily): return 0

    days = sorted(need_daily)
    start_day = days[0]; end_day = days[-1]
    start_dt, _ = day_bounds_utc(start_day)
    end_dt = dt.datetime(end_day.year, end_day.month, end_day.day, 23, 59, 59, tzinfo=timezone.utc)

    print(f"[{ts()}]    [api] {coin.symbol} market_chart/range {start_day}→{end_day} …")
    try:
        data = http_get(f"/coins/{coin.id}/market_chart/range", params={
            "vs_currency": "usd",
            "from": int(start_dt.timestamp()),
            "to":   int(end_dt.timestamp()),
        })
    except Exception as e:
        print(f"  · API backfill failed for {coin.id}: {e}")
        return 0

    prices = data.get("prices", []) or []
    mcaps  = data.get("market_caps", []) or []
    vols   = data.get("total_volumes", []) or []
    print(f"[{ts()}]    [api] payload sizes: prices={len(prices)} mcaps={len(mcaps)} vols={len(vols)}")

    ohlc  = bucket_daily_ohlc(prices, start_day, end_day)
    m_last = bucket_daily_last(mcaps, start_day, end_day)
    v_last = bucket_daily_last(vols,  start_day, end_day)
    print(f"[{ts()}]    [api] bucketed days with price={len(ohlc)}")

    # Enrich from rolling for the entire window
    roll_rows = list(session.execute(SEL_ROLLING_RANGE, [
        coin.id,
        start_dt,
        end_dt + dt.timedelta(seconds=1)
    ], timeout=REQUEST_TIMEOUT))
    roll_rows.sort(key=lambda r: r.last_updated)
    st_i = -1
    cur_rank = getattr(coin, "market_cap_rank", None)
    cur_circ = None
    cur_tot  = None

    def advance_state_until(day_end_dt):
        nonlocal st_i, cur_rank, cur_circ, cur_tot
        while st_i + 1 < len(roll_rows) and roll_rows[st_i + 1].last_updated <= day_end_dt:
            st_i += 1
            rr = roll_rows[st_i]
            if rr.market_cap_rank is not None: cur_rank = rr.market_cap_rank
            if rr.circulating_supply is not None: cur_circ = float(rr.circulating_supply)
            if rr.total_supply is not None: cur_tot = float(rr.total_supply)

    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    cnt_daily = 0
    prev_close = None

    for di, d in enumerate(days, 1):
        row = ohlc.get(d)
        mcap = m_last.get(d, {}).get("val")
        vol  = v_last.get(d,  {}).get("val")
        if not row:
            continue

        o, h, l, c, last_ts = row["open"], row["high"], row["low"], row["close"], row["last_ts"]
        csrc = "hourly" if row.get("is_true_ohlc") else "prev_close"
        if not row.get("is_true_ohlc"):
            if prev_close is None:
                o = h = l = c
                csrc = "flat"
            else:
                o = float(prev_close)
                h = max(o, c)
                l = min(o, c)
                csrc = "prev_close"

        day_end = day_bounds_utc(d)[1] - dt.timedelta(seconds=1)
        advance_state_until(day_end)

        existing = session.execute(SEL_DAILY_ONE, [coin.id, d], timeout=REQUEST_TIMEOUT).one()
        if _daily_row_equal(existing, o, h, l, c, mcap, vol, csrc):
            prev_close = c
            if (di % 25 == 0) or (di == len(days)):
                try:
                    bs = len(batch)
                except Exception:
                    bs = "n/a"
                print(f"[{ts()}]      · API {coin.symbol} day {di}/{len(days)} wrote={cnt_daily} batch_size={bs} (unchanged)")
            continue

        if not DRY_RUN:
            batch.add(INS_DAY, (
                coin.id, d, coin.symbol, coin.name,
                float(o), float(h), float(l), float(c), float(c),
                float(mcap) if mcap is not None else None,
                float(vol)  if vol  is not None else None,
                cur_rank, cur_circ, cur_tot,
                csrc, last_ts if row.get("is_true_ohlc") else day_end
            ))
        cnt_daily += 1
        prev_close = c

        if (cnt_daily % 40 == 0) and not DRY_RUN:
            print(f"[{ts()}]      · API flush at cnt_daily={cnt_daily}")
            session.execute(batch); batch.clear()

        if (di % 25 == 0) or (di == len(days)):
            try:
                bs = len(batch)
            except Exception:
                bs = "n/a"
            print(f"[{ts()}]      · API {coin.symbol} day {di}/{len(days)} wrote={cnt_daily} batch_size={bs}")

    if (cnt_daily % 40 != 0) and not DRY_RUN and len(batch) > 0:
        print(f"[{ts()}]      · API final flush at cnt_daily={cnt_daily}")
        session.execute(batch)
    print(f"[{ts()}]    [api] {coin.symbol} wrote={cnt_daily}")
    return cnt_daily

# ---------- Main ----------
def main():
    now = utcnow()
    end_excl = dt.datetime(now.year, now.month, now.day, tzinfo=timezone.utc) + dt.timedelta(days=1)

    last_inclusive   = end_excl.date() - dt.timedelta(days=1)
    start_10m_date   = last_inclusive - dt.timedelta(days=DAYS_10M-1)
    start_daily_date = last_inclusive - dt.timedelta(days=DAYS_DAILY-1)

    want_10m_days   = set(date_seq(last_inclusive, DAYS_10M))
    want_daily_days = set(date_seq(last_inclusive, DAYS_DAILY))

    assets = top_assets(min(TOP_N_DQ, DQ_MAX_COINS))
    print(f"[{ts()}] DQ windows → 10m: {start_10m_date.isoformat()}→{last_inclusive.isoformat()} | "
          f"daily: {start_daily_date.isoformat()}→{last_inclusive.isoformat()}")

    coins_needing_api = []
    fixedD_local = fixed10_seeded = 0
    t_all = time.perf_counter()

    for i, coin in enumerate(assets, 1):
        t_coin = time.perf_counter()
        try:
            print(f"[{ts()}] → Coin {i}/{len(assets)}: {coin.symbol} ({coin.id}) rank={coin.market_cap_rank}")

            have_10m = existing_days_10m(
                coin.id,
                dt.datetime.combine(start_10m_date, dt.time.min, tzinfo=timezone.utc),
                dt.datetime.combine(last_inclusive + dt.timedelta(days=1), dt.time.min, tzinfo=timezone.utc)
            )
            print(f"[{ts()}]    Found {len(have_10m)} days with 10m")

            have_daily = existing_days_daily(coin.id, start_daily_date, last_inclusive)
            print(f"[{ts()}]    Found {len(have_daily)} days with daily")

            need_10m   = want_10m_days   - have_10m
            need_daily = want_daily_days - have_daily
            print(f"[{ts()}]    Missing → 10m:{len(need_10m)} daily:{len(need_daily)}")

            # Plan print
            print(f"[{ts()}]    Plan: daily_from_10m={bool(need_daily and FIX_DAILY_FROM_10M)} "
                  f"seed_10m={bool(need_10m and SEED_10M_FROM_DAILY)} "
                  f"api_pass={bool(need_daily and BACKFILL_DAILY_FROM_API)}")

            if need_daily:
                fixed = repair_daily_from_10m(coin, need_daily)
                fixedD_local += fixed
                print(f"[{ts()}]    repair_daily_from_10m → wrote {fixed} rows")
                if fixed:
                    have_daily = existing_days_daily(coin.id, start_daily_date, last_inclusive)
                    need_daily = want_daily_days - have_daily

            if need_10m and SEED_10M_FROM_DAILY:
                fixed = seed_10m_from_daily(coin, need_10m)
                fixed10_seeded += fixed
                print(f"[{ts()}]    seed_10m_from_daily → wrote {fixed} rows")

            if need_daily:
                print(f"[{ts()}]    → defer to API: {len(need_daily)} daily day(s)")
                coins_needing_api.append((coin, need_daily))

            time.sleep(PAUSE_S)
            elapsed_coin = time.perf_counter() - t_coin
            print(f"[{ts()}] ← Done {coin.symbol} in {elapsed_coin:.2f}s")

            if (i % LOG_EVERY == 0) and not VERBOSE:
                print(f"[{ts()}] Progress {i}/{len(assets)} (last coin {elapsed_coin:.2f}s)")

        except Exception as e:
            print(f"[{ts()}] [WARN] coin {coin.symbol} ({coin.id}) failed: {e}")
            continue

    fixedD_api = 0
    if BACKFILL_DAILY_FROM_API and coins_needing_api:
        api_targets = coins_needing_api[:DQ_MAX_API_COINS_RUN]
        print(f"[{ts()}] Coins still missing daily after local repair: {len(coins_needing_api)}; "
              f"API target this run: {len(api_targets)} (cap={DQ_MAX_API_COINS_RUN})")
        for j, (coin, need_daily) in enumerate(api_targets, 1):
            if VERBOSE:
                print(f"[{ts()}] API repair {j}/{len(api_targets)} → {coin.symbol} ({coin.id}) "
                      f"need_daily={len(need_daily)}")
            try:
                fixedD_api += backfill_daily_from_api(coin, need_daily)
            except Exception as e:
                print(f"[{ts()}]  · API repair failed for {coin.symbol}: {e}")
            time.sleep(PAUSE_S)
    else:
        print(f"[{ts()}] No API pass needed or disabled.")

    print(f"[{ts()}] DONE in {tdur(t_all)} | Local fixes → daily_from_10m:{fixedD_local} "
          f"{'(10m_seeded:'+str(fixed10_seeded)+')' if SEED_10M_FROM_DAILY else ''} "
          f"| API fixes → daily:{fixedD_api} "
          f"| Remaining coins needing API on future runs: "
          f"{max(0, len(coins_needing_api) - (0 if not BACKFILL_DAILY_FROM_API else DQ_MAX_API_COINS_RUN))}")

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
