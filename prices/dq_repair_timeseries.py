# prices/dq_repair_timeseries.py
import os, time, requests, datetime as dt
from datetime import timedelta, timezone
import sys, pathlib

# ───────────────────── Repo root & helpers ─────────────────────
# Make the backend repo root importable (two levels up from this file)
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.append(str(_REPO_ROOT))

# Try to use shared helper; fall back if not present
try:
    from paths import rel, chdir_repo_root
except Exception:
    def rel(*parts: str) -> pathlib.Path:
        return _REPO_ROOT.joinpath(*parts)
    def chdir_repo_root() -> None:
        os.chdir(_REPO_ROOT)

# Ensure consistent CWD (so relative files like secure-connect.zip work)
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

# Table names (configurable)
TABLE_LIVE      = os.getenv("TABLE_LIVE", os.getenv("TABLE_LATEST", "prices_live"))
TEN_MIN_TABLE   = os.getenv("TEN_MIN_TABLE", "prices_10m_7d")
DAILY_TABLE     = os.getenv("DAILY_TABLE", "candles_daily_contin")

TOP_N_DQ               = int(os.getenv("TOP_N_DQ", "110"))
DQ_MAX_COINS           = int(os.getenv("DQ_MAX_COINS", "110"))
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

if not ASTRA_TOKEN:
    raise SystemExit("Missing ASTRA_TOKEN")

# ---------- Helpers ----------
def ts(): return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
def tdur(t0): return f"{(time.perf_counter()-t0):.2f}s"
def utcnow(): return dt.datetime.now(timezone.utc)

def _to_pydate(x) -> dt.date:
    return dt.date(x.year, x.month, x.day)

def equalish(a, b, eps=1e-12):
    if a is None and b is None: return True
    if a is None or b is None:  return False
    try:
        return abs(float(a) - float(b)) <= eps
    except Exception:
        return False

def iso_z(dt_obj: dt.datetime) -> str:
    return dt_obj.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

def to_dt(iso_s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(iso_s.replace("Z", "+00:00")).astimezone(timezone.utc)

def date_seq(last_inclusive: dt.date, days: int):
    start = last_inclusive - dt.timedelta(days=days-1)
    return [start + dt.timedelta(days=i) for i in range(days)]

def paprika_get(url, params=None):
    last = None
    t0 = time.perf_counter()
    for i in range(RETRIES):
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 200:
            if TIME_API:
                print(f"[{ts()}] API OK {url} took {tdur(t0)}")
            return r.json()
        msg = (r.text or "")[:200].replace("\n", " ")
        print(f"[{ts()}] API {url} -> {r.status_code}: {msg}")
        if r.status_code in (402, 429, 500, 502, 503, 504):
            sleep_for = BACKOFF_S * (i + 1)
            print(f"[{ts()}]  …backing off {sleep_for}s (attempt {i+1}/{RETRIES})")
            time.sleep(sleep_for)
            continue
        try:
            r.raise_for_status()
        except Exception as e:
            last = e
    raise RuntimeError(f"Paprika failed: {url} :: {last}")

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
    f"SELECT id, symbol, name, rank FROM {TABLE_LIVE}",
    fetch_size=FETCH_SIZE
)

SEL_10M_RANGE_FULL = session.prepare(f"""
  SELECT ts, price_usd, market_cap, volume_24h
  FROM {TEN_MIN_TABLE}
  WHERE id = ? AND ts >= ? AND ts < ?
""")

SEL_10M_RANGE_DAYS = session.prepare(f"""
  SELECT ts FROM {TEN_MIN_TABLE}
  WHERE id = ? AND ts >= ? AND ts < ?
""")

SEL_DAILY_ONE = session.prepare(f"""
  SELECT date, price_usd, market_cap, volume_24h, last_updated,
         open, high, low, close, candle_source
  FROM {DAILY_TABLE}
  WHERE id = ? AND date = ? LIMIT 1
""")

SEL_DAILY_RANGE = session.prepare(f"""
  SELECT date FROM {DAILY_TABLE}
  WHERE id = ? AND date >= ? AND date <= ?
""")

INS_10M = session.prepare(f"""
  INSERT INTO {TEN_MIN_TABLE}
    (id, ts, symbol, name, rank, price_usd, market_cap, volume_24h, last_updated)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

UPD_DAY = session.prepare(f"""
  UPDATE {DAILY_TABLE} SET
    symbol = ?, name = ?, rank = ?,
    price_usd = ?, market_cap = ?, volume_24h = ?, last_updated = ?,
    open = ?, high = ?, low = ?, close = ?, candle_source = ?
  WHERE id = ? AND date = ?
""")

# ---------- Core functions ----------
def top_assets(limit: int):
    t0 = time.perf_counter()
    rows = list(session.execute(SEL_LIVE, timeout=REQUEST_TIMEOUT))
    rows = [r for r in rows if isinstance(r.rank, int) and r.rank > 0]
    rows.sort(key=lambda r: r.rank)
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
    """Compute OHLC from *all* 10m points of each missing UTC day and upsert daily (skip unchanged)."""
    if not (FIX_DAILY_FROM_10M and missing_days): return 0
    t0 = time.perf_counter()
    cnt = 0
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for day in sorted(missing_days):
        day_start = dt.datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
        day_end   = day_start + dt.timedelta(days=1)
        pts = session.execute(SEL_10M_RANGE_FULL, [coin.id, day_start, day_end], timeout=REQUEST_TIMEOUT)
        prices, mcaps, vols, last_ts = [], [], [], None
        for p in pts:
            last_ts = p.ts
            if p.price_usd is not None: prices.append(float(p.price_usd))
            if p.market_cap is not None: mcaps.append(float(p.market_cap))
            if p.volume_24h is not None: vols.append(float(p.volume_24h))
        if not prices:
            continue
        o, h, l, c = prices[0], max(prices), min(prices), prices[-1]
        mcap = mcaps[-1] if mcaps else None
        vol  = vols[-1]  if vols  else None
        last_upd = last_ts or (day_end - timedelta(seconds=1))

        # skip unchanged (use the same source tag as your writer for final days)
        existing = session.execute(SEL_DAILY_ONE, [coin.id, day], timeout=REQUEST_TIMEOUT).one()
        if _daily_row_equal(existing, o, h, l, c, mcap, vol, "10m_final"):
            if VERBOSE: print(f"  · skip unchanged daily {coin.symbol} {day}")
            continue

        if not DRY_RUN:
            batch.add(UPD_DAY, (
                coin.symbol, coin.name, int(coin.rank),
                c, mcap, vol, last_upd,
                o, h, l, c, "10m_final",
                coin.id, day
            ))
        cnt += 1
        if cnt % 40 == 0 and not DRY_RUN:
            session.execute(batch); batch.clear()
    if (cnt % 40 != 0) and not DRY_RUN and len(batch) > 0:
        session.execute(batch)
    if cnt and VERBOSE:
        print(f"  · daily OHLC from 10m: +{cnt} rows ({tdur(t0)})")
    return cnt

def seed_10m_from_daily(coin, missing_days: set[dt.date]) -> int:
    """OPTIONAL: seed one synthetic 10m row at 23:59:59Z using daily close."""
    if not (SEED_10M_FROM_DAILY and missing_days): return 0
    t0 = time.perf_counter()
    cnt = 0
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for day in sorted(missing_days):
        row = session.execute(SEL_DAILY_ONE, [coin.id, day], timeout=REQUEST_TIMEOUT).one()
        if not row: continue
        close = float(row.close if row.close is not None else (row.price_usd or 0.0))
        ts_ = dt.datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=timezone.utc)
        mcap = float(row.market_cap or 0.0)
        vol  = float(row.volume_24h or 0.0)
        last_updated = row.last_updated or ts_
        if not DRY_RUN:
            batch.add(INS_10M, (coin.id, ts_, coin.symbol, coin.name, int(coin.rank), close, mcap, vol, last_updated))
        cnt += 1
        if cnt % 40 == 0 and not DRY_RUN:
            session.execute(batch); batch.clear()
    if (cnt % 40 != 0) and not DRY_RUN and len(batch) > 0:
        session.execute(batch)
    if cnt and VERBOSE:
        print(f"  · 10m seeded from daily close: +{cnt} rows ({tdur(t0)})")
    return cnt

def backfill_daily_from_api(coin, need_daily: set[dt.date]) -> int:
    """Fetch daily snapshots (1d) and upsert prev-close style (skip unchanged)."""
    if not (BACKFILL_DAILY_FROM_API and need_daily): return 0
    days = sorted(need_daily)
    start_day = days[0]; end_day = days[-1] + dt.timedelta(days=1)
    start_dt  = dt.datetime(start_day.year, start_day.month, start_day.day, tzinfo=timezone.utc)
    end_dt    = dt.datetime(end_day.year, end_day.month, end_day.day, tzinfo=timezone.utc)

    url = f"https://api.coinpaprika.com/v1/tickers/{coin.id}/historical"
    t0 = time.perf_counter()
    try:
        data = paprika_get(url, params={
            "start": iso_z(start_dt), "end": iso_z(end_dt),
            "interval": "1d", "quote": "usd"
        })
    except Exception as e:
        print(f"  · API backfill failed for {coin.id}: {e} ({tdur(t0)})")
        return 0

    data.sort(key=lambda d: d.get("timestamp") or "")
    by_day = {}
    for d in data:
        ts_dt = to_dt(d["timestamp"])
        by_day[_to_pydate(ts_dt.date())] = {
            "ts": ts_dt,
            "price": float(d.get("price") or 0.0),
            "mcap":  float(d.get("market_cap") or 0.0),
            "vol":   float(d.get("volume_24h") or 0.0),
        }

    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    cnt_daily = 0
    prev_close = None
    for day in days:
        v = by_day.get(day)
        if not v:
            continue
        close = v["price"]
        if prev_close is None:
            o = h = l = close
            source = "daily_flat"
        else:
            o = prev_close
            h = max(o, close)
            l = min(o, close)
            source = "daily_prevclose"

        # skip unchanged
        existing = session.execute(SEL_DAILY_ONE, [coin.id, day], timeout=REQUEST_TIMEOUT).one()
        if _daily_row_equal(existing, o, h, l, close, v["mcap"], v["vol"], source):
            if VERBOSE: print(f"  · skip unchanged API daily {coin.symbol} {day}")
            prev_close = close
            continue

        if not DRY_RUN:
            batch.add(UPD_DAY, (
                coin.symbol, coin.name, int(coin.rank),
                close, v["mcap"], v["vol"], v["ts"],
                o, h, l, close, source,
                coin.id, day
            ))
        cnt_daily += 1
        prev_close = close
        if cnt_daily % 40 == 0 and not DRY_RUN:
            session.execute(batch); batch.clear()
    if (cnt_daily % 40 != 0) and not DRY_RUN and len(batch) > 0:
        session.execute(batch)

    if VERBOSE:
        print(f"  · API fixed daily={cnt_daily} ({tdur(t0)})")
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
    fixed10_local = fixedD_local = fixed10_seeded = 0
    t_all = time.perf_counter()

    for i, coin in enumerate(assets, 1):
        t_coin = time.perf_counter()
        try:
            print(f"[{ts()}] → Coin {i}/{len(assets)}: {coin.symbol} ({coin.id}) rank={coin.rank}")

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

            if need_daily:
                fixed = repair_daily_from_10m(coin, need_daily)
                fixedD_local += fixed
                if fixed:
                    have_daily = existing_days_daily(coin.id, start_daily_date, last_inclusive)
                    need_daily = want_daily_days - have_daily

            if need_10m and SEED_10M_FROM_DAILY:
                fixed = seed_10m_from_daily(coin, need_10m)
                fixed10_seeded += fixed

            if need_daily:
                coins_needing_api.append((coin, need_daily))

            time.sleep(PAUSE_S)
            elapsed_coin = time.perf_counter() - t_coin
            print(f"[{ts()}] ← Done {coin.symbol} in {elapsed_coin:.2f}s")

            if (i % LOG_EVERY == 0) and not VERBOSE:
                print(f"[{ts()}] Progress {i}/{len(assets)} (last coin {elapsed_coin:.2f}s)")

        except Exception as e:
            # Protect the loop: one bad coin shouldn't abort the run
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
