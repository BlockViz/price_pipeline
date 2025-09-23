#!/usr/bin/env python3
# backend/prices/EE_create_daily_from_10m.py
import os, time
from datetime import datetime, timedelta, timezone, date
import sys, pathlib

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
from cassandra import OperationTimedOut, ReadTimeout, ReadFailure, WriteTimeout, DriverException
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy
from cassandra.query import SimpleStatement
from dotenv import load_dotenv

# Load .env from repo root explicitly
load_dotenv(dotenv_path=rel(".env"))

# ───────────────────────── Config ─────────────────────────
BUNDLE              = os.getenv("ASTRA_BUNDLE_PATH", "secure-connect.zip")
ASTRA_TOKEN         = os.getenv("ASTRA_TOKEN")
KEYSPACE            = os.getenv("ASTRA_KEYSPACE", "default_keyspace")

TOP_N               = int(os.getenv("TOP_N", "200"))
REQUEST_TIMEOUT     = int(os.getenv("REQUEST_TIMEOUT_SEC", "30"))
CONNECT_TIMEOUT_SEC = int(os.getenv("CONNECT_TIMEOUT_SEC", "15"))
FETCH_SIZE          = int(os.getenv("FETCH_SIZE", "500"))

# Behavior
SLOT_DELAY_SEC      = int(os.getenv("SLOT_DELAY_SEC", "120"))     # guard against too-fresh 10m slot
PROGRESS_EVERY      = int(os.getenv("PROGRESS_EVERY", "20"))      # coin progress interval
VERBOSE_MODE        = os.getenv("VERBOSE_MODE", "0") == "1"       # extra per-coin logs

TEN_MIN_TABLE       = os.getenv("TEN_MIN_TABLE", "gecko_prices_10m_7d")
DAILY_TABLE         = os.getenv("DAILY_TABLE", "gecko_candles_daily_contin")
TABLE_LIVE          = os.getenv("TABLE_LIVE", "gecko_prices_live")
TABLE_MCAP_DAILY    = os.getenv("TABLE_MCAP_DAILY", "gecko_market_cap_daily_contin")

# require ≥2 ten-minute points to finalize a closed day
MIN_10M_POINTS_FOR_FINAL = int(os.getenv("MIN_10M_POINTS_FOR_FINAL", "2"))

if not ASTRA_TOKEN:
    raise SystemExit("Missing ASTRA_TOKEN")

def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def floor_10m(dt_utc: datetime) -> datetime:
    return dt_utc.replace(minute=(dt_utc.minute // 10) * 10, second=0, microsecond=0)

def sanitize_num(x, fallback=None):
    try:
        if x is None:
            return fallback
        return float(x)
    except Exception:
        return fallback

def equalish(a, b):
    if a is None and b is None: return True
    if a is None or b is None:  return False
    try:
        return abs(float(a) - float(b)) <= 1e-12
    except Exception:
        return False

def day_bounds_utc(d: date):
    start = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    end_excl = start + timedelta(days=1)
    return start, end_excl

print(f"[{now_str()}] Connecting to Astra (bundle='{BUNDLE}', keyspace='{KEYSPACE}')")
auth = PlainTextAuthProvider("token", ASTRA_TOKEN)
exec_profile = ExecutionProfile(load_balancing_policy=RoundRobinPolicy(), request_timeout=REQUEST_TIMEOUT)
cluster = Cluster(
    cloud={"secure_connect_bundle": BUNDLE},
    auth_provider=auth,
    execution_profiles={EXEC_PROFILE_DEFAULT: exec_profile},
    connect_timeout=CONNECT_TIMEOUT_SEC,
)
s = cluster.connect(KEYSPACE)
print(f"[{now_str()}] Connected.")

# ───────────────────────── Statements ─────────────────────────
SEL_LIVE = SimpleStatement(f"""
  SELECT id, symbol, name, market_cap_rank, price_usd, market_cap, volume_24h, last_updated, category
  FROM {TABLE_LIVE}
""", fetch_size=FETCH_SIZE)

# Order ASC so we can take first/last deterministically
SEL_10M_RANGE = s.prepare(f"""
  SELECT ts, price_usd, market_cap, volume_24h,
         market_cap_rank, circulating_supply, total_supply, last_updated
  FROM {TEN_MIN_TABLE}
  WHERE id=? AND ts>=? AND ts<? ORDER BY ts ASC
""")

SEL_10M_PREV = s.prepare(f"""
  SELECT ts, price_usd, market_cap, volume_24h,
         market_cap_rank, circulating_supply, total_supply, last_updated
  FROM {TEN_MIN_TABLE}
  WHERE id=? AND ts<? LIMIT 1
""")

SEL_DAILY_ONE = s.prepare(f"""
  SELECT symbol, name, open, high, low, close, price_usd, market_cap, volume_24h,
         market_cap_rank, circulating_supply, total_supply, candle_source, last_updated
  FROM {DAILY_TABLE}
  WHERE id=? AND date=? LIMIT 1
""")

INS_UPSERT = s.prepare(f"""
  INSERT INTO {DAILY_TABLE}
    (id, date, symbol, name,
     open, high, low, close, price_usd,
     market_cap, volume_24h,
     market_cap_rank, circulating_supply, total_supply,
     candle_source, last_updated)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# ── UPDATED to match schema: gecko_market_cap_daily_contin(category, date, last_updated, market_cap, market_cap_rank, volume_24h)
INS_MCAP_DAILY = s.prepare(f"""
  INSERT INTO {TABLE_MCAP_DAILY}
    (category, date, last_updated, market_cap, market_cap_rank, volume_24h)
  VALUES (?, ?, ?, ?, ?, ?)
""")

# ───────────────────────── Helpers ─────────────────────────
def compute_daily_from_10m(rows, allow_finalize: bool):
    """
    Given 10m rows (ASC), build daily aggregates and ancillary fields.
    Returns dict or None:
      { 'open','high','low','close','price_usd','market_cap','volume_24h',
        'market_cap_rank','circulating_supply','total_supply',
        'last_updated' }
    """
    prices, mcaps, vols = [], [], []
    ranks, circs, tots = [], [], []
    last_upd_candidates = []

    for r in rows:
        if r.price_usd   is not None: prices.append(float(r.price_usd))
        if r.market_cap  is not None: mcaps.append(float(r.market_cap))
        if r.volume_24h  is not None: vols.append(float(r.volume_24h))
        if getattr(r, "market_cap_rank", None) is not None: ranks.append(int(r.market_cap_rank))
        if getattr(r, "circulating_supply", None) is not None: circs.append(float(r.circulating_supply))
        if getattr(r, "total_supply", None) is not None: tots.append(float(r.total_supply))
        if getattr(r, "last_updated", None) is not None: last_upd_candidates.append(r.last_updated)

    if not prices:
        return None

    if allow_finalize and len(prices) < MIN_10M_POINTS_FOR_FINAL:
        # not enough data to mark a closed day final
        return None

    o = prices[0]
    h = max(prices)
    l = min(prices)
    c = prices[-1]

    mcap = mcaps[-1] if mcaps else None
    vol  = vols[-1]  if vols  else None
    rank = ranks[-1] if ranks else None
    circ = circs[-1] if circs else None
    tot  = tots[-1]  if tots  else None

    last_upd = max(last_upd_candidates) if last_upd_candidates else None
    return {
        "open": o, "high": h, "low": l, "close": c, "price_usd": c,
        "market_cap": mcap, "volume_24h": vol,
        "market_cap_rank": rank, "circulating_supply": circ, "total_supply": tot,
        "last_updated": last_upd,
    }

def write_daily_row(coin, d: date, agg: dict, candle_source: str):
    """Upsert a single daily row; reuse existing ancillary values if agg lacks them."""
    # read existing to avoid nulling fields and to skip unchanged
    existing = None
    try:
        existing = s.execute(SEL_DAILY_ONE, [coin.id, d], timeout=REQUEST_TIMEOUT).one()
    except (OperationTimedOut, ReadTimeout, DriverException):
        pass

    o = agg.get("open")
    h = agg.get("high")
    l = agg.get("low")
    c = agg.get("close")
    price_usd = agg.get("price_usd", c)

    mcap = agg.get("market_cap")
    vol  = agg.get("volume_24h")
    rnk  = agg.get("market_cap_rank")
    circ = agg.get("circulating_supply")
    tot  = agg.get("total_supply")
    last_upd = agg.get("last_updated")

    # fill from existing row if needed
    if existing:
        if mcap is None: mcap = sanitize_num(getattr(existing, "market_cap", None))
        if vol  is None: vol  = sanitize_num(getattr(existing, "volume_24h", None))
        if rnk  is None: rnk  = getattr(existing, "market_cap_rank", None)
        if circ is None: circ = sanitize_num(getattr(existing, "circulating_supply", None))
        if tot  is None: tot  = sanitize_num(getattr(existing, "total_supply", None))

    # ultimately default mcap/vol to 0.0
    if mcap is None: mcap = 0.0
    if vol  is None: vol  = 0.0

    # choose last_updated
    if not last_upd:
        # if none from 10m, clamp to end-of-day - 1s (UTC)
        start, end_excl = day_bounds_utc(d)
        last_upd = end_excl - timedelta(seconds=1)

    # skip if unchanged vs existing and both sources are final or both partial
    if existing:
        same_vals = all([
            equalish(getattr(existing, "open", None),  o),
            equalish(getattr(existing, "high", None),  h),
            equalish(getattr(existing, "low", None),   l),
            equalish(getattr(existing, "close", None), c),
            equalish(getattr(existing, "price_usd", None), price_usd),
            equalish(getattr(existing, "market_cap", None), mcap),
            equalish(getattr(existing, "volume_24h", None), vol),
            getattr(existing, "candle_source", None) == candle_source
        ])
        if same_vals:
            if VERBOSE_MODE:
                print(f"[{now_str()}]    {coin.symbol} {d} unchanged ({candle_source}) → skip")
            return False

    # upsert
    try:
        s.execute(
            INS_UPSERT,
            [
                coin.id, d, coin.symbol, coin.name,
                o, h, l, c, price_usd,
                mcap, vol,
                rnk, circ, tot,
                candle_source, last_upd
            ],
            timeout=REQUEST_TIMEOUT
        )
        if VERBOSE_MODE:
            print(f"[{now_str()}]    UPSERT {coin.symbol} {d} "
                  f"O={o} H={h} L={l} C={c} M={mcap} V={vol} R={rnk} CS={circ} TS={tot} src={candle_source}")
        return True
    except (WriteTimeout, OperationTimedOut, DriverException) as e:
        print(f"[{now_str()}] [WRITE-ERR] {coin.symbol} {d}: {e}")
        return False

# ───────────────────────── Main ─────────────────────────
def main():
    # Determine days: finalize YESTERDAY (closed) + write TODAY (partial up to safe 10m)
    now_utc = datetime.now(timezone.utc)
    today = now_utc.date()
    yesterday = today - timedelta(days=1)

    today_start, today_end_excl = day_bounds_utc(today)
    last_safe_10m_end = floor_10m(now_utc - timedelta(seconds=SLOT_DELAY_SEC))
    today_effective_end = min(today_end_excl, last_safe_10m_end)
    is_today_partial = today_effective_end < today_end_excl

    print(f"[{now_str()}] Daily windows:"
          f" yesterday[{yesterday} 00:00Z → {yesterday} 24:00Z] (final),"
          f" today[{today_start} → {today_effective_end}] ({'partial' if is_today_partial else 'final'})")

    # Load coins
    print(f"[{now_str()}] Loading top coins (timeout={REQUEST_TIMEOUT}s)…")
    t0 = time.time()
    coins = list(s.execute(SEL_LIVE, timeout=REQUEST_TIMEOUT))
    print(f"[{now_str()}] Loaded {len(coins)} rows from {TABLE_LIVE} in {time.time()-t0:.2f}s")

    coins = [r for r in coins if isinstance(r.market_cap_rank, int) and r.market_cap_rank > 0]
    coins.sort(key=lambda r: r.market_cap_rank)
    coins = coins[:TOP_N]
    print(f"[{now_str()}] Processing top {len(coins)} coins (TOP_N={TOP_N})")

    day_totals = {}

    def bump_day_total(day_key, category, mcap_value, vol_value, last_upd) -> None:
        key = (day_key, category)
        entry = day_totals.setdefault(key, {"market_cap": 0.0, "volume_24h": 0.0, "last_updated": last_upd})
        entry["market_cap"] += mcap_value
        entry["volume_24h"] += vol_value
        if last_upd and (entry["last_updated"] is None or last_upd > entry["last_updated"]):
            entry["last_updated"] = last_upd

    wrote = errors = unchanged = 0

    for idx, c in enumerate(coins, 1):
        coin_category = (getattr(c, 'category', None) or 'Other').strip() or 'Other'
        if idx == 1 or idx % PROGRESS_EVERY == 0 or idx == len(coins):
            print(f"[{now_str()}] → Coin {idx}/{len(coins)}: {getattr(c,'symbol','?')} "
                  f"({getattr(c,'id','?')}) rank={getattr(c,'market_cap_rank','?')}")

        # 1) Finalize yesterday (require enough 10m points)
        y0, y1 = day_bounds_utc(yesterday)
        try:
            pts = list(s.execute(SEL_10M_RANGE, [c.id, y0, y1], timeout=REQUEST_TIMEOUT))
        except (OperationTimedOut, ReadTimeout, ReadFailure, DriverException) as e:
            errors += 1
            print(f"[{now_str()}] [READ-ERR] {c.symbol} yesterday 10m: {e}")
            pts = []

        agg = compute_daily_from_10m(pts, allow_finalize=True)
        if agg:
            mcap_total = sanitize_num(agg.get("market_cap"), 0.0)
            vol_total = sanitize_num(agg.get("volume_24h"), 0.0)
            last_upd_val = agg.get("last_updated") or (day_bounds_utc(yesterday)[1] - timedelta(seconds=1))
            bump_day_total(yesterday, coin_category, mcap_total, vol_total, last_upd_val)
            bump_day_total(yesterday, 'ALL', mcap_total, vol_total, last_upd_val)
            ok = write_daily_row(c, yesterday, agg, candle_source="10m_final")
            wrote += int(bool(ok))
            if not ok:
                unchanged += 1

        # 2) Today (partial up to safe boundary)
        if today_effective_end > today_start:
            try:
                pts = list(s.execute(SEL_10M_RANGE, [c.id, today_start, today_effective_end], timeout=REQUEST_TIMEOUT))
            except (OperationTimedOut, ReadTimeout, ReadFailure, DriverException) as e:
                errors += 1
                print(f"[{now_str()}] [READ-ERR] {c.symbol} today 10m: {e}")
                pts = []

            agg = compute_daily_from_10m(pts, allow_finalize=False)

            if agg:
                mcap_total = sanitize_num(agg.get("market_cap"), 0.0)
                vol_total = sanitize_num(agg.get("volume_24h"), 0.0)
                last_upd_val = agg.get("last_updated") or (day_bounds_utc(today)[1] - timedelta(seconds=1))
                bump_day_total(today, coin_category, mcap_total, vol_total, last_upd_val)
                bump_day_total(today, 'ALL', mcap_total, vol_total, last_upd_val)
                ok = write_daily_row(c, today, agg, candle_source="10m_partial")
                wrote += int(bool(ok))
                if not ok:
                    unchanged += 1
            else:
                # No 10m yet today: make a prev-close style stub using live price
                # Find yesterday close from daily table
                prev_row = None
                try:
                    prev_row = s.execute(SEL_DAILY_ONE, [c.id, yesterday], timeout=REQUEST_TIMEOUT).one()
                except (OperationTimedOut, ReadTimeout, DriverException):
                    prev_row = None

                live_close = sanitize_num(getattr(c, "price_usd", None), 0.0)
                if prev_row and (prev_row.close is not None or prev_row.price_usd is not None):
                    prev_close = float(prev_row.close if prev_row.close is not None else prev_row.price_usd)
                    o, h, l, cl = prev_close, max(prev_close, live_close), min(prev_close, live_close), live_close
                    csrc = "prev_close"
                else:
                    o = h = l = cl = live_close
                    csrc = "flat"

                stub = {
                    "open": o, "high": h, "low": l, "close": cl, "price_usd": cl,
                    "market_cap": sanitize_num(getattr(c, "market_cap", None), 0.0),
                    "volume_24h": sanitize_num(getattr(c, "volume_24h", None), 0.0),
                    "market_cap_rank": getattr(c, "market_cap_rank", None),
                    "circulating_supply": None,
                    "total_supply": None,
                    "last_updated": getattr(c, "last_updated", None),
                }
                mcap_total = sanitize_num(stub.get("market_cap"), 0.0)
                vol_total = sanitize_num(stub.get("volume_24h"), 0.0)
                last_upd_val = stub.get("last_updated") or (day_bounds_utc(today)[1] - timedelta(seconds=1))
                bump_day_total(today, coin_category, mcap_total, vol_total, last_upd_val)
                bump_day_total(today, 'ALL', mcap_total, vol_total, last_upd_val)
                ok = write_daily_row(c, today, stub, candle_source=csrc)
                wrote += int(bool(ok))
                if not ok:
                    unchanged += 1

    if day_totals:
        print(f"[{now_str()}] [mcap-daily] writing {len(day_totals)} aggregates into {TABLE_MCAP_DAILY}")
        agg_written = 0
        for (day_key, category), totals in sorted(day_totals.items(), key=lambda kv: (kv[0][0], 0 if kv[0][1] == 'ALL' else 1, kv[0][1].lower())):
            day_end = day_bounds_utc(day_key)[1]
            last_upd = totals.get('last_updated') or (day_end - timedelta(seconds=1))
            rank_value = 0 if category == 'ALL' else None
            try:
                s.execute(
                    INS_MCAP_DAILY,
                    [category, day_key, last_upd, totals['market_cap'], rank_value, totals['volume_24h']],
                    timeout=REQUEST_TIMEOUT
                )
                agg_written += 1
            except (WriteTimeout, OperationTimedOut, DriverException) as e:
                print(f"[{now_str()}] [mcap-daily] failed for category='{category}' day={day_key}: {e}")
        print(f"[{now_str()}] [mcap-daily] rows_written={agg_written}")
    else:
        print(f"[{now_str()}] [mcap-daily] no aggregates captured (coins={len(coins)})")

    print(f"[{now_str()}] [daily-from-10m] wrote≈{wrote} unchanged≈{unchanged} errors={errors}")

if __name__ == "__main__":
    try:
        main()
    finally:
        print(f"[{now_str()}] Shutting down…")
        try:
            cluster.shutdown()
        except Exception as e:
            print(f"[{now_str()}] Error during shutdown: {e}")
        print(f"[{now_str()}] Done.")
