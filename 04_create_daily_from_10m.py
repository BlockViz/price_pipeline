import os
import time
from datetime import datetime, timedelta, timezone

from cassandra import OperationTimedOut, ReadTimeout, ReadFailure, WriteTimeout, DriverException
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy
from cassandra.query import SimpleStatement
from dotenv import load_dotenv

load_dotenv()

# ---- Config ----
BUNDLE              = os.getenv("ASTRA_BUNDLE_PATH", "secure-connect.zip")
ASTRA_TOKEN         = os.getenv("ASTRA_TOKEN")
KEYSPACE            = os.getenv("ASTRA_KEYSPACE", "default_keyspace")

TOP_N               = int(os.getenv("TOP_N", "200"))
REQUEST_TIMEOUT     = int(os.getenv("REQUEST_TIMEOUT_SEC", "30"))
CONNECT_TIMEOUT_SEC = int(os.getenv("CONNECT_TIMEOUT_SEC", "15"))
FETCH_SIZE          = int(os.getenv("FETCH_SIZE", "500"))

SLOT_DELAY_SEC      = int(os.getenv("SLOT_DELAY_SEC", "120"))     # guard against too-fresh 10m slot
PROGRESS_EVERY      = int(os.getenv("PROGRESS_EVERY", "20"))      # coin progress interval
VERBOSE_MODE        = os.getenv("VERBOSE_MODE", "0") == "1"       # extra per-coin logs

TEN_MIN_TABLE       = os.getenv("TEN_MIN_TABLE", "prices_10m_7d")
DAILY_TABLE         = os.getenv("DAILY_TABLE", "candles_daily_contin")

# require ≥2 ten-minute points to finalize a closed day (safety)
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

SEL_LIVE = SimpleStatement("""
  SELECT id, symbol, name, rank, price_usd, market_cap, volume_24h, last_updated
  FROM prices_live
""", fetch_size=FETCH_SIZE)

def utc_today_bounds():
    now_utc = datetime.now(timezone.utc)
    day = now_utc.date()
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)  # 00:00:00
    end_excl = start + timedelta(days=1)                                  # tomorrow 00:00
    return start, end_excl, day

def main():
    print(f"[{now_str()}] Preparing statements…")
    t0 = time.time()

    # Order ASC so we can take first/last deterministically
    SEL_10M_DAY = s.prepare(f"""
      SELECT ts, price_usd, market_cap, volume_24h
      FROM {TEN_MIN_TABLE}
      WHERE id=? AND ts>=? AND ts<? ORDER BY ts ASC
    """)

    # For prev-close fallback & to compare existing row (avoid churn)
    SEL_PREV_DAY = s.prepare(f"""
      SELECT symbol, name, rank, price_usd, market_cap, volume_24h, last_updated,
             open, high, low, close, candle_source
      FROM {DAILY_TABLE}
      WHERE id=? AND date=? LIMIT 1
    """)

    UPD_DAILY = s.prepare(f"""
      UPDATE {DAILY_TABLE} SET
        symbol = ?, name = ?, rank = ?,
        price_usd = ?, market_cap = ?, volume_24h = ?, last_updated = ?,
        open = ?, high = ?, low = ?, close = ?, candle_source = ?
      WHERE id = ? AND date = ?
    """)

    print(f"[{now_str()}] Prepared in {time.time()-t0:.2f}s")

    # Today window & safe end
    start_utc, end_excl_utc, date_utc = utc_today_bounds()
    last_safe_10m_end = floor_10m(datetime.now(timezone.utc) - timedelta(seconds=SLOT_DELAY_SEC))
    effective_end = min(end_excl_utc, last_safe_10m_end)
    is_partial_day = effective_end < end_excl_utc

    print(f"[{now_str()}] Today UTC: date={date_utc} "
          f"start={start_utc} end_excl={end_excl_utc} "
          f"safe_10m_end={last_safe_10m_end} effective_end={effective_end} "
          f"{'(partial)' if is_partial_day else '(final)'}")

    # Load coins
    print(f"[{now_str()}] Loading top coins (timeout={REQUEST_TIMEOUT}s)…")
    t0 = time.time()
    coins = list(s.execute(SEL_LIVE, timeout=REQUEST_TIMEOUT))
    print(f"[{now_str()}] Loaded {len(coins)} rows from prices_live in {time.time()-t0:.2f}s")

    coins = [r for r in coins if isinstance(r.rank, int) and r.rank > 0]
    coins.sort(key=lambda r: r.rank)
    coins = coins[:TOP_N]
    print(f"[{now_str()}] Processing top {len(coins)} coins (TOP_N={TOP_N})")

    upserts = errors = unchanged_skips = 0

    for idx, r in enumerate(coins, 1):
        if idx == 1 or idx % PROGRESS_EVERY == 0 or idx == len(coins):
            print(f"[{now_str()}] → Coin {idx}/{len(coins)}: {getattr(r,'symbol','?')} "
                  f"({getattr(r,'id','?')}) rank={getattr(r,'rank','?')}")

        # Read today's 10m points up to effective_end
        prices, mcaps, vols = [], [], []
        last_pt_ts = None
        if effective_end > start_utc:
            try:
                bound = SEL_10M_DAY.bind([r.id, start_utc, effective_end])
                bound.fetch_size = 160  # ≤144 pts/day
                t_read = time.time()
                pts = list(s.execute(bound, timeout=REQUEST_TIMEOUT))
                dt_read = time.time() - t_read
                if VERBOSE_MODE:
                    print(f"[{now_str()}]    10m {start_utc}→{effective_end} rows={len(pts)} in {dt_read:.2f}s")
                for p in pts:
                    last_pt_ts = p.ts or last_pt_ts
                    if p.price_usd   is not None: prices.append(float(p.price_usd))
                    if p.market_cap  is not None: mcaps.append(float(p.market_cap))
                    if p.volume_24h  is not None: vols.append(float(p.volume_24h))
            except (OperationTimedOut, ReadTimeout, ReadFailure, DriverException) as e:
                errors += 1
                print(f"[{now_str()}] [READ-ERR] {r.symbol} today 10m: {e} (fallback)")

        # Compute daily candle
        have_enough_to_finalize = (not is_partial_day) and (len(prices) >= MIN_10M_POINTS_FOR_FINAL)
        if prices and (is_partial_day or have_enough_to_finalize):
            # 10m-derived
            o = prices[0]
            h = max(prices)
            l = min(prices)
            c = prices[-1]

            # prefer last non-null mcap/vol from 10m
            mcap = mcaps[-1] if mcaps else None
            vol  = vols[-1]  if vols  else None

            # as fallback for mcap/vol, reuse existing daily row if present
            try:
                existing = s.execute(SEL_PREV_DAY, [r.id, date_utc], timeout=REQUEST_TIMEOUT).one()
            except (OperationTimedOut, ReadTimeout, DriverException):
                existing = None

            if mcap is None and existing:
                mcap = sanitize_num(getattr(existing, "market_cap", None), sanitize_num(r.market_cap, 0.0))
            if vol  is None and existing:
                vol  = sanitize_num(getattr(existing, "volume_24h", None), sanitize_num(r.volume_24h, 0.0))
            if mcap is None: mcap = sanitize_num(r.market_cap, 0.0)
            if vol  is None:  vol  = sanitize_num(r.volume_24h, 0.0)

            price_usd = c
            # last_updated → use last 10m point time if available, else live last_updated
            last_upd = last_pt_ts or r.last_updated
            if last_upd and last_upd.tzinfo is None:
                last_upd = last_upd.replace(tzinfo=timezone.utc)

            source = "10m_partial" if is_partial_day else "10m_final"

        else:
            # Very early UTC day (no safe 10m yet): prev-close vs live snapshot
            prev_row = None
            try:
                prev_row = s.execute(SEL_PREV_DAY, [r.id, (date_utc - timedelta(days=1))],
                                     timeout=REQUEST_TIMEOUT).one()
            except (OperationTimedOut, ReadTimeout, DriverException):
                pass

            c = sanitize_num(r.price_usd, 0.0)
            if prev_row and (prev_row.close is not None or prev_row.price_usd is not None):
                prev_close = float(prev_row.close if prev_row.close is not None else prev_row.price_usd)
                o, h, l = prev_close, max(prev_close, c), min(prev_close, c)
                source = "daily_prevclose"
            else:
                o = h = l = c
                source = "daily_flat"

            price_usd = c
            mcap = sanitize_num(r.market_cap, 0.0)
            vol  = sanitize_num(r.volume_24h, 0.0)
            last_upd = r.last_updated
            if last_upd and last_upd.tzinfo is None:
                last_upd = last_upd.replace(tzinfo=timezone.utc)

        # Compare with existing (skip unchanged to reduce writes/tombstones)
        try:
            existing_today = s.execute(SEL_PREV_DAY, [r.id, date_utc], timeout=REQUEST_TIMEOUT).one()
        except (OperationTimedOut, ReadTimeout, DriverException):
            existing_today = None

        if existing_today:
            same = all([
                equalish(getattr(existing_today, "open", None),  o),
                equalish(getattr(existing_today, "high", None),  h),
                equalish(getattr(existing_today, "low", None),   l),
                equalish(getattr(existing_today, "close", None), price_usd),
                equalish(getattr(existing_today, "market_cap", None), mcap),
                equalish(getattr(existing_today, "volume_24h", None), vol),
                # If both are 10m_final, also skip if source same (don’t refinalize identical)
                (getattr(existing_today, "candle_source", None) == source)
                if source == "10m_final" else True
            ])
            if same:
                unchanged_skips += 1
                if VERBOSE_MODE:
                    print(f"[{now_str()}]    {r.symbol} {date_utc} unchanged ({source}) → skip")
                continue

        # Upsert
        try:
            s.execute(UPD_DAILY, (
                r.symbol, r.name, r.rank,
                price_usd, mcap, vol, last_upd,
                o, h, l, price_usd, source,
                r.id, date_utc
            ), timeout=REQUEST_TIMEOUT)
            if VERBOSE_MODE:
                print(f"[{now_str()}]    UPSERT {r.symbol} {date_utc} "
                      f"O={o} H={h} L={l} C={price_usd} M={mcap} V={vol} src={source}")
        except (WriteTimeout, OperationTimedOut, DriverException) as e:
            errors += 1
            print(f"[{now_str()}] [WRITE-ERR] {r.symbol} {date_utc}: {e}")

    print(f"[{now_str()}] [daily-from-10m] date={date_utc} "
          f"errors={errors} unchanged_skips={unchanged_skips}")

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
