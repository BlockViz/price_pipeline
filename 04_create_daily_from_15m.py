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

TOP_N               = int(os.getenv("TOP_N", "110"))
REQUEST_TIMEOUT     = int(os.getenv("REQUEST_TIMEOUT_SEC", "30"))
CONNECT_TIMEOUT_SEC = int(os.getenv("CONNECT_TIMEOUT_SEC", "15"))
FETCH_SIZE          = int(os.getenv("FETCH_SIZE", "500"))

SLOT_DELAY_SEC      = int(os.getenv("SLOT_DELAY_SEC", "120"))   # guard against too-fresh 15m slot
PROGRESS_EVERY      = int(os.getenv("PROGRESS_EVERY", "20"))    # coin progress interval
VERBOSE_MODE        = os.getenv("VERBOSE_MODE", "0") == "1"     # extra per-coin/hour logs

# renamed target dataset
DAILY_TABLE         = os.getenv("DAILY_TABLE", "candles_daily_contin")

if not ASTRA_TOKEN:
    raise SystemExit("Missing ASTRA_TOKEN")

def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def floor_15m(dt_utc: datetime) -> datetime:
    return dt_utc.replace(minute=(dt_utc.minute // 15) * 15, second=0, microsecond=0)

print(f"[{now_str()}] Connecting to Astra (bundle='{BUNDLE}', keyspace='{KEYSPACE}')")
auth = PlainTextAuthProvider("token", ASTRA_TOKEN)

exec_profile = ExecutionProfile(
    load_balancing_policy=RoundRobinPolicy(),
    request_timeout=REQUEST_TIMEOUT,
)
cluster = Cluster(
    cloud={"secure_connect_bundle": BUNDLE},
    auth_provider=auth,
    execution_profiles={EXEC_PROFILE_DEFAULT: exec_profile},
    connect_timeout=CONNECT_TIMEOUT_SEC,
)
s = cluster.connect(KEYSPACE)
print(f"[{now_str()}] Connected.")

# Statements built here; prepares inside main so we can time & log
SEL_LIVE = SimpleStatement("""
  SELECT id, symbol, name, rank, price_usd, market_cap, volume_24h, last_updated
  FROM prices_live
""", fetch_size=FETCH_SIZE)

def utc_today_bounds():
    now_utc = datetime.now(timezone.utc)
    day = now_utc.date()
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)        # 00:00:00
    end_excl = start + timedelta(days=1)                                       # tomorrow 00:00
    return start, end_excl, day

def main():
    # Prepare statements with timing and target table name
    print(f"[{now_str()}] Preparing statements…")
    t0 = time.time()
    SEL_15M_DAY = s.prepare("""
      SELECT ts, price_usd, market_cap, volume_24h
      FROM prices_15m_7d
      WHERE id=? AND ts>=? AND ts<?
    """)
    # prev day for fallback now reads from the *renamed* table
    SEL_PREV_DAY = s.prepare(f"""
      SELECT close, price_usd FROM {DAILY_TABLE}
      WHERE id=? AND date=? LIMIT 1
    """)
    # upsert into renamed table (UPDATE is an upsert in Cassandra)
    UPD_DAILY = s.prepare(f"""
      UPDATE {DAILY_TABLE} SET
        symbol = ?, name = ?, rank = ?,
        price_usd = ?, market_cap = ?, volume_24h = ?, last_updated = ?,
        open = ?, high = ?, low = ?, close = ?, candle_source = ?
      WHERE id = ? AND date = ?
    """)
    print(f"[{now_str()}] Prepared in {time.time()-t0:.2f}s")

    # Boundaries for today and last safe 15m slot
    start_utc, end_excl_utc, date_utc = utc_today_bounds()
    last_safe_15m_end = floor_15m(datetime.now(timezone.utc) - timedelta(seconds=SLOT_DELAY_SEC))
    effective_end = min(end_excl_utc, last_safe_15m_end)
    is_partial_day = effective_end < end_excl_utc

    print(f"[{now_str()}] Today UTC: date={date_utc} start={start_utc} end_excl={end_excl_utc} "
          f"safe_15m_end={last_safe_15m_end} effective_end={effective_end} "
          f"{'(partial)' if is_partial_day else '(final)'}")

    # Load live coins
    print(f"[{now_str()}] Loading top coins (timeout={REQUEST_TIMEOUT}s)…")
    t0 = time.time()
    coins = list(s.execute(SEL_LIVE, timeout=REQUEST_TIMEOUT))
    print(f"[{now_str()}] Loaded {len(coins)} rows from prices_live in {time.time()-t0:.2f}s")

    coins = [r for r in coins if isinstance(r.rank, int) and r.rank > 0]
    coins.sort(key=lambda r: r.rank)
    coins = coins[:TOP_N]
    print(f"[{now_str()}] Processing top {len(coins)} coins (TOP_N={TOP_N})")

    upserts = 0
    errors  = 0

    for idx, r in enumerate(coins, 1):
        if idx == 1 or idx % PROGRESS_EVERY == 0 or idx == len(coins):
            print(f"[{now_str()}] → Coin {idx}/{len(coins)}: {getattr(r, 'symbol', '?')} "
                  f"({getattr(r, 'id', '?')}) rank={getattr(r, 'rank', '?')}")

        # pull today's 15m points up to effective_end
        prices, mcaps, vols = [], [], []
        if effective_end > start_utc:  # we have at least one safe 15m slot
            try:
                bound = SEL_15M_DAY.bind([r.id, start_utc, effective_end])
                bound.fetch_size = 64  # expect up to 96 points per day; paging is fine
                t_read = time.time()
                pts = list(s.execute(bound, timeout=REQUEST_TIMEOUT))
                dt_read = time.time() - t_read
                if VERBOSE_MODE:
                    print(f"[{now_str()}]    15m {start_utc}→{effective_end} rows={len(pts)} in {dt_read:.2f}s")
                for p in pts:
                    if p.price_usd is not None: prices.append(float(p.price_usd))
                    if p.market_cap is not None: mcaps.append(float(p.market_cap))
                    if p.volume_24h is not None: vols.append(float(p.volume_24h))
            except (OperationTimedOut, ReadTimeout, ReadFailure, DriverException) as e:
                errors += 1
                print(f"[{now_str()}] [READ-ERR] {r.symbol} today 15m: {e} (using fallback)")
                # fall through to fallback path

        # compute candle
        source = None
        if prices:
            # Intraday-derived candle for today
            o = prices[0]; h = max(prices); l = min(prices); c = prices[-1]
            mcap = mcaps[-1] if mcaps else float(r.market_cap or 0.0)
            vol  = vols[-1]  if vols  else float(r.volume_24h or 0.0)
            source = "15m_final" if not is_partial_day else "15m_partial"
        else:
            # No safe 15m yet today: derive from prev close + current live snapshot
            try:
                prev = s.execute(SEL_PREV_DAY, [r.id, (date_utc - timedelta(days=1))],
                                 timeout=REQUEST_TIMEOUT).one()
            except (OperationTimedOut, ReadTimeout, DriverException) as e:
                errors += 1
                print(f"[{now_str()}] [PREV-ERR] {r.symbol} prev day read: {e}")
                prev = None
            c = float(r.price_usd or 0.0)
            if prev and (prev.close is not None or prev.price_usd is not None):
                prev_close = float(prev.close if prev.close is not None else prev.price_usd)
                o, h, l = prev_close, max(prev_close, c), min(prev_close, c)
                source = "daily_prevclose"
            else:
                o = h = l = c
                source = "daily_flat"
            mcap = float(r.market_cap or 0.0)
            vol  = float(r.volume_24h or 0.0)

        # write upsert into renamed table
        price_usd = c
        last_upd = r.last_updated

        try:
            s.execute(UPD_DAILY, (
                r.symbol, r.name, r.rank,
                price_usd, mcap, vol, last_upd,
                o, h, l, c, source,
                r.id, date_utc
            ), timeout=REQUEST_TIMEOUT)
            if VERBOSE_MODE:
                print(f"[{now_str()}]    UPSERT {r.symbol} {date_utc} "
                      f"o={o} h={h} l={l} c={c} src={source}")
            upserts += 1
        except (WriteTimeout, OperationTimedOut, DriverException) as e:
            errors += 1
            print(f"[{now_str()}] [WRITE-ERR] {r.symbol} {date_utc}: {e}")

    print(f"[{now_str()}] [daily] date={date_utc} upserts={upserts} errors={errors} "
          f"source={'partial' if is_partial_day else 'final'}")

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
