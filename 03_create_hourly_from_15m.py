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

# Live behavior
SLOT_DELAY_SEC      = int(os.getenv("SLOT_DELAY_SEC", "120"))  # guard against too-fresh 15m
FINALIZE_PREV       = os.getenv("FINALIZE_PREV", "1") == "1"   # finalize previous hour once
CURRENT_ONLY        = os.getenv("CURRENT_ONLY", "1") == "1"    # only current + prev (no older backfill)
PROGRESS_EVERY      = int(os.getenv("PROGRESS_EVERY", "20"))
VERBOSE_MODE        = os.getenv("VERBOSE_MODE", "0") == "1"

HOURLY_TABLE        = os.getenv("HOURLY_TABLE", "candles_hourly_30d")

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

# Live coins
SEL_LIVE = SimpleStatement(
    "SELECT id, symbol, name, rank FROM prices_live",
    fetch_size=FETCH_SIZE
)

def plan_windows():
    """
    Returns a list of (start, end, is_final, label) to process:
      - Previous hour (final) once, if enabled and not already final
      - Current hour (partial) up to last safe 15m boundary
    """
    now_guarded = datetime.now(timezone.utc) - timedelta(seconds=SLOT_DELAY_SEC)
    curr_start = now_guarded.replace(minute=0, second=0, microsecond=0)
    curr_end   = curr_start + timedelta(hours=1)
    last_safe  = floor_15m(now_guarded)
    curr_eff_end = min(curr_end, last_safe)

    prev_start = curr_start - timedelta(hours=1)
    prev_end   = curr_start

    windows = []
    # previous hour (closed → final)
    if FINALIZE_PREV:
        windows.append((prev_start, prev_end, True, "prev_final"))

    # current hour (in progress → partial)
    if curr_eff_end > curr_start:
        windows.append((curr_start, curr_eff_end, False, "curr_partial"))
    else:
        # No safe 15m yet for current hour; skip it quietly
        pass

    return windows

def main():
    # Prepare statements
    print(f"[{now_str()}] Preparing statements…")
    t0 = time.time()
    SEL_15M_RANGE = s.prepare("""
      SELECT ts, price_usd, market_cap, volume_24h
      FROM prices_15m_7d
      WHERE id=? AND ts>=? AND ts<?
    """)
    INS_UPSERT = s.prepare(f"""
      INSERT INTO {HOURLY_TABLE}
        (id, ts, symbol, name, open, high, low, close, market_cap, volume_24h, source)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    # Used to skip re-finalizing an already-final previous hour
    SEL_HOURLY_ONE = s.prepare(f"""
      SELECT source FROM {HOURLY_TABLE} WHERE id=? AND ts=? LIMIT 1
    """)
    print(f"[{now_str()}] Prepared in {time.time()-t0:.2f}s")

    # Plan windows
    windows = plan_windows()
    if not windows:
        print(f"[{now_str()}] Nothing to do (no safe 15m yet).")
        return
    w_desc = ", ".join([f"[{lbl}:{'final' if fin else 'partial'} {st}→{en}]" for st,en,fin,lbl in windows])
    print(f"[{now_str()}] Windows: {w_desc}")

    # Load coins
    print(f"[{now_str()}] Loading top coins…")
    t0 = time.time()
    coins = list(s.execute(SEL_LIVE, timeout=REQUEST_TIMEOUT))
    print(f"[{now_str()}] Loaded {len(coins)} live rows in {time.time()-t0:.2f}s")

    coins = [c for c in coins if isinstance(c.rank, int) and c.rank > 0]
    coins.sort(key=lambda c: c.rank)
    coins = coins[:TOP_N]
    print(f"[{now_str()}] Processing top {len(coins)} coins (TOP_N={TOP_N})")

    wrote = skipped = empty = errors = finalized_skips = 0

    for ci, c in enumerate(coins, 1):
        if ci == 1 or ci % PROGRESS_EVERY == 0 or ci == len(coins):
            print(f"[{now_str()}] → Coin {ci}/{len(coins)}: {getattr(c,'symbol','?')} ({getattr(c,'id','?')}) r={getattr(c,'rank','?')}")

        for (start, end, is_final, label) in windows:
            # Skip re-finalizing: if row exists with source='15m_final', skip
            if is_final:
                try:
                    row = s.execute(SEL_HOURLY_ONE, [c.id, start], timeout=REQUEST_TIMEOUT).one()
                    if row and (getattr(row, "source", None) == "15m_final"):
                        finalized_skips += 1
                        if VERBOSE_MODE:
                            print(f"[{now_str()}]    {c.symbol} {start} already final → skip")
                        continue
                except (OperationTimedOut, ReadTimeout, DriverException) as e:
                    errors += 1
                    print(f"[{now_str()}] [CHK-ERR] {c.symbol} {start}: {e} (continuing)")

            # Fetch 15m points
            try:
                bound = SEL_15M_RANGE.bind([c.id, start, end])
                bound.fetch_size = 64
                t_read = time.time()
                pts = list(s.execute(bound, timeout=REQUEST_TIMEOUT))
                if VERBOSE_MODE:
                    print(f"[{now_str()}]    {c.symbol} {start}→{end} got {len(pts)} pts in {time.time()-t_read:.2f}s")
            except (OperationTimedOut, ReadTimeout, ReadFailure, DriverException) as e:
                errors += 1
                print(f"[{now_str()}] [READ-ERR] {c.symbol} {start}→{end}: {e}")
                skipped += 1
                continue

            prices, mcaps, vols = [], [], []
            for p in pts:
                if p.price_usd is not None: prices.append(float(p.price_usd))
                if p.market_cap is not None: mcaps.append(float(p.market_cap))
                if p.volume_24h is not None: vols.append(float(p.volume_24h))

            if not prices:
                empty += 1
                if VERBOSE_MODE:
                    print(f"[{now_str()}]    {c.symbol} {start} empty (no 15m)")
                continue

            o  = prices[0]
            h  = max(prices)
            l  = min(prices)
            cl = prices[-1]
            mcap = mcaps[-1] if mcaps else None
            vol  = vols[-1]  if vols else None
            source = "15m_final" if is_final else "15m_partial"

            try:
                s.execute(INS_UPSERT, [c.id, start, c.symbol, c.name, o, h, l, cl, mcap, vol, source],
                          timeout=REQUEST_TIMEOUT)
                if VERBOSE_MODE:
                    print(f"[{now_str()}]    UPSERT {c.symbol} {start} ({source})")
                wrote += 1
            except (WriteTimeout, OperationTimedOut, DriverException) as e:
                errors += 1
                print(f"[{now_str()}] [WRITE-ERR] {c.symbol} {start}: {e}")
                skipped += 1

    print(f"[{now_str()}] [hourly-live] wrote={wrote} skipped={skipped} empty={empty} "
          f"errors={errors} prev_final_skips={finalized_skips}")

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
