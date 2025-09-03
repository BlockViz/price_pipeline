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

# Behavior
SLOT_DELAY_SEC      = int(os.getenv("SLOT_DELAY_SEC", "120"))  # guard against too-fresh 10m
FINALIZE_PREV       = os.getenv("FINALIZE_PREV", "1") == "1"   # finalize previous hour once
CURRENT_ONLY        = os.getenv("CURRENT_ONLY", "1") == "1"    # only current + prev (no older backfill)
PROGRESS_EVERY      = int(os.getenv("PROGRESS_EVERY", "20"))
VERBOSE_MODE        = os.getenv("VERBOSE_MODE", "0") == "1"

TEN_MIN_TABLE       = os.getenv("TEN_MIN_TABLE", "prices_10m_7d")
HOURLY_TABLE        = os.getenv("HOURLY_TABLE", "candles_hourly_30d")
MIN_10M_POINTS_FOR_FINAL = int(os.getenv("MIN_10M_POINTS_FOR_FINAL", "2"))  # require ≥2 points to finalize

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
    """Loose equality for floats; treats None==None as True."""
    if a is None and b is None: return True
    if a is None or b is None:  return False
    try:
        return abs(float(a) - float(b)) <= 1e-12
    except Exception:
        return False

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
      - Previous hour (closed) as final, if enabled
      - Current hour (partial) up to last safe 10m boundary
    """
    now_guarded = datetime.now(timezone.utc) - timedelta(seconds=SLOT_DELAY_SEC)
    curr_start = now_guarded.replace(minute=0, second=0, microsecond=0)
    curr_end   = curr_start + timedelta(hours=1)
    last_safe  = floor_10m(now_guarded)
    curr_eff_end = min(curr_end, last_safe)

    prev_start = curr_start - timedelta(hours=1)
    prev_end   = curr_start

    windows = []
    if FINALIZE_PREV:
        windows.append((prev_start, prev_end, True,  "prev_final"))
    if curr_eff_end > curr_start:
        windows.append((curr_start, curr_eff_end, False, "curr_partial"))

    return windows

def main():
    # Prepare statements
    print(f"[{now_str()}] Preparing statements…")
    t0 = time.time()
    SEL_10M_RANGE = s.prepare(f"""
      SELECT ts, price_usd, market_cap, volume_24h
      FROM {TEN_MIN_TABLE}
      WHERE id=? AND ts>=? AND ts<? ORDER BY ts ASC
    """)
    SEL_HOURLY_ONE = s.prepare(f"""
      SELECT symbol, name, open, high, low, close, market_cap, volume_24h, source
      FROM {HOURLY_TABLE} WHERE id=? AND ts=? LIMIT 1
    """)
    # NOTE: INSERT is an upsert in Cassandra; we use it intentionally.
    INS_UPSERT = s.prepare(f"""
      INSERT INTO {HOURLY_TABLE}
        (id, ts, symbol, name, open, high, low, close, market_cap, volume_24h, source)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    print(f"[{now_str()}] Prepared in {time.time()-t0:.2f}s")

    # Plan windows
    windows = plan_windows()
    if not windows:
        print(f"[{now_str()}] Nothing to do (no safe 10m yet).")
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

    wrote = skipped = empty = errors = finalized_skips = unchanged_skips = 0

    for ci, c in enumerate(coins, 1):
        if ci == 1 or ci % PROGRESS_EVERY == 0 or ci == len(coins):
            print(f"[{now_str()}] → Coin {ci}/{len(coins)}: {getattr(c,'symbol','?')} ({getattr(c,'id','?')}) r={getattr(c,'rank','?')}")

        for (start, end, is_final, label) in windows:
            # Fetch 10m points
            try:
                bound = SEL_10M_RANGE.bind([c.id, start, end]); bound.fetch_size = 64
                t_read = time.time()
                pts = list(s.execute(bound, timeout=REQUEST_TIMEOUT))
                if VERBOSE_MODE:
                    print(f"[{now_str()}]    {c.symbol} {start}→{end} got {len(pts)} pts in {time.time()-t_read:.2f}s")
            except (OperationTimedOut, ReadTimeout, ReadFailure, DriverException) as e:
                errors += 1
                print(f"[{now_str()}] [READ-ERR] {c.symbol} {start}→{end}: {e}")
                skipped += 1
                continue

            # Build OHLC from 10m
            prices, mcaps, vols = [], [], []
            for p in pts:
                if p.price_usd   is not None: prices.append(float(p.price_usd))
                if p.market_cap  is not None: mcaps.append(float(p.market_cap))
                if p.volume_24h  is not None: vols.append(float(p.volume_24h))

            if is_final and len(prices) < MIN_10M_POINTS_FOR_FINAL:
                # don't finalize a closed hour if we somehow have <2 points; leave last partial until data is complete
                if VERBOSE_MODE:
                    print(f"[{now_str()}]    {c.symbol} {start} final: not enough 10m points ({len(prices)}) → skip finalization")
                skipped += 1
                continue

            if not prices:
                empty += 1
                if VERBOSE_MODE:
                    print(f"[{now_str()}]    {c.symbol} {start} empty (no 10m) → skip")
                continue

            o  = prices[0]
            h  = max(prices)
            l  = min(prices)
            cl = prices[-1]

            # Use last available in hour; fallback to existing row; final fallback 0.0
            mcap = mcaps[-1] if mcaps else None
            vol  = vols[-1]  if vols  else None

            # Look at existing hourly row for comparison / fallback and finalization skip
            try:
                existing = s.execute(SEL_HOURLY_ONE, [c.id, start], timeout=REQUEST_TIMEOUT).one()
            except (OperationTimedOut, ReadTimeout, DriverException) as e:
                existing = None
                if VERBOSE_MODE:
                    print(f"[{now_str()}] [EXIST-ERR] {c.symbol} {start}: {e} (treat as no row)")

            if (mcap is None or vol is None) and existing:
                if mcap is None: mcap = sanitize_num(getattr(existing, "market_cap", None), 0.0)
                if vol  is None: vol  = sanitize_num(getattr(existing, "volume_24h", None), 0.0)
            if mcap is None: mcap = 0.0
            if vol  is None:  vol  = 0.0

            # Skip if already final and identical
            if is_final and existing and getattr(existing, "source", None) == "10m_final":
                same = all([
                    equalish(o, existing.open),
                    equalish(h, existing.high),
                    equalish(l, existing.low),
                    equalish(cl, existing.close),
                    equalish(mcap, existing.market_cap),
                    equalish(vol, existing.volume_24h),
                ])
                if same:
                    finalized_skips += 1
                    if VERBOSE_MODE:
                        print(f"[{now_str()}]    {c.symbol} {start} already final & identical → skip")
                    continue

            # If not final, also skip writing when a partial already matches (reduces churn)
            if (not is_final) and existing and getattr(existing, "source", None) == "10m_partial":
                same = all([
                    equalish(o, existing.open),
                    equalish(h, existing.high),
                    equalish(l, existing.low),
                    equalish(cl, existing.close),
                    equalish(mcap, existing.market_cap),
                    equalish(vol, existing.volume_24h),
                ])
                if same:
                    unchanged_skips += 1
                    if VERBOSE_MODE:
                        print(f"[{now_str()}]    {c.symbol} {start} partial unchanged → skip")
                    continue

            source = "10m_final" if is_final else "10m_partial"

            try:
                s.execute(
                    INS_UPSERT,
                    [c.id, start, c.symbol, c.name, o, h, l, cl, mcap, vol, source],
                    timeout=REQUEST_TIMEOUT
                )
                if VERBOSE_MODE:
                    print(f"[{now_str()}]    UPSERT {c.symbol} {start} ({source}) O={o} H={h} L={l} C={cl} M={mcap} V={vol}")
                wrote += 1
            except (WriteTimeout, OperationTimedOut, DriverException) as e:
                errors += 1
                print(f"[{now_str()}] [WRITE-ERR] {c.symbol} {start}: {e}")
                skipped += 1

    print(f"[{now_str()}] [hourly-from-10m] wrote={wrote} skipped={skipped} empty={empty} "
          f"errors={errors} already_final_identical={finalized_skips} partial_unchanged_skips={unchanged_skips}")

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
