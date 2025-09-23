#!/usr/bin/env python3
# prices/03_create_hourly_from_10m.py
import os, time
from datetime import datetime, timedelta, timezone
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
SLOT_DELAY_SEC      = int(os.getenv("SLOT_DELAY_SEC", "120"))     # guard against too-fresh 10m
FINALIZE_PREV       = os.getenv("FINALIZE_PREV", "1") == "1"      # finalize previous hour once
PROGRESS_EVERY      = int(os.getenv("PROGRESS_EVERY", "20"))
VERBOSE_MODE        = os.getenv("VERBOSE_MODE", "0") == "1"
MIN_10M_POINTS_FOR_FINAL = int(os.getenv("MIN_10M_POINTS_FOR_FINAL", "2"))  # require ≥2 points to finalize

# Table names (new Gecko pipeline)
TEN_MIN_TABLE       = os.getenv("TEN_MIN_TABLE", "gecko_prices_10m_7d")
HOURLY_TABLE        = os.getenv("HOURLY_TABLE", "gecko_candles_hourly_30d")
TABLE_LIVE          = os.getenv("TABLE_LIVE", "gecko_prices_live")           # for coin list
TABLE_MCAP_HOURLY   = os.getenv("TABLE_MCAP_HOURLY", "gecko_market_cap_hourly_30d")

if not ASTRA_TOKEN:
    raise SystemExit("Missing ASTRA_TOKEN")

def now_str() -> str:
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

# Live coins (use CoinGecko rank field)
SEL_LIVE = SimpleStatement(
    f"SELECT id, symbol, name, market_cap_rank, category FROM {TABLE_LIVE}",
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
    SEL_HOURLY_ONE = s.prepare(f"""
      SELECT symbol, name, open, high, low, close, price_usd, market_cap, volume_24h,
             market_cap_rank, circulating_supply, total_supply, candle_source
      FROM {HOURLY_TABLE} WHERE id=? AND ts=? LIMIT 1
    """)
    # Cassandra INSERT is an upsert; use intentionally.
    INS_UPSERT = s.prepare(f"""
      INSERT INTO {HOURLY_TABLE}
        (id, ts, symbol, name, open, high, low, close, price_usd,
         market_cap, volume_24h, market_cap_rank, circulating_supply, total_supply,
         candle_source, last_updated)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    INS_MCAP_HOURLY = s.prepare(f"""
      INSERT INTO {TABLE_MCAP_HOURLY} (category, ts, id, name, symbol, market_cap, market_cap_rank, volume_24h, last_updated)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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

    coins = [c for c in coins if isinstance(c.market_cap_rank, int) and c.market_cap_rank > 0]
    coins.sort(key=lambda c: c.market_cap_rank)
    coins = coins[:TOP_N]
    print(f"[{now_str()}] Processing top {len(coins)} coins (TOP_N={TOP_N})")

    hour_totals = {}

    def bump_hour_total(slot_start, category, mcap_value, vol_value, last_upd) -> None:
        key = (slot_start, category)
        entry = hour_totals.setdefault(key, {"market_cap": 0.0, "volume_24h": 0.0, "last_updated": last_upd})
        entry["market_cap"] += mcap_value
        entry["volume_24h"] += vol_value
        if last_upd and (entry["last_updated"] is None or last_upd > entry["last_updated"]):
            entry["last_updated"] = last_upd

    wrote = skipped = empty = errors = finalized_skips = unchanged_skips = 0

    for ci, c in enumerate(coins, 1):
        if ci == 1 or ci % PROGRESS_EVERY == 0 or ci == len(coins):
            print(f"[{now_str()}] → Coin {ci}/{len(coins)}: {getattr(c,'symbol','?')} ({getattr(c,'id','?')}) r={getattr(c,'market_cap_rank','?')}")

        coin_category = (getattr(c, 'category', None) or 'Other').strip() or 'Other'

        for (start, end, is_final, label) in windows:
            # Fetch 10m points within the hour window
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

            # Build aggregates from 10m
            prices, mcaps, vols = [], [], []
            ranks, circs, tots = [], [], []
            last_upd_candidates = []

            for p in pts:
                if p.price_usd   is not None: prices.append(float(p.price_usd))
                if p.market_cap  is not None: mcaps.append(float(p.market_cap))
                if p.volume_24h  is not None: vols.append(float(p.volume_24h))
                if getattr(p, "market_cap_rank", None) is not None: ranks.append(int(p.market_cap_rank))
                if getattr(p, "circulating_supply", None) is not None: circs.append(float(p.circulating_supply))
                if getattr(p, "total_supply", None) is not None: tots.append(float(p.total_supply))
                if getattr(p, "last_updated", None) is not None: last_upd_candidates.append(p.last_updated)

            # For a closed hour, require at least 2 10m points; else leave it as partial next run
            if is_final and len(prices) < MIN_10M_POINTS_FOR_FINAL:
                if VERBOSE_MODE:
                    print(f"[{now_str()}]    {c.symbol} {start} final: not enough 10m points ({len(prices)}) → skip finalization")
                skipped += 1
                continue

            if not prices:
                # Try to carry price (and extras) from the most recent 10m before start (only for partial hour)
                if not is_final:
                    try:
                        prev = s.execute(SEL_10M_PREV, [c.id, start], timeout=REQUEST_TIMEOUT).one()
                    except (OperationTimedOut, ReadTimeout, ReadFailure, DriverException) as e:
                        prev = None
                        if VERBOSE_MODE:
                            print(f"[{now_str()}] [PREV-ERR] {c.symbol} < {start}: {e}")

                    if prev and prev.price_usd is not None:
                        prices = [float(prev.price_usd)]
                        if prev.market_cap  is not None: mcaps.append(float(prev.market_cap))
                        if prev.volume_24h  is not None: vols.append(float(prev.volume_24h))
                        if getattr(prev, "market_cap_rank", None) is not None: ranks.append(int(prev.market_cap_rank))
                        if getattr(prev, "circulating_supply", None) is not None: circs.append(float(prev.circulating_supply))
                        if getattr(prev, "total_supply", None) is not None: tots.append(float(prev.total_supply))
                        if getattr(prev, "last_updated", None) is not None: last_upd_candidates.append(prev.last_updated)
                    else:
                        empty += 1
                        if VERBOSE_MODE:
                            print(f"[{now_str()}]    {c.symbol} {start} empty (no 10m & no prev) → skip")
                        continue
                else:
                    empty += 1
                    if VERBOSE_MODE:
                        print(f"[{now_str()}]    {c.symbol} {start} empty (final hour) → skip")
                    continue

            # OHLC from 10m within hour
            o  = prices[0]
            h  = max(prices)
            l  = min(prices)
            cl = prices[-1]

            # Last known within hour (or carried prev) for ancillary fields
            mcap = mcaps[-1] if mcaps else None
            vol  = vols[-1]  if vols  else None
            rank = ranks[-1] if ranks else None
            circ = circs[-1] if circs else None
            tot  = tots[-1]  if tots  else None

            # Fallback to existing row for mcap/vol/rank/supplies (avoid nulling established values)
            try:
                existing = s.execute(SEL_HOURLY_ONE, [c.id, start], timeout=REQUEST_TIMEOUT).one()
            except (OperationTimedOut, ReadTimeout, DriverException) as e:
                existing = None
                if VERBOSE_MODE:
                    print(f"[{now_str()}] [EXIST-ERR] {c.symbol} {start}: {e} (treat as no row)")

            if existing:
                if mcap is None: mcap = sanitize_num(getattr(existing, "market_cap", None))
                if vol  is None: vol  = sanitize_num(getattr(existing, "volume_24h", None))
                if rank is None: rank = getattr(existing, "market_cap_rank", None)
                if circ is None: circ = sanitize_num(getattr(existing, "circulating_supply", None))
                if tot  is None: tot  = sanitize_num(getattr(existing, "total_supply", None))

            # Defaults if still None
            if mcap is None: mcap = 0.0
            if vol  is None: vol  = 0.0

            # Determine candle_source label and last_updated
            candle_source = "10m_final" if is_final else "10m_partial"
            if last_upd_candidates:
                last_upd = max(last_upd_candidates)
            else:
                # if we carried prev or had no last_updated in 10m rows, clamp to hour end - 1s
                last_upd = (end - timedelta(seconds=1))

            # Skip if already final & identical
            if is_final and existing and getattr(existing, "candle_source", None) == "10m_final":
                same = all([
                    equalish(o, existing.open),
                    equalish(h, existing.high),
                    equalish(l, existing.low),
                    equalish(cl, existing.close),
                    equalish(cl, existing.price_usd),
                    equalish(mcap, existing.market_cap),
                    equalish(vol,  existing.volume_24h),
                    (rank == getattr(existing, "market_cap_rank", None)),
                    equalish(circ, getattr(existing, "circulating_supply", None)),
                    equalish(tot,  getattr(existing, "total_supply", None)),
                ])
                if same:
                    finalized_skips += 1
                    if VERBOSE_MODE:
                        print(f"[{now_str()}]    {c.symbol} {start} already final & identical → skip")
                    continue

            # For partials, skip writing when unchanged vs existing partial (reduces churn)
            if (not is_final) and existing and getattr(existing, "candle_source", None) == "10m_partial":
                same = all([
                    equalish(o, existing.open),
                    equalish(h, existing.high),
                    equalish(l, existing.low),
                    equalish(cl, existing.close),
                    equalish(cl, existing.price_usd),
                    equalish(mcap, existing.market_cap),
                    equalish(vol,  existing.volume_24h),
                    (rank == getattr(existing, "market_cap_rank", None)),
                    equalish(circ, getattr(existing, "circulating_supply", None)),
                    equalish(tot,  getattr(existing, "total_supply", None)),
                ])
                if same:
                    unchanged_skips += 1
                    if VERBOSE_MODE:
                        print(f"[{now_str()}]    {c.symbol} {start} partial unchanged → skip")
                    continue

            # UPSERT the hourly row
            try:
                s.execute(
                    INS_UPSERT,
                    [c.id, start, c.symbol, c.name,
                     o, h, l, cl, cl,
                     mcap, vol, rank, circ, tot,
                     candle_source, last_upd],
                    timeout=REQUEST_TIMEOUT
                )
                bump_hour_total(start, coin_category, mcap, vol, last_upd)
                bump_hour_total(start, 'ALL', mcap, vol, last_upd)
                if VERBOSE_MODE:
                    print(f"[{now_str()}]    UPSERT {c.symbol} {start} ({candle_source}) "
                          f"O={o} H={h} L={l} C={cl} M={mcap} V={vol} R={rank} CS={circ} TS={tot}")
                wrote += 1
            except (WriteTimeout, OperationTimedOut, DriverException) as e:
                errors += 1
                print(f"[{now_str()}] [WRITE-ERR] {c.symbol} {start}: {e}")
                skipped += 1

    if hour_totals:
        print(f"[{now_str()}] [mcap-hourly] writing {len(hour_totals)} aggregates into {TABLE_MCAP_HOURLY}")
        agg_written = 0
        for (slot_start, category), totals in sorted(hour_totals.items(), key=lambda kv: (kv[0][0], 0 if kv[0][1] == 'ALL' else 1, kv[0][1].lower())):
            last_upd = totals.get('last_updated') or (slot_start + timedelta(hours=1) - timedelta(seconds=1))
            cat_id = f"CATEGORY::{category}"
            display_name = 'All Categories' if category == 'ALL' else category
            symbol_val = 'ALL' if category == 'ALL' else category.upper().replace(' ', '_')
            rank_value = 0 if category == 'ALL' else None
            try:
                s.execute(
                    INS_MCAP_HOURLY,
                    [category, slot_start, cat_id, display_name, symbol_val, totals['market_cap'], rank_value, totals['volume_24h'], last_upd],
                    timeout=REQUEST_TIMEOUT
                )
                agg_written += 1
            except (WriteTimeout, OperationTimedOut, DriverException) as e:
                print(f"[{now_str()}] [mcap-hourly] failed for category='{category}' slot={slot_start}: {e}")
        print(f"[{now_str()}] [mcap-hourly] rows_written={agg_written}")
    else:
        print(f"[{now_str()}] [mcap-hourly] no aggregates captured (coins={len(coins)})")

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
