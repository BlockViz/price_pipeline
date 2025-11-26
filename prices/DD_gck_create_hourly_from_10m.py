#!/usr/bin/env python3
# prices/DD_gck_create_hourly_from_10m.py
#
# Build hourly OHLC candles from 10m slots (unchanged), AND write
# category + ALL aggregates that are ALWAYS equal to the sum of the
# hourly candles for that hour.
#
# ✅ Robust aggregate fix:
#   - Even if we SKIP writing a coin’s hourly candle (because it’s
#     unchanged/identical or we have no fresh 10m points), we STILL
#     include that coin’s EXISTING hourly candle in the in-memory
#     aggregates for both its category and "ALL".
#   - This guarantees:
#       SUM(HOURLY.market_cap) BY (ts) == "ALL" in MCAP_HOURLY
#       SUM(categories excluding "ALL") == "ALL"
#
# 🔎 Verbose logging:
#   - Set VERBOSE_MODE=1 to get detailed per-coin/per-hour traces.
#   - PROGRESS_EVERY controls periodic progress logs.
#
# ⏰ UTC handling & Astra (Cassandra):
#   - Cassandra "timestamp" is UTC (ms since epoch).
#   - The driver returns naive datetimes that represent UTC.
#   - We normalize all internal times to timezone-aware UTC for math,
#     but when binding to Cassandra we convert to NAIVE UTC (driver-safe).
#
# Uses the shared Astra connector (astra_connect.connect) so it works
# with local .env or plain environment vars in CI.

import os, time, sys
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Dict, Any
from collections import defaultdict

# ───────────────────────── Astra connector ─────────────────────────
from astra_connect.connect import get_session, AstraConfig
AstraConfig.from_env()  # load .env if present, otherwise use process env

# ───────────────────────── Config ─────────────────────────
TOP_N                   = int(os.getenv("TOP_N", "200"))
REQUEST_TIMEOUT         = int(os.getenv("REQUEST_TIMEOUT_SEC", "30"))
FETCH_SIZE              = int(os.getenv("FETCH_SIZE", "500"))

# Behavior
SLOT_DELAY_SEC          = int(os.getenv("SLOT_DELAY_SEC", "120"))   # guard against too-fresh 10m
FINALIZE_PREV           = os.getenv("FINALIZE_PREV", "1") == "1"    # finalize previous hour once
PROGRESS_EVERY          = int(os.getenv("PROGRESS_EVERY", "20"))
VERBOSE_MODE            = os.getenv("VERBOSE_MODE", "1") == "1"
MIN_10M_POINTS_FOR_FINAL= int(os.getenv("MIN_10M_POINTS_FOR_FINAL", "1"))  # allow finalize on a single 10m/carry point
BACKFILL_HOURS          = int(os.getenv("BACKFILL_HOURS", "24"))  # 24h safety window to recover missed runs

# Table names (Gecko pipeline)
TEN_MIN_TABLE           = os.getenv("TEN_MIN_TABLE", "gecko_prices_10m_7d")
HOURLY_TABLE            = os.getenv("HOURLY_TABLE", "gecko_candles_hourly_30d")
TABLE_LIVE              = os.getenv("TABLE_LIVE", "gecko_prices_live")           # for coin list
TABLE_MCAP_HOURLY       = os.getenv("TABLE_MCAP_HOURLY", "gecko_market_cap_hourly_30d")

# ───────────────────────── Time helpers (UTC-safe) ─────────────────────────
UTC = timezone.utc

def now_utc() -> datetime:
    """Timezone-aware now in UTC."""
    return datetime.now(UTC)

def now_str() -> str:
    return now_utc().strftime("%Y-%m-%d %H:%M:%S")

def ensure_aware_utc(dt: datetime | None) -> datetime | None:
    """Make a datetime timezone-aware UTC. Cassandra returns naive UTC."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)  # interpret naive as UTC
    return dt.astimezone(UTC)

def to_cassandra_ts(dt: datetime) -> datetime:
    """
    Cassandra driver accepts naive UTC datetimes for TIMESTAMP columns.
    Convert aware-UTC -> naive-UTC for binding.
    """
    if dt.tzinfo is not None:
        return dt.astimezone(UTC).replace(tzinfo=None)
    # already naive; treat as UTC
    return dt

def floor_10m(dt_utc: datetime) -> datetime:
    """Floor to 10-minute boundary (aware UTC in/out)."""
    dt_utc = ensure_aware_utc(dt_utc)  # make aware UTC
    return dt_utc.replace(minute=(dt_utc.minute // 10) * 10, second=0, microsecond=0)

# ───────────────────────── Misc helpers ─────────────────────────
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

def vprint(msg: str):
    if VERBOSE_MODE:
        print(msg)

# ───────────────────────── Session & prepared statements ─────────────────────────
print(f"[{now_str()}] Connecting to Astra…")
session, cluster = get_session(return_cluster=True)
print(f"[{now_str()}] Connected. keyspace='{session.keyspace}'")

from cassandra.query import SimpleStatement

# Live coins (use CoinGecko rank field)
SEL_LIVE = SimpleStatement(
    f"SELECT id, symbol, name, market_cap_rank, category FROM {TABLE_LIVE}",
    fetch_size=FETCH_SIZE
)

def plan_windows() -> List[Tuple[datetime, datetime, bool, str]]:
    """
    Returns (startUTC, endUTC, is_final, label) windows:
      - BACKFILL_HOURS closed hours before the current hour, as final
      - Current hour as partial up to last safe 10m boundary
    """
    now_guarded = now_utc() - timedelta(seconds=SLOT_DELAY_SEC)
    curr_start  = now_guarded.replace(minute=0, second=0, microsecond=0)
    curr_end    = curr_start + timedelta(hours=1)
    last_safe   = floor_10m(now_guarded)
    curr_eff_end= min(curr_end, last_safe)

    windows: List[Tuple[datetime, datetime, bool, str]] = []

    # Backfill closed hours
    if BACKFILL_HOURS > 0:
        for i in range(BACKFILL_HOURS, 0, -1):
            st = curr_start - timedelta(hours=i)
            en = st + timedelta(hours=1)
            label = f"backfill_h-{i}" if i > 1 else "prev_final"
            windows.append((st, en, True, label))

    # Current partial hour
    if curr_eff_end > curr_start:
        windows.append((curr_start, curr_eff_end, False, "curr_partial"))

    return windows

def main():
    # Prepare statements
    print(f"[{now_str()}] Preparing statements…")
    t0 = time.time()
    SEL_10M_RANGE = session.prepare(f"""
      SELECT ts, price_usd, market_cap, volume_24h,
             market_cap_rank, circulating_supply, total_supply, last_updated
      FROM {TEN_MIN_TABLE}
      WHERE id=? AND ts>=? AND ts<? ORDER BY ts ASC
    """)
    SEL_10M_PREV = session.prepare(f"""
      SELECT ts, price_usd, market_cap, volume_24h,
             market_cap_rank, circulating_supply, total_supply, last_updated
      FROM {TEN_MIN_TABLE}
      WHERE id=? AND ts<? LIMIT 1
    """)
    SEL_HOURLY_ONE = session.prepare(f"""
      SELECT symbol, name, open, high, low, close, price_usd, market_cap, volume_24h,
             market_cap_rank, circulating_supply, total_supply, candle_source, last_updated
      FROM {HOURLY_TABLE} WHERE id=? AND ts=? LIMIT 1
    """)
    # Cassandra INSERT is an upsert; use intentionally.
    INS_UPSERT = session.prepare(f"""
      INSERT INTO {HOURLY_TABLE}
        (id, ts, symbol, name, open, high, low, close, price_usd,
         market_cap, volume_24h, market_cap_rank, circulating_supply, total_supply,
         candle_source, last_updated)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)
    INS_MCAP_HOURLY = session.prepare(f"""
      INSERT INTO {TABLE_MCAP_HOURLY}
        (category, ts, last_updated, market_cap, market_cap_rank, volume_24h)
      VALUES (?, ?, ?, ?, ?, ?)
    """)
    print(f"[{now_str()}] Prepared in {time.time()-t0:.2f}s")

    # Plan windows
    windows = plan_windows()
    if not windows:
        print(f"[{now_str()}] Nothing to do (no safe 10m yet).")
        return
    w_desc = ", ".join([
        f"[{lbl}:{'final' if fin else 'partial'} {st.isoformat()}→{en.isoformat()}]"
        for st, en, fin, lbl in windows
    ])
    print(f"[{now_str()}] Windows: {w_desc}")

    # Load coins
    print(f"[{now_str()}] Loading top coins…")
    t0 = time.time()
    coins = list(session.execute(SEL_LIVE, timeout=REQUEST_TIMEOUT))
    print(f"[{now_str()}] Loaded {len(coins)} live rows in {time.time()-t0:.2f}s")

    coins = [c for c in coins if isinstance(c.market_cap_rank, int) and c.market_cap_rank > 0]
    coins.sort(key=lambda c: c.market_cap_rank)
    coins = coins[:TOP_N]
    print(f"[{now_str()}] Processing top {len(coins)} coins (TOP_N={TOP_N})")

    # Aggregation buckets
    hour_totals: Dict[Tuple[datetime, str], Dict[str, Any]] = {}

    def bump_hour_total(slot_start: datetime, category: str, mcap_value, vol_value, last_upd: datetime) -> None:
        """Sum into (ts, category). slot_start & last_upd are aware UTC."""
        key = (slot_start, category)
        entry = hour_totals.setdefault(
            key,
            {"market_cap": 0.0, "volume_24h": 0.0, "last_updated": last_upd}
        )
        entry["market_cap"] += float(mcap_value or 0.0)
        entry["volume_24h"] += float(vol_value or 0.0)
        if last_upd and (entry["last_updated"] is None or last_upd > entry["last_updated"]):
            entry["last_updated"] = last_upd

    wrote = skipped = empty = errors = finalized_skips = unchanged_skips = 0

    for ci, c in enumerate(coins, 1):
        if ci == 1 or ci % PROGRESS_EVERY == 0 or ci == len(coins):
            print(f"[{now_str()}] → Coin {ci}/{len(coins)}: {getattr(c,'symbol','?')} ({getattr(c,'id','?')}) r={getattr(c,'market_cap_rank','?')}")

        coin_category = (getattr(c, 'category', None) or 'Other').strip() or 'Other'

        for (start, end, is_final, label) in windows:
            # Fetch 10m points within the hour window (bind as naive UTC)
            try:
                bstart = to_cassandra_ts(start)
                bend   = to_cassandra_ts(end)
                bound = SEL_10M_RANGE.bind([c.id, bstart, bend]); bound.fetch_size = 64
                t_read = time.time()
                pts = list(session.execute(bound, timeout=REQUEST_TIMEOUT))
                vprint(f"[{now_str()}]    {c.symbol} {start.isoformat()}→{end.isoformat()} got {len(pts)} 10m pts in {time.time()-t_read:.2f}s")
            except Exception as e:
                errors += 1
                print(f"[{now_str()}] [READ-ERR] {c.symbol} {start.isoformat()}→{end.isoformat()}: {e}")
                skipped += 1
                continue

            # Fetch any existing hourly row up-front so we can include it in aggregates even if we skip writing
            try:
                bound_hour = SEL_HOURLY_ONE.bind([c.id, to_cassandra_ts(start)])
                existing = session.execute(bound_hour, timeout=REQUEST_TIMEOUT).one()
                # Keep a local normalized timestamp instead of mutating the Row (Rows are immutable)
                existing_last_upd = ensure_aware_utc(getattr(existing, "last_updated", None)) if existing else None
            except Exception as e:
                existing = None
                existing_last_upd = None
                vprint(f"[{now_str()}] [EXIST-ERR] {c.symbol} {start.isoformat()}: {e} (treat as no row)")

            # Build aggregates from 10m
            prices, mcaps, vols = [], [], []
            ranks, circs, tots = [], [], []
            last_upd_candidates: List[datetime] = []

            def add_prev_point_if_available() -> bool:
                """Carry the latest 10m point before the window start when inside-slot data is missing."""
                try:
                    prev = session.execute(
                        SEL_10M_PREV.bind([c.id, to_cassandra_ts(start)]),
                        timeout=REQUEST_TIMEOUT
                    ).one()
                except Exception as e:
                    vprint(f"[{now_str()}] [PREV-ERR] {c.symbol} < {start.isoformat()}: {e}")
                    return False

                if prev and prev.price_usd is not None:
                    prices.append(float(prev.price_usd))
                    if prev.market_cap  is not None: mcaps.append(float(prev.market_cap))
                    if prev.volume_24h  is not None: vols.append(float(prev.volume_24h))
                    if getattr(prev, "market_cap_rank", None)    is not None: ranks.append(int(prev.market_cap_rank))
                    if getattr(prev, "circulating_supply", None) is not None: circs.append(float(prev.circulating_supply))
                    if getattr(prev, "total_supply", None)       is not None: tots.append(float(prev.total_supply))
                    lu_prev = ensure_aware_utc(getattr(prev, "last_updated", None))
                    if lu_prev is not None: last_upd_candidates.append(lu_prev)
                    return True
                return False

            for p in pts:
                # Normalize last_updated from 10m rows to aware UTC
                lu = ensure_aware_utc(getattr(p, "last_updated", None))
                if p.price_usd   is not None: prices.append(float(p.price_usd))
                if p.market_cap  is not None: mcaps.append(float(p.market_cap))
                if p.volume_24h  is not None: vols.append(float(p.volume_24h))
                if getattr(p, "market_cap_rank", None)       is not None: ranks.append(int(p.market_cap_rank))
                if getattr(p, "circulating_supply", None)    is not None: circs.append(float(p.circulating_supply))
                if getattr(p, "total_supply", None)          is not None: tots.append(float(p.total_supply))
                if lu is not None: last_upd_candidates.append(lu)

            # For a closed hour, require at least MIN_10M_POINTS_FOR_FINAL points; if empty, try carry; otherwise aggregate existing and skip.
            if is_final and len(prices) < MIN_10M_POINTS_FOR_FINAL:
                if not prices:
                    add_prev_point_if_available()
                if len(prices) < MIN_10M_POINTS_FOR_FINAL:
                    vprint(f"[{now_str()}]    {c.symbol} {start.isoformat()} final: insufficient 10m points ({len(prices)}) → aggregate existing; skip write")
                    if existing:
                        mcap_exist = sanitize_num(getattr(existing, "market_cap", None), 0.0)
                        vol_exist  = sanitize_num(getattr(existing, "volume_24h", None), 0.0)
                        last_upd   = existing_last_upd or (end - timedelta(seconds=1))
                        bump_hour_total(start, coin_category, mcap_exist, vol_exist, last_upd)
                        bump_hour_total(start, 'ALL',        mcap_exist, vol_exist, last_upd)
                    skipped += 1
                    continue

            if not prices:
                # Try to carry price (and extras) from the most recent 10m before start (only for partial hour)
                if not is_final:
                    added = add_prev_point_if_available()
                    if not added:
                        # No 10m and no prev — still include existing candle (if any) in aggregates
                        if existing:
                            mcap_exist = sanitize_num(getattr(existing, "market_cap", None), 0.0)
                            vol_exist  = sanitize_num(getattr(existing, "volume_24h", None), 0.0)
                            last_upd   = existing_last_upd or (end - timedelta(seconds=1))
                            bump_hour_total(start, coin_category, mcap_exist, vol_exist, last_upd)
                            bump_hour_total(start, 'ALL',        mcap_exist, vol_exist, last_upd)
                        empty += 1
                        vprint(f"[{now_str()}]    {c.symbol} {start.isoformat()} empty (no 10m & no prev) → aggregate existing if present; skip write")
                        continue
                else:
                    # final hour with no points — aggregate existing if present; skip write
                    if existing:
                        mcap_exist = sanitize_num(getattr(existing, "market_cap", None), 0.0)
                        vol_exist  = sanitize_num(getattr(existing, "volume_24h", None), 0.0)
                        last_upd   = existing_last_upd or (end - timedelta(seconds=1))
                        bump_hour_total(start, coin_category, mcap_exist, vol_exist, last_upd)
                        bump_hour_total(start, 'ALL',        mcap_exist, vol_exist, last_upd)
                    empty += 1
                    vprint(f"[{now_str()}]    {c.symbol} {start.isoformat()} empty (final hour) → aggregate existing if present; skip write")
                    continue

            # OHLC from 10m within hour (or carried prev for open)
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

            # Fallback to existing row for ancillary fields (avoid nulling established values)
            if existing:
                if mcap is None: mcap = sanitize_num(getattr(existing, "market_cap", None))
                if vol  is None: vol  = sanitize_num(getattr(existing, "volume_24h", None))
                if rank is None: rank = getattr(existing, "market_cap_rank", None)
                if circ is None: circ = sanitize_num(getattr(existing, "circulating_supply", None))
                if tot  is None:  tot  = sanitize_num(getattr(existing, "total_supply", None))

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

            # Skip write if already final & identical, BUT include existing in aggregates
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
                    mcap_exist = sanitize_num(getattr(existing, "market_cap", None), 0.0)
                    vol_exist  = sanitize_num(getattr(existing, "volume_24h", None), 0.0)
                    last_upd_e = existing_last_upd or (end - timedelta(seconds=1))
                    bump_hour_total(start, coin_category, mcap_exist, vol_exist, last_upd_e)
                    bump_hour_total(start, 'ALL',        mcap_exist, vol_exist, last_upd_e)
                    finalized_skips += 1
                    vprint(f"[{now_str()}]    {c.symbol} {start.isoformat()} already final & identical → aggregate existing; skip write")
                    continue

            # For partials, skip writing when unchanged vs existing partial (reduces churn) BUT include existing in aggregates
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
                    mcap_exist = sanitize_num(getattr(existing, "market_cap", None), 0.0)
                    vol_exist  = sanitize_num(getattr(existing, "volume_24h", None), 0.0)
                    last_upd_e = existing_last_upd or (end - timedelta(seconds=1))
                    bump_hour_total(start, coin_category, mcap_exist, vol_exist, last_upd_e)
                    bump_hour_total(start, 'ALL',        mcap_exist, vol_exist, last_upd_e)
                    unchanged_skips += 1
                    vprint(f"[{now_str()}]    {c.symbol} {start.isoformat()} partial unchanged → aggregate existing; skip write")
                    continue

            # UPSERT the hourly row (bind timestamps as naive UTC)
            try:
                session.execute(
                    INS_UPSERT,
                    [c.id, to_cassandra_ts(start), c.symbol, c.name,
                     o, h, l, cl, cl,
                     mcap, vol, rank, circ, tot,
                     candle_source, to_cassandra_ts(last_upd)],
                    timeout=REQUEST_TIMEOUT
                )
                # Include the freshly written values in aggregates
                bump_hour_total(start, coin_category, mcap, vol, last_upd)
                bump_hour_total(start, 'ALL',        mcap, vol, last_upd)
                vprint(f"[{now_str()}]    UPSERT {c.symbol} {start.isoformat()} ({candle_source}) "
                       f"O={o} H={h} L={l} C={cl} M={mcap} V={vol} R={rank} CS={circ} TS={tot} LU={last_upd.isoformat()}")
                wrote += 1
            except Exception as e:
                errors += 1
                print(f"[{now_str()}] [WRITE-ERR] {c.symbol} {start.isoformat()}: {e}")
                skipped += 1

    # Write category aggregates
    
    if hour_totals:
        print(f"[{now_str()}] [mcap-hourly] writing {len(hour_totals)} aggregates into {TABLE_MCAP_HOURLY}")

        # ➊ Compute ranks per slot (ALL = 0; categories ranked by total mcap DESC)
        slot_caps = defaultdict(list)  # slot_start -> [(category, mcap_sum)]
        for (slot_start, category), totals in hour_totals.items():
            if category != 'ALL':
                slot_caps[slot_start].append((category, float(totals.get('market_cap') or 0.0)))

        rank_map: dict[tuple[datetime, str], int] = {}
        for slot_start, items in slot_caps.items():
            items.sort(key=lambda t: t[1], reverse=True)
            for i, (cat, _mc) in enumerate(items, start=1):
                rank_map[(slot_start, cat)] = i
            rank_map[(slot_start, 'ALL')] = 0  # sentinel

        # ➋ Upsert rows with computed rank
        agg_written = 0
        for (slot_start, category), totals in sorted(
            hour_totals.items(),
            key=lambda kv: (kv[0][0], 0 if kv[0][1] == 'ALL' else 1, kv[0][1].lower())
        ):
            last_upd = totals.get('last_updated') or (slot_start + timedelta(hours=1) - timedelta(seconds=1))
            rank_value = rank_map.get((slot_start, category))  # None if unseen → leave null

            try:
                session.execute(
                    INS_MCAP_HOURLY,
                    [
                        category,
                        to_cassandra_ts(slot_start),
                        to_cassandra_ts(last_upd),
                        float(totals['market_cap']),
                        rank_value,
                        float(totals['volume_24h']),
                    ],
                    timeout=REQUEST_TIMEOUT,
                )
                vprint(
                    f"[{now_str()}]    MCAP_AGG [{category}] {slot_start.isoformat()} "
                    f"M={totals['market_cap']:.6f} V={totals['volume_24h']:.6f} "
                    f"RANK={rank_value} LU={last_upd.isoformat()}"
                )
                agg_written += 1
            except Exception as e:
                print(f"[{now_str()}] [mcap-hourly] failed for category='{category}' slot={slot_start.isoformat()}: {e}")
        print(f"[{now_str()}] [mcap-hourly] rows_written={agg_written}")
    else:
        print(f"[{now_str()}] [mcap-hourly] no aggregates captured (coins={len(coins)})")

    print(
        f"[{now_str()}] [hourly-from-10m] wrote={wrote} skipped={skipped} empty={empty} "
        f"errors={errors} already_final_identical={finalized_skips} partial_unchanged_skips={unchanged_skips}"
    )

# ───────────────────────── Entrypoint ─────────────────────────
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
