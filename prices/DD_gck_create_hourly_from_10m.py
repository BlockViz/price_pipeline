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
VERBOSE_MODE            = os.getenv("VERBOSE_MODE", "0") == "1"
MIN_10M_POINTS_FOR_FINAL= int(os.getenv("MIN_10M_POINTS_FOR_FINAL", "1"))  # allow finalize on a single 10m/carry point
CURRENT_ONLY            = os.getenv("CURRENT_ONLY", "0") == "1"
BOOTSTRAP_BACKFILL_HOURS= int(os.getenv("BOOTSTRAP_BACKFILL_HOURS", "24"))
MAX_CATCHUP_HOURS        = int(os.getenv("MAX_CATCHUP_HOURS", "48"))
WRITE_CONCURRENCY        = int(os.getenv("WRITE_CONCURRENCY", "32"))
WATERMARK_SCAN_LIMIT     = 12

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

def floor_hour(dt_utc: datetime) -> datetime:
    """Floor to hour boundary (aware UTC in/out)."""
    dt_utc = ensure_aware_utc(dt_utc)
    return dt_utc.replace(minute=0, second=0, microsecond=0)

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

def rebuild_all_from_categories(hour_totals: Dict[Tuple[datetime, str], Dict[str, Any]]) -> Dict[Tuple[datetime, str], Dict[str, Any]]:
    """
    Ensure the ALL bucket equals the sum of category buckets for each hour.
    last_updated = max(last_updated) across categories.
    """
    by_hour: dict[datetime, list[tuple[str, Dict[str, Any]]]] = {}
    for (slot_start, category), totals in hour_totals.items():
        by_hour.setdefault(slot_start, []).append((category, totals))

    for slot_start, items in by_hour.items():
        non_all = [(cat, vals) for cat, vals in items if cat != "ALL"]
        if not non_all:
            continue
        latest_lu = None
        total_mcap = 0.0
        total_vol = 0.0
        for _cat, vals in non_all:
            lu = vals.get("last_updated")
            if lu is not None and (latest_lu is None or lu > latest_lu):
                latest_lu = lu
            total_mcap += float(vals.get("market_cap") or 0.0)
            total_vol += float(vals.get("volume_24h") or 0.0)
        hour_totals[(slot_start, "ALL")] = {
            "market_cap": total_mcap,
            "volume_24h": total_vol,
            "last_updated": latest_lu,
        }
    return hour_totals

# ───────────────────────── Session & prepared statements ─────────────────────────
print(f"[{now_str()}] Connecting to Astra…")
session, cluster = get_session(return_cluster=True)
print(f"[{now_str()}] Connected. keyspace='{session.keyspace}'")

from cassandra.query import SimpleStatement
from cassandra.concurrent import execute_concurrent_with_args

# Live coins (use CoinGecko rank field)
SEL_LIVE = SimpleStatement(
    f"SELECT id, symbol, name, market_cap_rank, category FROM {TABLE_LIVE}",
    fetch_size=FETCH_SIZE
)

def main():
    # Prepare statements
    print(f"[{now_str()}] Preparing statements…")
    t0 = time.time()
    SEL_10M_RANGE = session.prepare(f"""
      SELECT ts, price_usd, market_cap, volume_24h,
             market_cap_rank, circulating_supply, total_supply, last_updated
      FROM {TEN_MIN_TABLE}
      WHERE id=? AND ts>=? AND ts<?
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
    SEL_HOURLY_LATEST = session.prepare(f"""
      SELECT ts, candle_source, last_updated
      FROM {HOURLY_TABLE} WHERE id=? ORDER BY ts DESC LIMIT 1
    """)
    SEL_HOURLY_RECENT = session.prepare(f"""
      SELECT ts, candle_source
      FROM {HOURLY_TABLE} WHERE id=? ORDER BY ts DESC LIMIT {WATERMARK_SCAN_LIMIT}
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

    # Time anchors
    now_guarded = now_utc() - timedelta(seconds=SLOT_DELAY_SEC)
    curr_hour_start = floor_hour(now_guarded)
    curr_hour_end = curr_hour_start + timedelta(hours=1)
    partial_end = min(curr_hour_end, floor_10m(now_guarded))
    partial_needed = partial_end > curr_hour_start
    prev_final = curr_hour_start - timedelta(hours=1)

    print(f"[{now_str()}] Anchor hour={curr_hour_start.isoformat()} partial_end={(partial_end.isoformat() if partial_needed else 'none')} CURRENT_ONLY={CURRENT_ONLY}")

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

    def iter_hours(start: datetime, end: datetime):
        h = start
        while h < end:
            yield h
            h += timedelta(hours=1)

    def make_bucket() -> Dict[str, Any]:
        return {
            "price_count": 0,
            "earliest_ts": None,
            "latest_ts": None,
            "open": None,
            "close": None,
            "high": None,
            "low": None,
            "last_updated": None,
            "mcap": None, "mcap_ts": None,
            "vol": None, "vol_ts": None,
            "rank": None, "rank_ts": None,
            "circ": None, "circ_ts": None,
            "tot": None, "tot_ts": None,
        }

    def update_last_non_null(bucket: Dict[str, Any], key: str, ts_key: str, value, row_ts: datetime) -> None:
        if value is None:
            return
        if bucket[ts_key] is None or row_ts >= bucket[ts_key]:
            bucket[key] = value
            bucket[ts_key] = row_ts

    def update_bucket(bucket: Dict[str, Any], row_ts: datetime, row) -> None:
        price = getattr(row, "price_usd", None)
        if price is not None:
            price = float(price)
            bucket["price_count"] += 1
            if bucket["earliest_ts"] is None or row_ts < bucket["earliest_ts"]:
                bucket["earliest_ts"] = row_ts
                bucket["open"] = price
            if bucket["latest_ts"] is None or row_ts > bucket["latest_ts"]:
                bucket["latest_ts"] = row_ts
                bucket["close"] = price
            if bucket["high"] is None or price > bucket["high"]:
                bucket["high"] = price
            if bucket["low"] is None or price < bucket["low"]:
                bucket["low"] = price

        lu = ensure_aware_utc(getattr(row, "last_updated", None))
        if lu is not None and (bucket["last_updated"] is None or lu > bucket["last_updated"]):
            bucket["last_updated"] = lu

        update_last_non_null(bucket, "mcap", "mcap_ts", sanitize_num(getattr(row, "market_cap", None)), row_ts)
        update_last_non_null(bucket, "vol",  "vol_ts",  sanitize_num(getattr(row, "volume_24h", None)), row_ts)
        rank_val = getattr(row, "market_cap_rank", None)
        update_last_non_null(bucket, "rank", "rank_ts", int(rank_val) if rank_val is not None else None, row_ts)
        update_last_non_null(bucket, "circ", "circ_ts", sanitize_num(getattr(row, "circulating_supply", None)), row_ts)
        update_last_non_null(bucket, "tot",  "tot_ts",  sanitize_num(getattr(row, "total_supply", None)), row_ts)

    def get_latest_final_ts(coin_id):
        try:
            row = session.execute(SEL_HOURLY_LATEST.bind([coin_id]), timeout=REQUEST_TIMEOUT).one()
            if row and getattr(row, "ts", None):
                if getattr(row, "candle_source", None) == "10m_final":
                    return ensure_aware_utc(row.ts)
        except Exception as e:
            vprint(f"[{now_str()}] [WATERMARK-ERR] {coin_id}: {e} (fallback to recent scan)")

        try:
            for r in session.execute(SEL_HOURLY_RECENT.bind([coin_id]), timeout=REQUEST_TIMEOUT):
                if getattr(r, "candle_source", None) == "10m_final" and getattr(r, "ts", None):
                    return ensure_aware_utc(r.ts)
        except Exception as e:
            vprint(f"[{now_str()}] [WATERMARK-SCAN-ERR] {coin_id}: {e}")
        return None

    for ci, c in enumerate(coins, 1):
        if ci == 1 or ci % PROGRESS_EVERY == 0 or ci == len(coins):
            print(f"[{now_str()}] → Coin {ci}/{len(coins)}: {getattr(c,'symbol','?')} ({getattr(c,'id','?')}) r={getattr(c,'market_cap_rank','?')}")

        coin_category = (getattr(c, 'category', None) or 'Other').strip() or 'Other'

        watermark_ts = get_latest_final_ts(c.id)
        if watermark_ts:
            watermark_hour = floor_hour(watermark_ts)
            start_hour = watermark_hour + timedelta(hours=1)
        else:
            watermark_hour = None
            start_hour = curr_hour_start - timedelta(hours=BOOTSTRAP_BACKFILL_HOURS)

        end_final_hour_excl = curr_hour_start
        if MAX_CATCHUP_HOURS > 0:
            max_start = end_final_hour_excl - timedelta(hours=MAX_CATCHUP_HOURS)
            if start_hour < max_start:
                print(f"[{now_str()}]    {c.symbol} clamp start {start_hour.isoformat()} -> {max_start.isoformat()} (MAX_CATCHUP_HOURS={MAX_CATCHUP_HOURS})")
                start_hour = max_start

        hours: List[Tuple[datetime, datetime, bool]] = []
        if CURRENT_ONLY:
            hours.append((prev_final, prev_final + timedelta(hours=1), True))
        else:
            for h in iter_hours(start_hour, end_final_hour_excl):
                hours.append((h, h + timedelta(hours=1), True))
        if partial_needed:
            hours.append((curr_hour_start, partial_end, False))

        vprint(
            f"[{now_str()}]    {c.symbol} watermark={(watermark_hour.isoformat() if watermark_hour else 'none')} "
            f"final_range=[{start_hour.isoformat()},{end_final_hour_excl.isoformat()}) "
            f"partial_end={(partial_end.isoformat() if partial_needed else 'none')} hours={len(hours)}"
        )

        if not hours:
            continue

        range_start = min(h[0] for h in hours)
        range_end = max(h[1] for h in hours)
        buckets: Dict[datetime, Dict[str, Any]] = {}

        # Fetch 10m points for the whole span
        try:
            bstart = to_cassandra_ts(range_start)
            bend   = to_cassandra_ts(range_end)
            bound = SEL_10M_RANGE.bind([c.id, bstart, bend]); bound.fetch_size = FETCH_SIZE
            t_read = time.time()
            row_count = 0
            for p in session.execute(bound, timeout=REQUEST_TIMEOUT):
                row_count += 1
                ts = ensure_aware_utc(getattr(p, "ts", None))
                if ts is None:
                    continue
                hour_start = floor_hour(ts)
                bucket = buckets.setdefault(hour_start, make_bucket())
                update_bucket(bucket, ts, p)
            vprint(f"[{now_str()}]    {c.symbol} {range_start.isoformat()}→{range_end.isoformat()} got {row_count} 10m pts in {time.time()-t_read:.2f}s")
        except Exception as e:
            errors += 1
            print(f"[{now_str()}] [READ-ERR] {c.symbol} {range_start.isoformat()}→{range_end.isoformat()}: {e}")
            skipped += 1
            continue

        existing_cache: Dict[datetime, Any] = {}
        existing_last_upd_cache: Dict[datetime, datetime | None] = {}

        def get_existing(hour_start: datetime):
            if hour_start in existing_cache:
                return existing_cache[hour_start]
            try:
                bound_hour = SEL_HOURLY_ONE.bind([c.id, to_cassandra_ts(hour_start)])
                existing = session.execute(bound_hour, timeout=REQUEST_TIMEOUT).one()
                existing_cache[hour_start] = existing
                existing_last_upd_cache[hour_start] = ensure_aware_utc(getattr(existing, "last_updated", None)) if existing else None
                return existing
            except Exception as e:
                existing_cache[hour_start] = None
                existing_last_upd_cache[hour_start] = None
                vprint(f"[{now_str()}] [EXIST-ERR] {c.symbol} {hour_start.isoformat()}: {e} (treat as no row)")
                return None

        def add_prev_point_if_available(hour_start: datetime) -> bool:
            try:
                prev = session.execute(
                    SEL_10M_PREV.bind([c.id, to_cassandra_ts(hour_start)]),
                    timeout=REQUEST_TIMEOUT
                ).one()
            except Exception as e:
                vprint(f"[{now_str()}] [PREV-ERR] {c.symbol} < {hour_start.isoformat()}: {e}")
                return False
            if prev and prev.price_usd is not None:
                ts_prev = ensure_aware_utc(getattr(prev, "ts", None)) or hour_start
                bucket = buckets.setdefault(hour_start, make_bucket())
                update_bucket(bucket, ts_prev, prev)
                return True
            return False

        hourly_upsert_args: List[Tuple[Any, ...]] = []
        hourly_upsert_meta: List[Dict[str, Any]] = []

        for start, end, is_final in hours:
            bucket = buckets.get(start)
            price_count = bucket["price_count"] if bucket else 0

            if is_final and price_count < MIN_10M_POINTS_FOR_FINAL:
                vprint(f"[{now_str()}]    {c.symbol} {start.isoformat()} final: insufficient 10m points ({price_count}) → aggregate existing; skip write")
                existing = get_existing(start)
                if existing:
                    mcap_exist = sanitize_num(getattr(existing, "market_cap", None), 0.0)
                    vol_exist  = sanitize_num(getattr(existing, "volume_24h", None), 0.0)
                    last_upd   = existing_last_upd_cache.get(start) or (end - timedelta(seconds=1))
                    bump_hour_total(start, coin_category, mcap_exist, vol_exist, last_upd)
                    bump_hour_total(start, 'ALL',        mcap_exist, vol_exist, last_upd)
                skipped += 1
                continue

            if not is_final and price_count == 0:
                added = add_prev_point_if_available(start)
                if not added:
                    existing = get_existing(start)
                    if existing:
                        mcap_exist = sanitize_num(getattr(existing, "market_cap", None), 0.0)
                        vol_exist  = sanitize_num(getattr(existing, "volume_24h", None), 0.0)
                        last_upd   = existing_last_upd_cache.get(start) or (end - timedelta(seconds=1))
                        bump_hour_total(start, coin_category, mcap_exist, vol_exist, last_upd)
                        bump_hour_total(start, 'ALL',        mcap_exist, vol_exist, last_upd)
                    empty += 1
                    vprint(f"[{now_str()}]    {c.symbol} {start.isoformat()} empty (no 10m & no prev) → aggregate existing if present; skip write")
                    continue
                bucket = buckets.get(start)
                price_count = bucket["price_count"] if bucket else 0

            if not bucket or price_count == 0:
                existing = get_existing(start)
                if existing:
                    mcap_exist = sanitize_num(getattr(existing, "market_cap", None), 0.0)
                    vol_exist  = sanitize_num(getattr(existing, "volume_24h", None), 0.0)
                    last_upd   = existing_last_upd_cache.get(start) or (end - timedelta(seconds=1))
                    bump_hour_total(start, coin_category, mcap_exist, vol_exist, last_upd)
                    bump_hour_total(start, 'ALL',        mcap_exist, vol_exist, last_upd)
                empty += 1
                kind = "final hour" if is_final else "partial hour"
                vprint(f"[{now_str()}]    {c.symbol} {start.isoformat()} empty ({kind}) → aggregate existing if present; skip write")
                continue

            o = bucket["open"]
            h = bucket["high"]
            l = bucket["low"]
            cl = bucket["close"]

            mcap = bucket["mcap"]
            vol  = bucket["vol"]
            rank = bucket["rank"]
            circ = bucket["circ"]
            tot  = bucket["tot"]

            if mcap is None or vol is None or rank is None or circ is None or tot is None:
                existing = get_existing(start)
                if existing:
                    if mcap is None: mcap = sanitize_num(getattr(existing, "market_cap", None))
                    if vol  is None: vol  = sanitize_num(getattr(existing, "volume_24h", None))
                    if rank is None: rank = getattr(existing, "market_cap_rank", None)
                    if circ is None: circ = sanitize_num(getattr(existing, "circulating_supply", None))
                    if tot  is None:  tot  = sanitize_num(getattr(existing, "total_supply", None))

            if mcap is None: mcap = 0.0
            if vol  is None: vol  = 0.0

            candle_source = "10m_final" if is_final else "10m_partial"
            last_upd = bucket["last_updated"] or (end - timedelta(seconds=1))

            existing = None
            if is_final:
                existing = get_existing(start)
                if existing and getattr(existing, "candle_source", None) == "10m_final":
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
                        last_upd_e = existing_last_upd_cache.get(start) or (end - timedelta(seconds=1))
                        bump_hour_total(start, coin_category, mcap_exist, vol_exist, last_upd_e)
                        bump_hour_total(start, 'ALL',        mcap_exist, vol_exist, last_upd_e)
                        finalized_skips += 1
                        vprint(f"[{now_str()}]    {c.symbol} {start.isoformat()} already final & identical → aggregate existing; skip write")
                        continue
            else:
                existing = get_existing(start)
                if existing and getattr(existing, "candle_source", None) == "10m_partial":
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
                        last_upd_e = existing_last_upd_cache.get(start) or (end - timedelta(seconds=1))
                        bump_hour_total(start, coin_category, mcap_exist, vol_exist, last_upd_e)
                        bump_hour_total(start, 'ALL',        mcap_exist, vol_exist, last_upd_e)
                        unchanged_skips += 1
                        vprint(f"[{now_str()}]    {c.symbol} {start.isoformat()} partial unchanged → aggregate existing; skip write")
                        continue

            hourly_upsert_args.append(
                [c.id, to_cassandra_ts(start), c.symbol, c.name,
                 o, h, l, cl, cl,
                 mcap, vol, rank, circ, tot,
                 candle_source, to_cassandra_ts(last_upd)]
            )
            hourly_upsert_meta.append(
                {
                    "symbol": c.symbol,
                    "start": start,
                    "end": end,
                    "category": coin_category,
                    "mcap": mcap,
                    "vol": vol,
                    "rank": rank,
                    "circ": circ,
                    "tot": tot,
                    "o": o,
                    "h": h,
                    "l": l,
                    "cl": cl,
                    "last_upd": last_upd,
                    "candle_source": candle_source,
                }
            )

        if hourly_upsert_args:
            results = execute_concurrent_with_args(
                session,
                INS_UPSERT,
                hourly_upsert_args,
                concurrency=WRITE_CONCURRENCY,
                raise_on_first_error=False,
            )
            for (success, result), meta in zip(results, hourly_upsert_meta):
                if success:
                    bump_hour_total(meta["start"], meta["category"], meta["mcap"], meta["vol"], meta["last_upd"])
                    bump_hour_total(meta["start"], 'ALL',        meta["mcap"], meta["vol"], meta["last_upd"])
                    vprint(
                        f"[{now_str()}]    UPSERT {meta['symbol']} {meta['start'].isoformat()} ({meta['candle_source']}) "
                        f"O={meta['o']} H={meta['h']} L={meta['l']} C={meta['cl']} M={meta['mcap']} V={meta['vol']} "
                        f"R={meta['rank']} CS={meta['circ']} TS={meta['tot']} LU={meta['last_upd'].isoformat()}"
                    )
                    wrote += 1
                else:
                    errors += 1
                    skipped += 1
                    print(f"[{now_str()}] [WRITE-ERR] {meta['symbol']} {meta['start'].isoformat()}: {result}")

    # Write category aggregates
    if hour_totals:
        print(f"[{now_str()}] [mcap-hourly] writing {len(hour_totals)} aggregates into {TABLE_MCAP_HOURLY}")

        # Rebuild ALL from category buckets to avoid stale/partial ALL.
        hour_totals = rebuild_all_from_categories(hour_totals)

        # Compute ranks per slot (ALL = 0; categories ranked by total mcap DESC)
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
        agg_args: List[List[Any]] = []
        agg_meta: List[Dict[str, Any]] = []
        for (slot_start, category), totals in sorted(
            hour_totals.items(),
            key=lambda kv: (kv[0][0], 0 if kv[0][1] == 'ALL' else 1, kv[0][1].lower())
        ):
            last_upd = totals.get('last_updated') or (slot_start + timedelta(hours=1) - timedelta(seconds=1))
            rank_value = rank_map.get((slot_start, category))  # None if unseen → leave null

            agg_args.append(
                [
                    category,
                    to_cassandra_ts(slot_start),
                    to_cassandra_ts(last_upd),
                    float(totals['market_cap']),
                    rank_value,
                    float(totals['volume_24h']),
                ]
            )
            agg_meta.append(
                {
                    "category": category,
                    "slot_start": slot_start,
                    "last_upd": last_upd,
                    "rank_value": rank_value,
                    "market_cap": totals["market_cap"],
                    "volume_24h": totals["volume_24h"],
                }
            )

        if agg_args:
            results = execute_concurrent_with_args(
                session,
                INS_MCAP_HOURLY,
                agg_args,
                concurrency=WRITE_CONCURRENCY,
                raise_on_first_error=False,
            )
            for (success, result), meta in zip(results, agg_meta):
                if success:
                    vprint(
                        f"[{now_str()}]    MCAP_AGG [{meta['category']}] {meta['slot_start'].isoformat()} "
                        f"M={meta['market_cap']:.6f} V={meta['volume_24h']:.6f} "
                        f"RANK={meta['rank_value']} LU={meta['last_upd'].isoformat()}"
                    )
                    agg_written += 1
                else:
                    print(
                        f"[{now_str()}] [mcap-hourly] failed for category='{meta['category']}' "
                        f"slot={meta['slot_start'].isoformat()}: {result}"
                    )
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
