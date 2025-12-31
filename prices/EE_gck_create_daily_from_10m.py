#!/usr/bin/env python3
# backend/prices/EE_gck_create_daily_from_10m.py

import os, time
from datetime import datetime, timedelta, timezone, date
from typing import Dict, Tuple, Any, List

# Astra connector
from astra_connect.connect import get_session, AstraConfig
AstraConfig.from_env()  # loads .env if present, otherwise uses process env

# Config
TOP_N               = int(os.getenv("TOP_N", "300"))
REQUEST_TIMEOUT     = int(os.getenv("REQUEST_TIMEOUT_SEC", "30"))
FETCH_SIZE          = int(os.getenv("FETCH_SIZE", "500"))

# Behavior
SLOT_DELAY_SEC      = int(os.getenv("SLOT_DELAY_SEC", "180"))
PROGRESS_EVERY      = int(os.getenv("PROGRESS_EVERY", "20"))
VERBOSE_MODE        = os.getenv("VERBOSE_MODE", "0") == "1"
MIN_10M_POINTS_FOR_FINAL = int(os.getenv("MIN_10M_POINTS_FOR_FINAL", "2"))
BOOTSTRAP_BACKFILL_DAYS  = int(os.getenv("BOOTSTRAP_BACKFILL_DAYS", "7"))
MAX_CATCHUP_DAYS         = min(int(os.getenv("MAX_CATCHUP_DAYS", "7")), 7)
WATERMARK_SCAN_LIMIT     = 12

TEN_MIN_TABLE       = os.getenv("TEN_MIN_TABLE", "gecko_prices_10m_7d")
DAILY_TABLE         = os.getenv("DAILY_TABLE", "gecko_candles_daily_contin")
TABLE_LIVE          = os.getenv("TABLE_LIVE", "gecko_prices_live")
TABLE_MCAP_DAILY    = os.getenv("TABLE_MCAP_DAILY", "gecko_market_cap_daily_contin")
WRITE_MCAP_DAILY = os.getenv("WRITE_MCAP_DAILY", "0") == "1"


UTC = timezone.utc

def now_utc() -> datetime:
    return datetime.now(UTC)

def now_str() -> str:
    return now_utc().strftime("%Y-%m-%d %H:%M:%S")

def ensure_aware_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)

def to_cassandra_ts(dt: datetime) -> datetime:
    if dt.tzinfo is not None:
        return dt.astimezone(UTC).replace(tzinfo=None)
    return dt

def floor_10m(dt_utc: datetime) -> datetime:
    dt_utc = ensure_aware_utc(dt_utc)
    return dt_utc.replace(minute=(dt_utc.minute // 10) * 10, second=0, microsecond=0)

def day_bounds_utc(d: date) -> tuple[datetime, datetime]:
    start = datetime(d.year, d.month, d.day, tzinfo=UTC)
    end_excl = start + timedelta(days=1)
    return start, end_excl

def normalize_date(d) -> date | None:
    if d is None:
        return None
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    if hasattr(d, "date"):
        try:
            return d.date()
        except Exception:
            pass
    try:
        return date(d.year, d.month, d.day)
    except Exception:
        return None

def sanitize_num(x, fallback=None):
    try:
        if x is None:
            return fallback
        return float(x)
    except Exception:
        return fallback

def equalish(a, b):
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    try:
        return abs(float(a) - float(b)) <= 1e-12
    except Exception:
        return False

print(f"[{now_str()}] Connecting to Astra...")
session, cluster = get_session(return_cluster=True)
print(f"[{now_str()}] Connected. keyspace='{session.keyspace}'")

from cassandra.query import SimpleStatement

SEL_LIVE = SimpleStatement(
    f"""
      SELECT id, symbol, name, market_cap_rank, category
      FROM {TABLE_LIVE}
    """,
    fetch_size=FETCH_SIZE,
)

SEL_10M_RANGE = session.prepare(f"""
  SELECT ts, price_usd, market_cap, volume_24h,
         market_cap_rank, circulating_supply, total_supply, last_updated
  FROM {TEN_MIN_TABLE}
  WHERE id=? AND ts>=? AND ts<?
""")

SEL_DAILY_ONE = session.prepare(f"""
  SELECT symbol, name, open, high, low, close, price_usd, market_cap, volume_24h,
         market_cap_rank, circulating_supply, total_supply, candle_source, last_updated
  FROM {DAILY_TABLE}
  WHERE id=? AND date=? LIMIT 1
""")

SEL_DAILY_LATEST = session.prepare(f"""
  SELECT date, candle_source, last_updated
  FROM {DAILY_TABLE} WHERE id=? ORDER BY date DESC LIMIT 1
""")

SEL_DAILY_RECENT = session.prepare(f"""
  SELECT date, candle_source
  FROM {DAILY_TABLE} WHERE id=? ORDER BY date DESC LIMIT {WATERMARK_SCAN_LIMIT}
""")

INS_UPSERT = session.prepare(f"""
  INSERT INTO {DAILY_TABLE}
    (id, date, symbol, name,
     open, high, low, close, price_usd,
     market_cap, volume_24h,
     market_cap_rank, circulating_supply, total_supply,
     candle_source, last_updated)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

INS_MCAP_DAILY = session.prepare(f"""
  INSERT INTO {TABLE_MCAP_DAILY}
    (category, date, last_updated, market_cap, market_cap_rank, volume_24h)
  VALUES (?, ?, ?, ?, ?, ?)
""")


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


def rebuild_all_from_categories(day_totals: Dict[Tuple[date, str], Dict[str, Any]]) -> Dict[Tuple[date, str], Dict[str, Any]]:
    """
    Ensure ALL equals the sum of category buckets per day.
    last_updated is max(last_updated) across categories.
    """
    by_day: dict[date, list[tuple[str, Dict[str, Any]]]] = {}
    for (d, category), totals in day_totals.items():
        by_day.setdefault(d, []).append((category, totals))

    for d, items in by_day.items():
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
        day_totals[(d, "ALL")] = {
            "market_cap": total_mcap,
            "volume_24h": total_vol,
            "last_updated": latest_lu,
        }
    return day_totals


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


def iter_days(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d = d + timedelta(days=1)


def get_latest_final_date(coin_id) -> date | None:
    try:
        row = session.execute(SEL_DAILY_LATEST.bind([coin_id]), timeout=REQUEST_TIMEOUT).one()
        if row and getattr(row, "date", None):
            if getattr(row, "candle_source", None) == "10m_final":
                return normalize_date(row.date)
    except Exception as e:
        if VERBOSE_MODE:
            print(f"[{now_str()}] [WATERMARK-ERR] {coin_id}: {e} (fallback to recent scan)")

    try:
        for r in session.execute(SEL_DAILY_RECENT.bind([coin_id]), timeout=REQUEST_TIMEOUT):
            if getattr(r, "candle_source", None) == "10m_final" and getattr(r, "date", None):
                return normalize_date(r.date)
    except Exception as e:
        if VERBOSE_MODE:
            print(f"[{now_str()}] [WATERMARK-SCAN-ERR] {coin_id}: {e}")
    return None


def main():
    now_guarded = now_utc() - timedelta(seconds=SLOT_DELAY_SEC)
    today = now_guarded.date()
    yesterday = today - timedelta(days=1)

    today_start, today_end_excl = day_bounds_utc(today)
    today_effective_end = min(today_end_excl, floor_10m(now_guarded))
    partial_needed = today_effective_end > today_start

    print(
        f"[{now_str()}] Daily anchors: today={today} yesterday={yesterday} "
        f"partial_end={(today_effective_end.isoformat() if partial_needed else 'none')}"
    )

    print(f"[{now_str()}] Loading top coins (timeout={REQUEST_TIMEOUT}s)...")
    t0 = time.time()
    coins = list(session.execute(SEL_LIVE, timeout=REQUEST_TIMEOUT))
    print(f"[{now_str()}] Loaded {len(coins)} rows from {TABLE_LIVE} in {time.time()-t0:.2f}s")

    coins = [r for r in coins if isinstance(r.market_cap_rank, int) and r.market_cap_rank > 0]
    coins.sort(key=lambda r: r.market_cap_rank)
    coins = coins[:TOP_N]
    print(f"[{now_str()}] Processing top {len(coins)} coins (TOP_N={TOP_N})")

    day_totals: Dict[Tuple[date, str], Dict[str, Any]] = {}

    def bump_day_total(day_key: date, category: str, mcap_value, vol_value, last_upd: datetime) -> None:
        key = (day_key, category)
        entry = day_totals.setdefault(key, {"market_cap": 0.0, "volume_24h": 0.0, "last_updated": last_upd})
        entry["market_cap"] += float(mcap_value or 0.0)
        entry["volume_24h"] += float(vol_value or 0.0)
        if last_upd and (entry["last_updated"] is None or last_upd > entry["last_updated"]):
            entry["last_updated"] = last_upd

    wrote = skipped = empty = errors = unchanged = 0

    for idx, c in enumerate(coins, 1):
        coin_category = (getattr(c, 'category', None) or 'Other').strip() or 'Other'
        if idx == 1 or idx % PROGRESS_EVERY == 0 or idx == len(coins):
            print(
                f"[{now_str()}] -> Coin {idx}/{len(coins)}: {getattr(c,'symbol','?')} "
                f"({getattr(c,'id','?')}) rank={getattr(c,'market_cap_rank','?')}"
            )

        watermark_date = get_latest_final_date(c.id)
        if watermark_date:
            start_day = watermark_date + timedelta(days=1)
        else:
            start_day = yesterday - timedelta(days=BOOTSTRAP_BACKFILL_DAYS - 1)

        if MAX_CATCHUP_DAYS > 0:
            max_start = yesterday - timedelta(days=MAX_CATCHUP_DAYS - 1)
            if start_day < max_start:
                print(
                    f"[{now_str()}]    {c.symbol} clamp start {start_day} -> {max_start} "
                    f"(MAX_CATCHUP_DAYS={MAX_CATCHUP_DAYS})"
                )
                start_day = max_start

        final_days: List[date] = []
        if start_day <= yesterday:
            final_days = list(iter_days(start_day, yesterday))

        if not final_days and not partial_needed:
            continue

        range_start = today_start if not final_days else day_bounds_utc(final_days[0])[0]
        range_end = today_effective_end if partial_needed else day_bounds_utc(yesterday)[1]

        buckets: Dict[date, Dict[str, Any]] = {}
        try:
            bound = SEL_10M_RANGE.bind([c.id, to_cassandra_ts(range_start), to_cassandra_ts(range_end)])
            bound.fetch_size = FETCH_SIZE
            t_read = time.time()
            row_count = 0
            for row in session.execute(bound, timeout=REQUEST_TIMEOUT):
                row_count += 1
                ts = ensure_aware_utc(getattr(row, "ts", None))
                if ts is None:
                    continue
                day_key = ts.date()
                bucket = buckets.setdefault(day_key, make_bucket())
                update_bucket(bucket, ts, row)
            if VERBOSE_MODE:
                print(
                    f"[{now_str()}]    {c.symbol} {range_start.isoformat()}->{range_end.isoformat()} "
                    f"got {row_count} 10m pts in {time.time()-t_read:.2f}s"
                )
        except Exception as e:
            errors += 1
            print(f"[{now_str()}] [READ-ERR] {c.symbol} 10m range: {e}")
            continue

        existing_cache: Dict[date, Any] = {}
        existing_last_upd_cache: Dict[date, datetime | None] = {}

        def get_existing(d: date):
            if d in existing_cache:
                return existing_cache[d]
            try:
                existing = session.execute(SEL_DAILY_ONE, [c.id, d], timeout=REQUEST_TIMEOUT).one()
                existing_cache[d] = existing
                existing_last_upd_cache[d] = ensure_aware_utc(getattr(existing, "last_updated", None)) if existing else None
                return existing
            except Exception as e:
                existing_cache[d] = None
                existing_last_upd_cache[d] = None
                if VERBOSE_MODE:
                    print(f"[{now_str()}] [EXIST-ERR] {c.symbol} {d}: {e}")
                return None

        def aggregate_existing(d: date) -> None:
            existing = get_existing(d)
            if existing:
                mcap_exist = sanitize_num(getattr(existing, "market_cap", None), 0.0)
                vol_exist = sanitize_num(getattr(existing, "volume_24h", None), 0.0)
                last_upd = existing_last_upd_cache.get(d) or (day_bounds_utc(d)[1] - timedelta(seconds=1))
                bump_day_total(d, coin_category, mcap_exist, vol_exist, last_upd)
                bump_day_total(d, 'ALL', mcap_exist, vol_exist, last_upd)

        for d in final_days:
            bucket = buckets.get(d)
            price_count = bucket["price_count"] if bucket else 0
            if price_count < MIN_10M_POINTS_FOR_FINAL:
                if VERBOSE_MODE:
                    print(
                        f"[{now_str()}]    {c.symbol} {d} final: insufficient 10m points ({price_count}) "
                        "-> aggregate existing; skip write"
                    )
                aggregate_existing(d)
                skipped += 1
                continue

            o = bucket["open"]
            h = bucket["high"]
            l = bucket["low"]
            cl = bucket["close"]

            mcap = bucket["mcap"]
            vol = bucket["vol"]
            rank = bucket["rank"]
            circ = bucket["circ"]
            tot = bucket["tot"]

            existing = None
            if mcap is None or vol is None or rank is None or circ is None or tot is None:
                existing = get_existing(d)
                if existing:
                    if mcap is None: mcap = sanitize_num(getattr(existing, "market_cap", None))
                    if vol is None: vol = sanitize_num(getattr(existing, "volume_24h", None))
                    if rank is None: rank = getattr(existing, "market_cap_rank", None)
                    if circ is None: circ = sanitize_num(getattr(existing, "circulating_supply", None))
                    if tot is None: tot = sanitize_num(getattr(existing, "total_supply", None))

            if mcap is None: mcap = 0.0
            if vol is None: vol = 0.0

            last_upd = bucket["last_updated"] or (day_bounds_utc(d)[1] - timedelta(seconds=1))
            candle_source = "10m_final"

            existing = existing or get_existing(d)
            if existing and getattr(existing, "candle_source", None) == "10m_final":
                same = all([
                    equalish(o, getattr(existing, "open", None)),
                    equalish(h, getattr(existing, "high", None)),
                    equalish(l, getattr(existing, "low", None)),
                    equalish(cl, getattr(existing, "close", None)),
                    equalish(cl, getattr(existing, "price_usd", None)),
                    equalish(mcap, getattr(existing, "market_cap", None)),
                    equalish(vol, getattr(existing, "volume_24h", None)),
                    (rank == getattr(existing, "market_cap_rank", None)),
                    equalish(circ, getattr(existing, "circulating_supply", None)),
                    equalish(tot, getattr(existing, "total_supply", None)),
                ])
                if same:
                    aggregate_existing(d)
                    unchanged += 1
                    if VERBOSE_MODE:
                        print(f"[{now_str()}]    {c.symbol} {d} already final & identical -> aggregate existing; skip write")
                    continue

            try:
                session.execute(
                    INS_UPSERT,
                    [
                        c.id, d, c.symbol, c.name,
                        o, h, l, cl, cl,
                        mcap, vol,
                        rank, circ, tot,
                        candle_source, to_cassandra_ts(last_upd),
                    ],
                    timeout=REQUEST_TIMEOUT,
                )
                bump_day_total(d, coin_category, mcap, vol, last_upd)
                bump_day_total(d, 'ALL', mcap, vol, last_upd)
                wrote += 1
                if VERBOSE_MODE:
                    print(f"[{now_str()}]    UPSERT {c.symbol} {d} ({candle_source})")
            except Exception as e:
                errors += 1
                skipped += 1
                print(f"[{now_str()}] [WRITE-ERR] {c.symbol} {d}: {e}")

        if partial_needed:
            d = today
            bucket = buckets.get(d)
            price_count = bucket["price_count"] if bucket else 0
            if price_count == 0:
                aggregate_existing(d)
                empty += 1
                if VERBOSE_MODE:
                    print(f"[{now_str()}]    {c.symbol} {d} empty (no 10m) -> aggregate existing; skip write")
                continue

            o = bucket["open"]
            h = bucket["high"]
            l = bucket["low"]
            cl = bucket["close"]

            mcap = bucket["mcap"]
            vol = bucket["vol"]
            rank = bucket["rank"]
            circ = bucket["circ"]
            tot = bucket["tot"]

            existing = None
            if mcap is None or vol is None or rank is None or circ is None or tot is None:
                existing = get_existing(d)
                if existing:
                    if mcap is None: mcap = sanitize_num(getattr(existing, "market_cap", None))
                    if vol is None: vol = sanitize_num(getattr(existing, "volume_24h", None))
                    if rank is None: rank = getattr(existing, "market_cap_rank", None)
                    if circ is None: circ = sanitize_num(getattr(existing, "circulating_supply", None))
                    if tot is None: tot = sanitize_num(getattr(existing, "total_supply", None))

            if mcap is None: mcap = 0.0
            if vol is None: vol = 0.0

            last_upd = bucket["last_updated"] or (today_end_excl - timedelta(seconds=1))
            candle_source = "10m_partial"

            existing = existing or get_existing(d)
            if existing and getattr(existing, "candle_source", None) == "10m_partial":
                same = all([
                    equalish(o, getattr(existing, "open", None)),
                    equalish(h, getattr(existing, "high", None)),
                    equalish(l, getattr(existing, "low", None)),
                    equalish(cl, getattr(existing, "close", None)),
                    equalish(cl, getattr(existing, "price_usd", None)),
                    equalish(mcap, getattr(existing, "market_cap", None)),
                    equalish(vol, getattr(existing, "volume_24h", None)),
                    (rank == getattr(existing, "market_cap_rank", None)),
                    equalish(circ, getattr(existing, "circulating_supply", None)),
                    equalish(tot, getattr(existing, "total_supply", None)),
                ])
                if same:
                    aggregate_existing(d)
                    unchanged += 1
                    if VERBOSE_MODE:
                        print(f"[{now_str()}]    {c.symbol} {d} partial unchanged -> aggregate existing; skip write")
                    continue

            try:
                session.execute(
                    INS_UPSERT,
                    [
                        c.id, d, c.symbol, c.name,
                        o, h, l, cl, cl,
                        mcap, vol,
                        rank, circ, tot,
                        candle_source, to_cassandra_ts(last_upd),
                    ],
                    timeout=REQUEST_TIMEOUT,
                )
                bump_day_total(d, coin_category, mcap, vol, last_upd)
                bump_day_total(d, 'ALL', mcap, vol, last_upd)
                wrote += 1
                if VERBOSE_MODE:
                    print(f"[{now_str()}]    UPSERT {c.symbol} {d} ({candle_source})")
            except Exception as e:
                errors += 1
                skipped += 1
                print(f"[{now_str()}] [WRITE-ERR] {c.symbol} {d}: {e}")

    if WRITE_MCAP_DAILY and day_totals:
        print(f"[{now_str()}] [mcap-daily] writing {len(day_totals)} aggregates into {TABLE_MCAP_DAILY}")

        # Rebuild ALL from categories before ranking/writing.
        day_totals = rebuild_all_from_categories(day_totals)

        # Compute ranks per day (ALL=0; categories ranked by total mcap DESC)
        day_caps: dict[date, list[tuple[str, float]]] = {}
        for (day_key, category), totals in day_totals.items():
            if category != 'ALL':
                day_caps.setdefault(day_key, []).append((category, float(totals.get('market_cap') or 0.0)))

        rank_map: dict[tuple[date, str], int] = {}
        for dkey, items in day_caps.items():
            items.sort(key=lambda t: t[1], reverse=True)
            for i, (cat, _mc) in enumerate(items, start=1):
                rank_map[(dkey, cat)] = i
            rank_map[(dkey, 'ALL')] = 0

        agg_written = 0
        for (day_key, category), totals in sorted(
            day_totals.items(), key=lambda kv: (kv[0][0], 0 if kv[0][1] == 'ALL' else 1, kv[0][1].lower())
        ):
            day_end = day_bounds_utc(day_key)[1]
            last_upd = totals.get('last_updated') or (day_end - timedelta(seconds=1))
            rank_value = rank_map.get((day_key, category))
            try:
                session.execute(
                    INS_MCAP_DAILY,
                    [category, day_key, to_cassandra_ts(last_upd), float(totals['market_cap']), rank_value, float(totals['volume_24h'])],
                    timeout=REQUEST_TIMEOUT,
                )
                agg_written += 1
            except Exception as e:
                print(f"[{now_str()}] [mcap-daily] failed for category='{category}' day={day_key}: {e}")
        print(f"[{now_str()}] [mcap-daily] rows_written={agg_written}")
    else:
        print(f"[{now_str()}] [mcap-daily] no aggregates captured (coins={len(coins)})")

    print(
        f"[{now_str()}] [daily-from-10m] wrote={wrote} skipped={skipped} empty={empty} "
        f"unchanged={unchanged} errors={errors}"
    )


if __name__ == "__main__":
    try:
        main()
    finally:
        print(f"[{now_str()}] Shutting down...")
        try:
            cluster.shutdown()
        except Exception as e:
            print(f"[{now_str()}] Error during shutdown: {e}")
        print(f"[{now_str()}] Done.")
