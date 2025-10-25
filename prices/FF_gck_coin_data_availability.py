#!/usr/bin/env python3
# file: FF_gck_coin_data_availability.py
#
# Maintains three coverage/availability tables:
#   1) coin_daily_coverage_ranges  – contiguous daily coverage ranges
#   2) coin_daily_availability     – per-coin counters (idempotent, yesterday-only)
#   3) coin_intraday_coverage      – per-day bitmaps for 10m (last 7d) and 1h (last 30d)
#
# Source tables:
#   - gecko_candles_daily_contin
#   - gecko_prices_10m_7d
#   - gecko_candles_hourly_30d
#
# Universe:
#   - gecko_prices_live (id/symbol/name), or DISTINCT ids from daily (heavier)
#
# Notes:
#   - Daily summary/ranges are only updated when the target day EXISTS in daily.
#   - Intraday coverage scans the full window ONCE per granularity per coin,
#     buckets in-memory, reads existing rows, OR-merges, and UPSERTs only changed days.

from __future__ import annotations

import os
from time import perf_counter, sleep
from datetime import datetime, timezone, timedelta, date as dtdate
from typing import Dict, Optional, Tuple, Iterable, DefaultDict
from collections import defaultdict

from astra_connect.connect import get_session
from cassandra import ConsistencyLevel

# ────────────────────────── Config ──────────────────────────
KS = os.getenv("KEYSPACE", os.getenv("ASTRA_KEYSPACE", "default_keyspace")).strip()

TABLE_DAILY  = f"{KS}.gecko_candles_daily_contin"
TABLE_10M    = f"{KS}.gecko_prices_10m_7d"
TABLE_HOURLY = f"{KS}.gecko_candles_hourly_30d"
TABLE_LIVE   = f"{KS}.gecko_prices_live"

T_DAILY_RANGES  = f"{KS}.coin_daily_coverage_ranges"
T_DAILY_SUMMARY = f"{KS}.coin_daily_availability"
T_INTRADAY_BITS = f"{KS}.coin_intraday_coverage"

# Granularity tags (tinyint): 1=10m, 2=1h
G_10M = 1
G_1H  = 2
SLOTS_10M = 24 * 6   # 144 slots/day
SLOTS_1H  = 24       # 24 slots/day

# Windows to maintain
WIN_10M_DAYS   = int(os.getenv("WIN_10M_DAYS", "7"))
WIN_HOURLY_DYS = int(os.getenv("WIN_HOURLY_DAYS", "30"))

# Daily target (default: yesterday UTC)
TARGET_DAY_ISO = os.getenv("TARGET_DAY_ISO", "")  # e.g. "2025-01-14"

# Perf / behavior
SLEEP_BETWEEN_COINS_SEC = float(os.getenv("SLEEP_BETWEEN_COINS_SEC", "0"))
FETCH_SIZE = int(os.getenv("FETCH_SIZE", "2000"))
TIMEOUT    = int(os.getenv("REQUEST_TIMEOUT_SEC", "30"))
LOG_EVERY  = int(os.getenv("LOG_EVERY_COINS", "25"))
VERBOSE    = os.getenv("VERBOSE", "0").lower() not in {"0", "false", "no"}

# Intraday coverage TTLs (0 = no TTL)
TTL_10M = int(os.getenv("TTL_10M_COVERAGE_SEC", str(8 * 24 * 3600)))   # ~8 days
TTL_1H  = int(os.getenv("TTL_1H_COVERAGE_SEC",  str(33 * 24 * 3600)))  # ~33 days

# Universe source
IDS_SOURCE = os.getenv("IDS_SOURCE", "live").strip().lower()  # "live" | "daily"
AVAIL_RUN_DAILY      = os.getenv("AVAIL_RUN_DAILY", "1") == "1"       # daily ranges + summary
AVAIL_RUN_INTRADAY   = os.getenv("AVAIL_RUN_INTRADAY", "1") == "1"    # 10m/hourly bitmaps

# ────────────────────────── Logging helpers ──────────────────────────
_LOG_HEARTBEAT_SEC  = float(os.getenv("LOG_HEARTBEAT_SEC", "15"))
_last_log_monotonic = [perf_counter()]

def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(msg: str) -> None:
    print(f"[{_ts()}] {msg}", flush=True)

def heartbeat(tag: str = "") -> None:
    now = perf_counter()
    if (now - _last_log_monotonic[0]) >= _LOG_HEARTBEAT_SEC:
        _last_log_monotonic[0] = now
        log(f"[hb] {tag} … still working")

# ────────────────────────── Small utils ──────────────────────────
def utcnow() -> datetime:
    return datetime.now(timezone.utc)

def to_pydate(d) -> dtdate:
    if d is None:
        raise TypeError("date is None")
    if isinstance(d, dtdate) and not isinstance(d, datetime):
        return d
    if isinstance(d, datetime):
        return d.date()
    try:
        return d.date()  # cassandra.util.Date
    except Exception:
        pass
    return dtdate.fromisoformat(str(d)[:10])

def day_bounds_utc(d: dtdate) -> Tuple[datetime, datetime]:
    s = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    return s, s + timedelta(days=1)

def ensure_bytes_len(b: Optional[bytes], needed: int) -> bytearray:
    if not b:
        return bytearray(needed)
    ba = bytearray(b)
    if len(ba) < needed:
        ba.extend(b"\x00" * (needed - len(ba)))
    elif len(ba) > needed:
        ba = ba[:needed]
    return ba

def popcount(b: bytes) -> int:
    return sum(bin(x).count("1") for x in b)

def slot_index_10m(dt_: datetime) -> int:
    return dt_.hour * 6 + (dt_.minute // 10)

def slot_index_1h(dt_: datetime) -> int:
    return dt_.hour

def bind_cl(ps, params, cl=ConsistencyLevel.LOCAL_QUORUM):
    bound = ps.bind(params)
    bound.consistency_level = cl
    try:
        bound.fetch_size = FETCH_SIZE  # safe for SELECTs
    except Exception:
        pass
    return bound

def exec_timed(session, bound, *, label: str, timeout: int = TIMEOUT):
    t0 = perf_counter()
    rs = session.execute(bound, timeout=timeout)
    dt = perf_counter() - t0
    if VERBOSE:
        log(f"[Q] {label} took {dt:.2f}s")
    _last_log_monotonic[0] = perf_counter()
    return rs

# ────────────────────────── Main ──────────────────────────
def main() -> None:
    # Determine target day (yesterday by default)
    if TARGET_DAY_ISO:
        target_day: dtdate = dtdate.fromisoformat(TARGET_DAY_ISO)
    else:
        target_day = (utcnow() - timedelta(days=1)).date()

    log("Connecting to Astra…")
    session, cluster = get_session(return_cluster=True)
    log("Connected.")

    try:
        # Universe
        t_ids = perf_counter()
        if IDS_SOURCE == "daily":
            ps_ids = session.prepare(f"SELECT DISTINCT id FROM {TABLE_DAILY}")
            coin_rows = exec_timed(session, bind_cl(ps_ids, []), label="DISTINCT ids from daily")
            ids_meta: Dict[str, Dict[str, Optional[str]]] = {r.id: {"symbol": None, "name": None} for r in coin_rows}
        else:
            ps_ids = session.prepare(f"SELECT id, symbol, name FROM {TABLE_LIVE}")
            coin_rows = exec_timed(session, bind_cl(ps_ids, []), label="ids from live")
            ids_meta = {r.id: {"symbol": getattr(r, "symbol", None), "name": getattr(r, "name", None)} for r in coin_rows}

        coin_ids = sorted(ids_meta.keys())
        log(f"Universe size: {len(coin_ids)} (source={IDS_SOURCE})  target_day={target_day} (collect={(perf_counter()-t_ids):.2f}s)")

        # Prepared statements
        PS_DAILY_POINT  = session.prepare(
            f"SELECT date, price_usd, close, market_cap, volume_24h, symbol, name "
            f"FROM {TABLE_DAILY} WHERE id=? AND date=? LIMIT 1"
        )
        PS_UPSERT_RANGE = session.prepare(
            f"INSERT INTO {T_DAILY_RANGES} (id, start_date, end_date, detected_at) VALUES (?,?,?,?)"
        )
        PS_SUMMARY_READ = session.prepare(
            f"SELECT first_day, last_day, expected_days, have_any, have_price, have_volume, have_mcap, "
            f"       missing_any, missing_price, missing_volume, missing_mcap, "
            f"       symbol, name, open_range_start "
            f"FROM {T_DAILY_SUMMARY} WHERE id=?"
        )
        PS_SUMMARY_WRITE = session.prepare(
            f"INSERT INTO {T_DAILY_SUMMARY} "
            f"(id, first_day, last_day, expected_days, have_any, missing_any, "
            f" have_price, missing_price, have_volume, missing_volume, have_mcap, missing_mcap, "
            f" symbol, name, updated_at, open_range_start) "
            f"VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        )

        # Intraday scans & coverage reads
        PS_10M_WINDOW = session.prepare(
            f"SELECT ts FROM {TABLE_10M} WHERE id=? AND ts >= ? AND ts < ?"
        )
        PS_1H_WINDOW  = session.prepare(
            f"SELECT ts FROM {TABLE_HOURLY} WHERE id=? AND ts >= ? AND ts < ?"
        )
        PS_BITS_RANGE = session.prepare(
            f"SELECT day, bitmap, set_count, first_seen, last_seen "
            f"FROM {T_INTRADAY_BITS} WHERE id=? AND granularity=? AND day >= ? AND day <= ?"
        )

        # Coverage upserts (with TTLs if set)
        INS_BITS_BASE = (
            f"INSERT INTO {T_INTRADAY_BITS} "
            f"(id, granularity, day, bitmap, set_count, first_seen, last_seen) "
            f"VALUES (?,?,?,?,?,?,?)"
        )
        INS_BITS_10M = session.prepare(INS_BITS_BASE + ("" if TTL_10M <= 0 else f" USING TTL {TTL_10M}"))
        INS_BITS_1H  = session.prepare(INS_BITS_BASE + ("" if TTL_1H  <= 0 else f" USING TTL {TTL_1H}"))

        # Counters
        updates_ranges = 0
        updates_summary = 0
        updates_10m = 0
        updates_1h = 0
        now_ts = utcnow()

        # Window bounds (today = UTC date when the job runs)
        today = utcnow().date()
        start_10m = today - timedelta(days=WIN_10M_DAYS - 1)
        start_1h  = today - timedelta(days=WIN_HOURLY_DYS - 1)
        s10_start_dt, _ = day_bounds_utc(start_10m)
        s1h_start_dt, _ = day_bounds_utc(start_1h)
        _, end_win_dt   = day_bounds_utc(today)  # exclusive end

        # Helper closures
        def read_existing_bits_map(cid: str, gran: int, start_day: dtdate, end_day: dtdate):
            """Return map day -> (bitmap bytearray, set_count, first_seen, last_seen)."""
            out: Dict[dtdate, Tuple[bytearray, int, Optional[datetime], Optional[datetime]]] = {}
            rs = exec_timed(
                session,
                bind_cl(PS_BITS_RANGE, [cid, gran, start_day, end_day]),
                label=f"bits range read {cid} g={gran} [{start_day}..{end_day}]"
            )
            need_len = (SLOTS_10M + 7)//8 if gran == G_10M else (SLOTS_1H + 7)//8
            for r in rs:
                d = to_pydate(r.day)
                bm = ensure_bytes_len(getattr(r, "bitmap", None), need_len)
                sc = int(getattr(r, "set_count", 0) or 0)
                fs = getattr(r, "first_seen", None)
                ls = getattr(r, "last_seen", None)
                out[d] = (bm, sc, fs, ls)
            return out

        def scan_source_window_and_bucket(cid: str, ps, start_dt: datetime, end_dt: datetime, gran: int):
            """Scan the full window once and bucket ts into per-day bitmaps."""
            rs = exec_timed(
                session,
                bind_cl(ps, [cid, start_dt, end_dt]),
                label=f"source scan {cid} g={gran} [{start_dt}..{end_dt})"
            )
            need_len = (SLOTS_10M + 7)//8 if gran == G_10M else (SLOTS_1H + 7)//8
            day_to_bits: DefaultDict[dtdate, bytearray] = defaultdict(lambda: bytearray(need_len))
            total_rows = 0
            for r in rs:
                ts_ = getattr(r, "ts", None)
                if not isinstance(ts_, datetime):
                    continue
                if ts_.tzinfo is None:
                    ts_ = ts_.replace(tzinfo=timezone.utc)
                d = ts_.date()
                idx = slot_index_10m(ts_) if gran == G_10M else slot_index_1h(ts_)
                if 0 <= idx < (SLOTS_10M if gran == G_10M else SLOTS_1H):
                    ba = day_to_bits[d]
                    ba[idx // 8] |= (1 << (idx % 8))
                    total_rows += 1
            if VERBOSE:
                log(f"  bucketed {total_rows} rows into {len(day_to_bits)} day(s) for g={gran}")
            return day_to_bits

        # Per-coin loop
        for i, cid in enumerate(coin_ids, 1):
            if i == 1 or (i % LOG_EVERY == 0) or i == len(coin_ids):
                log(f"[{i}/{len(coin_ids)}] {cid}")

            symbol = (ids_meta.get(cid, {}).get("symbol") or "")[:64]
            name   = (ids_meta.get(cid, {}).get("name") or "")[:128]
            log(f"→ {cid} (symbol={symbol or '-'}, name={name or '-'})")

            # (1) Daily probe + summary/range (yesterday only, and only if row exists)
            if AVAIL_RUN_DAILY:
                t_phase = perf_counter()
                row = exec_timed(
                    session,
                    bind_cl(PS_DAILY_POINT, [cid, target_day]),
                    label=f"daily probe {cid} @ {target_day}"
                ).one()

                if row:
                    has_price = (getattr(row, "close", None) is not None) or (getattr(row, "price_usd", None) is not None)
                    has_mcap  = (getattr(row, "market_cap", None) is not None)
                    has_vol   = (getattr(row, "volume_24h", None) is not None)
                    # Fill missing labels from source row (best effort)
                    if not symbol:
                        symbol = getattr(row, "symbol", "") or ""
                    if not name:
                        name = getattr(row, "name", "") or ""

                    s = exec_timed(session, bind_cl(PS_SUMMARY_READ, [cid]), label=f"summary read {cid}").one()
                    if not s:
                        first_day = target_day
                        last_day  = target_day
                        expected_days = 1
                        have_any   = 1
                        have_price = 1 if has_price else 0
                        have_mcap  = 1 if has_mcap  else 0
                        have_vol   = 1 if has_vol   else 0
                        missing_any    = 0
                        missing_price  = 1 - have_price
                        missing_mcap   = 1 - have_mcap
                        missing_volume = 1 - have_vol
                        open_start     = target_day

                        exec_timed(session, bind_cl(PS_SUMMARY_WRITE, [
                            cid, first_day, last_day, expected_days,
                            have_any,   missing_any,
                            have_price, missing_price,
                            have_vol,   missing_volume,
                            have_mcap,  missing_mcap,
                            symbol, name, now_ts, open_start
                        ]), label=f"summary upsert {cid}")
                        exec_timed(session, bind_cl(PS_UPSERT_RANGE, [cid, open_start, target_day, now_ts]),
                                label=f"range upsert {cid} [{open_start}..{target_day}]")
                        updates_summary += 1
                        updates_ranges  += 1
                    else:
                        first_day = to_pydate(getattr(s, "first_day", target_day)) if getattr(s, "first_day", None) else target_day
                        last_day  = to_pydate(getattr(s, "last_day",  target_day)) if getattr(s, "last_day",  None) else target_day
                        expected_days = int(getattr(s, "expected_days", 0) or 0)
                        have_any      = int(getattr(s, "have_any", 0) or 0)
                        have_price    = int(getattr(s, "have_price", 0) or 0)
                        have_mcap     = int(getattr(s, "have_mcap", 0) or 0)
                        have_vol      = int(getattr(s, "have_volume", 0) or 0)
                        open_start    = getattr(s, "open_range_start", None)

                        if first_day <= target_day <= last_day:
                            if VERBOSE:
                                log("  daily: already counted (idempotent rerun)")
                        else:
                            if (last_day is not None) and (target_day == (last_day + timedelta(days=1))):
                                # extend current run
                                last_day = target_day
                                expected_days += 1
                                have_any   += 1
                                have_price += 1 if has_price else 0
                                have_mcap  += 1 if has_mcap  else 0
                                have_vol   += 1 if has_vol   else 0
                                if open_start is None:
                                    open_start = first_day or target_day
                                exec_timed(session, bind_cl(PS_UPSERT_RANGE, [cid, open_start, target_day, now_ts]),
                                        label=f"range extend {cid} [{open_start}..{target_day}]")
                                updates_ranges += 1
                            else:
                                # start a new run
                                if first_day is None:
                                    first_day = target_day
                                last_day = target_day
                                expected_days = (last_day - first_day).days + 1
                                have_any   += 1
                                have_price += 1 if has_price else 0
                                have_mcap  += 1 if has_mcap  else 0
                                have_vol   += 1 if has_vol   else 0
                                open_start = target_day
                                exec_timed(session, bind_cl(PS_UPSERT_RANGE, [cid, open_start, target_day, now_ts]),
                                        label=f"range open {cid} [{open_start}..{target_day}]")
                                updates_ranges += 1

                            missing_any    = max(0, expected_days - have_any)
                            missing_price  = max(0, expected_days - have_price)
                            missing_mcap   = max(0, expected_days - have_mcap)
                            missing_volume = max(0, expected_days - have_vol)

                            exec_timed(session, bind_cl(PS_SUMMARY_WRITE, [
                                cid, first_day, last_day, expected_days,
                                have_any,   missing_any,
                                have_price, missing_price,
                                have_vol,   missing_volume,
                                have_mcap,  missing_mcap,
                                symbol, name, now_ts, open_start
                            ]), label=f"summary upsert {cid}")
                            updates_summary += 1
                else:
                    if VERBOSE:
                        log(f"  daily: MISSING for {target_day} (skip summary/range)")

                if VERBOSE:
                    log(f"  daily phase took {(perf_counter()-t_phase):.2f}s")
                heartbeat(f"{cid} daily")
            else:
                if VERBOSE: log("skip daily availability (AVAIL_RUN_DAILY=0)")

            # (2) Intraday coverage (10m then 1h)
            if AVAIL_RUN_INTRADAY:
                # 10m
                t10 = perf_counter()
                existing_10m = read_existing_bits_map(cid, G_10M, start_10m, today)
                buckets_10m  = scan_source_window_and_bucket(cid, PS_10M_WINDOW, s10_start_dt, end_win_dt, G_10M)
                upserts_10m_for_coin = 0
                for d, new_bits in buckets_10m.items():
                    old_ba, old_count, fs, _ls = existing_10m.get(
                        d, (bytearray(len(new_bits)), 0, None, None)
                    )
                    merged = bytearray(max(len(old_ba), len(new_bits)))
                    for i in range(len(merged)):
                        merged[i] = (old_ba[i] if i < len(old_ba) else 0) | (new_bits[i] if i < len(new_bits) else 0)
                    if bytes(merged) == bytes(old_ba):
                        continue
                    count = popcount(merged)
                    first_seen = fs or now_ts
                    last_seen  = now_ts
                    exec_timed(
                        session,
                        bind_cl(INS_BITS_10M, [cid, G_10M, d, bytes(merged), count, first_seen, last_seen]),
                        label=f"bits10 upsert {cid} {d} (bits={count}, was={old_count})"
                    )
                    updates_10m += 1
                    upserts_10m_for_coin += 1
                    heartbeat(f"{cid} 10m")
                if VERBOSE:
                    log(f"  10m phase: upserts={upserts_10m_for_coin}, elapsed={(perf_counter()-t10):.2f}s")

                # 1h
                t1h = perf_counter()
                existing_1h = read_existing_bits_map(cid, G_1H, start_1h, today)
                buckets_1h  = scan_source_window_and_bucket(cid, PS_1H_WINDOW, s1h_start_dt, end_win_dt, G_1H)
                upserts_1h_for_coin = 0
                for d, new_bits in buckets_1h.items():
                    old_ba, old_count, fs, _ls = existing_1h.get(
                        d, (bytearray(len(new_bits)), 0, None, None)
                    )
                    merged = bytearray(max(len(old_ba), len(new_bits)))
                    for i in range(len(merged)):
                        merged[i] = (old_ba[i] if i < len(old_ba) else 0) | (new_bits[i] if i < len(new_bits) else 0)
                    if bytes(merged) == bytes(old_ba):
                        continue
                    count = popcount(merged)
                    first_seen = fs or now_ts
                    last_seen  = now_ts
                    exec_timed(
                        session,
                        bind_cl(INS_BITS_1H, [cid, G_1H, d, bytes(merged), count, first_seen, last_seen]),
                        label=f"bits1h upsert {cid} {d} (bits={count}, was={old_count})"
                    )
                    updates_1h += 1
                    upserts_1h_for_coin += 1
                    heartbeat(f"{cid} 1h")
                if VERBOSE:
                    log(f"  1h phase: upserts={upserts_1h_for_coin}, elapsed={(perf_counter()-t1h):.2f}s")

                log(f"← {cid} done")

                if SLEEP_BETWEEN_COINS_SEC:
                    sleep(SLEEP_BETWEEN_COINS_SEC)
            else:
                if VERBOSE: log("skip intraday coverage (AVAIL_RUN_INTRADAY=0)")

        elapsed = perf_counter() - t_ids
        log(f"DONE. coins={len(coin_ids)} | ranges_upserts={updates_ranges} "
            f"| daily_summary_upserts={updates_summary} | intraday_10m_upserts={updates_10m} "
            f"| intraday_1h_upserts={updates_1h} | elapsed={(perf_counter()-t_ids):.2f}s")

    finally:
        try:
            log("Shutting down cluster…")
            cluster.shutdown()
            log("Cluster shutdown complete.")
        except Exception as e:
            log(f"Cluster shutdown warning: {e}")

if __name__ == "__main__":
    main()
