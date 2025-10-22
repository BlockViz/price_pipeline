#!/usr/bin/env python3
# file: daily_data_availability.py
# Summarize per-coin daily data availability from KEYSPACE.gecko_candles_daily_contin
# into KEYSPACE.coin_daily_availability, with detailed progress logging.
# Also pulls `symbol` and `name` from the source and includes them in CSV,
# and (if the summary table has those columns) writes them there too.

from __future__ import annotations

import os
import csv
from time import perf_counter, sleep
from datetime import datetime, timezone, date as dtdate
from typing import Iterable, List, Dict, Set, Tuple, Optional

from astra_connect.connect import get_session
from cassandra import ConsistencyLevel

# ────────────────────────── Config ──────────────────────────
KEYSPACE = os.getenv("KEYSPACE", "default_keyspace").strip()
SOURCE_TABLE = f"{KEYSPACE}.gecko_candles_daily_contin"
SUMMARY_TABLE = f"{KEYSPACE}.coin_daily_availability"

# Verbosity / behavior (override via env)
LOG_EVERY_ROWS: int = int(os.getenv("LOG_EVERY_ROWS", "10000"))   # log every N rows scanned per coin
LOG_EVERY_COINS: int = int(os.getenv("LOG_EVERY_COINS", "25"))    # log every N coins processed
SHOW_QUERY_TIMES: bool = os.getenv("SHOW_QUERY_TIMES", "1").lower() not in {"0", "false"}
SLEEP_BETWEEN_COINS_SEC: float = float(os.getenv("SLEEP_BETWEEN_COINS_SEC", "0"))
FETCH_SIZE: int = int(os.getenv("FETCH_SIZE", "2000"))            # driver may ignore on prepared, harmless to set
REQUEST_TIMEOUT_SEC: int = int(os.getenv("REQUEST_TIMEOUT_SEC", "30"))
WRITE_CSV: bool = os.getenv("WRITE_CSV", "1").lower() not in {"0", "false"}
CSV_PATH: str = os.getenv("CSV_PATH", "coin_daily_availability.csv")

# ─────────────────────── Utilities ──────────────────────────
def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)

def to_pydate(d) -> dtdate:
    """Normalize Cassandra date-like values → datetime.date (handles datetime, cassandra.util.Date)."""
    if d is None:
        raise TypeError("date is None")
    if isinstance(d, dtdate) and not isinstance(d, datetime):
        return d
    if isinstance(d, datetime):
        return d.date()
    # cassandra.util.Date typically has .date()
    try:
        return d.date()  # type: ignore[attr-defined]
    except Exception:
        pass
    return dtdate.fromisoformat(str(d)[:10])

def is_present_price(row) -> bool:
    """Daily 'price' presence: prefer close, fallback to price_usd."""
    return (getattr(row, "close", None) is not None) or (getattr(row, "price_usd", None) is not None)

# ───────────────────────── Main ─────────────────────────────
def main() -> None:
    start_all = perf_counter()
    session, cluster = get_session(return_cluster=True)
    try:
        log(f"Preparing statements (source={SOURCE_TABLE}, summary={SUMMARY_TABLE})")

        # Distinct coin ids (partition keys)
        ps_ids = session.prepare(f"SELECT DISTINCT id FROM {SOURCE_TABLE}")

        # Partition-local scan per id (date + columns we care about + a bit of meta)
        ps_rows_for_id = session.prepare(
            f"""
            SELECT date, price_usd, volume_24h, market_cap, close, symbol, name
            FROM {SOURCE_TABLE}
            WHERE id = ?
            """
        )
        try:
            ps_rows_for_id.fetch_size = FETCH_SIZE  # some drivers ignore on prepared; harmless
        except Exception:
            pass

        # Try extended summary upsert including symbol/name; else fall back to base schema
        try:
            ps_upsert = session.prepare(
                f"""
                INSERT INTO {SUMMARY_TABLE} (
                  id, first_day, last_day, expected_days,
                  have_any, missing_any,
                  have_price, missing_price,
                  have_volume, missing_volume,
                  have_mcap, missing_mcap,
                  symbol, name,
                  updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """
            )
            upsert_includes_meta = True
            log("Summary table seems to accept symbol,name (extended upsert enabled).")
        except Exception:
            ps_upsert = session.prepare(
                f"""
                INSERT INTO {SUMMARY_TABLE} (
                  id, first_day, last_day, expected_days,
                  have_any, missing_any,
                  have_price, missing_price,
                  have_volume, missing_volume,
                  have_mcap, missing_mcap,
                  updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """
            )
            upsert_includes_meta = False
            log("Summary table does not have symbol/name; using base upsert.")

        # Get all coin ids
        t0 = perf_counter()
        log("Querying distinct coin ids…")
        bound_ids = ps_ids.bind([])
        bound_ids.consistency_level = ConsistencyLevel.LOCAL_QUORUM
        coin_ids = [row.id for row in session.execute(bound_ids, timeout=REQUEST_TIMEOUT_SEC)]
        t1 = perf_counter()
        log(f"Found {len(coin_ids)} coin ids (took {(t1 - t0):.2f}s)")

        rows_for_csv: List[Dict[str, object]] = []
        now_ts = datetime.now(timezone.utc)

        total_rows_scanned = 0
        total_rows_upserted = 0
        total_missing_any = 0

        for i, coin_id in enumerate(coin_ids, 1):
            if i == 1 or i % LOG_EVERY_COINS == 0:
                log(f"[{i}/{len(coin_ids)}] Starting {coin_id}…")

            coin_start = perf_counter()

            dates_set: Set[dtdate] = set()
            price_days: Set[dtdate] = set()
            vol_days: Set[dtdate] = set()
            mcap_days: Set[dtdate] = set()

            symbol: Optional[str] = None
            name: Optional[str] = None

            # Scan partition
            qstart = perf_counter()
            b = ps_rows_for_id.bind([coin_id])
            b.consistency_level = ConsistencyLevel.LOCAL_QUORUM
            rs = session.execute(b, timeout=REQUEST_TIMEOUT_SEC)

            qtime_first: Optional[float] = None
            row_count = 0
            for row in rs:
                if qtime_first is None:
                    qtime_first = perf_counter() - qstart
                    if SHOW_QUERY_TIMES:
                        log(f"  {coin_id}: time to first row {qtime_first:.2f}s")

                d = to_pydate(row.date)
                dates_set.add(d)
                row_count += 1

                if symbol is None and getattr(row, "symbol", None):
                    symbol = row.symbol
                if name is None and getattr(row, "name", None):
                    name = row.name

                if is_present_price(row):
                    price_days.add(d)
                if getattr(row, "volume_24h", None) is not None:
                    vol_days.add(d)
                if getattr(row, "market_cap", None) is not None:
                    mcap_days.add(d)

                if LOG_EVERY_ROWS > 0 and (row_count % LOG_EVERY_ROWS == 0):
                    log(f"  {coin_id}: scanned {row_count} rows so far…")

            scan_time = perf_counter() - coin_start
            total_rows_scanned += row_count

            if not dates_set:
                log(f"  {coin_id}: no rows (skipping). Scan time {scan_time:.2f}s")
                if SLEEP_BETWEEN_COINS_SEC:
                    sleep(SLEEP_BETWEEN_COINS_SEC)
                continue

            first_day = min(dates_set)
            last_day = max(dates_set)
            have_any = len(dates_set)
            expected_days = (last_day - first_day).days + 1

            missing_any = max(0, expected_days - have_any)
            missing_price = max(0, expected_days - len(price_days))
            missing_volume = max(0, expected_days - len(vol_days))
            missing_mcap = max(0, expected_days - len(mcap_days))

            total_missing_any += missing_any

            # Upsert summary with controlled consistency
            ust = perf_counter()
            if upsert_includes_meta:
                bu = ps_upsert.bind([
                    coin_id,
                    first_day, last_day, expected_days,
                    have_any, missing_any,
                    len(price_days), missing_price,
                    len(vol_days), missing_volume,
                    len(mcap_days), missing_mcap,
                    symbol or "", name or "",
                    now_ts,
                ])
            else:
                bu = ps_upsert.bind([
                    coin_id,
                    first_day, last_day, expected_days,
                    have_any, missing_any,
                    len(price_days), missing_price,
                    len(vol_days), missing_volume,
                    len(mcap_days), missing_mcap,
                    now_ts,
                ])
            bu.consistency_level = ConsistencyLevel.LOCAL_QUORUM
            session.execute(bu, timeout=REQUEST_TIMEOUT_SEC)
            upsert_time = perf_counter() - ust
            total_rows_upserted += 1

            log(
                "  {cid}: rows={rows}, span={f}..{l} ({days} days), "
                "have_any={have_any}, miss_any={miss_any}, "
                "have_price={hp}, miss_price={mp}, "
                "have_vol={hv}, miss_vol={mv}, "
                "have_mcap={hm}, miss_mcap={mm}, "
                "symbol={sym}, name={nm} | scan={scan:.2f}s, upsert={ins:.2f}s".format(
                    cid=coin_id,
                    rows=row_count,
                    f=first_day,
                    l=last_day,
                    days=expected_days,
                    have_any=have_any,
                    miss_any=missing_any,
                    hp=len(price_days), mp=missing_price,
                    hv=len(vol_days),   mv=missing_volume,
                    hm=len(mcap_days),  mm=missing_mcap,
                    sym=symbol or "",
                    nm=name or "",
                    scan=scan_time,
                    ins=upsert_time,
                )
            )

            if WRITE_CSV:
                rows_for_csv.append({
                    "id": coin_id,
                    "symbol": symbol or "",
                    "name": name or "",
                    "first_day": first_day.isoformat(),
                    "last_day": last_day.isoformat(),
                    "expected_days": expected_days,
                    "have_any": have_any,
                    "missing_any": missing_any,
                    "have_price": len(price_days),
                    "missing_price": missing_price,
                    "have_volume": len(vol_days),
                    "missing_volume": missing_volume,
                    "have_mcap": len(mcap_days),
                    "missing_mcap": missing_mcap,
                    "updated_at": now_ts.isoformat(),
                })

            if SLEEP_BETWEEN_COINS_SEC:
                sleep(SLEEP_BETWEEN_COINS_SEC)

        # CSV write at the end
        if WRITE_CSV and rows_for_csv:
            try:
                log(f"Writing CSV ({len(rows_for_csv)} rows) → {CSV_PATH}")
                with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=list(rows_for_csv[0].keys()))
                    writer.writeheader()
                    writer.writerows(rows_for_csv)
                log("CSV write complete.")
            except Exception as e:
                log(f"CSV write failed: {e}")

        elapsed = perf_counter() - start_all
        log(
            "DONE. coins={coins}, upserts={up}, scanned_rows={scanned}, "
            "sum_missing_any={miss}, elapsed={sec:.2f}s".format(
                coins=len(coin_ids),
                up=total_rows_upserted,
                scanned=total_rows_scanned,
                miss=total_missing_any,
                sec=elapsed,
            )
        )

    finally:
        try:
            log("Shutting down cluster…")
            cluster.shutdown()
            log("Cluster shutdown complete.")
        except Exception as e:
            log(f"Cluster shutdown warning: {e}")

if __name__ == "__main__":
    main()
