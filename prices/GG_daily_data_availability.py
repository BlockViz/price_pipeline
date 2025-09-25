#!/usr/bin/env python3
# file: daily_data_availability.py
# Summarize per-coin daily data availability from gecko_candles_daily_contin
# into keyspace.coin_daily_availability, with detailed progress logging.
# Also pulls `symbol` and `name` from the source and includes them in CSV,
# and (if the summary table has those columns) writes them there too.

from datetime import datetime, timezone, date as dtdate
from time import perf_counter, sleep
import os
from astra_connect.connect import get_session

# ---- config ---------------------------------------------------------------
KEYSPACE = os.getenv("KEYSPACE", "default_keyspace")
SOURCE_TABLE = f"{KEYSPACE}.gecko_candles_daily_contin"
SUMMARY_TABLE = f"{KEYSPACE}.coin_daily_availability"

# Verbosity controls (can override via env)
LOG_EVERY_ROWS = int(os.getenv("LOG_EVERY_ROWS", "10000"))   # log every N rows scanned per coin
LOG_EVERY_COINS = int(os.getenv("LOG_EVERY_COINS", "25"))    # log every N coins processed
SHOW_QUERY_TIMES = os.getenv("SHOW_QUERY_TIMES", "1") not in ("0", "false", "False")
SLEEP_BETWEEN_COINS_SEC = float(os.getenv("SLEEP_BETWEEN_COINS_SEC", "0"))

# "present" rule: treat a field as present iff it's NOT NULL
is_present_price  = lambda v: v is not None
is_present_volume = lambda v: v is not None
is_present_mcap   = lambda v: v is not None

# Optional: write a CSV for quick verification
WRITE_CSV = os.getenv("WRITE_CSV", "1") not in ("0", "false", "False")
CSV_PATH = os.getenv("CSV_PATH", "coin_daily_availability.csv")
# --------------------------------------------------------------------------


def ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{ts()}] {msg}", flush=True)


def to_pydate(d) -> dtdate:
    """
    Normalize Cassandra's Date (or anything date-ish) to a Python datetime.date.
    Falls back to ISO parsing of str(d) which works for cassandra.util.Date ("YYYY-MM-DD").
    """
    if isinstance(d, dtdate):
        return d
    try:
        return dtdate.fromisoformat(str(d))
    except Exception:
        raise TypeError(f"Could not coerce value to date: {d!r} (type={type(d)})")


def main():
    start_all = perf_counter()
    session, cluster = get_session(return_cluster=True)
    try:
        # Prepared statements
        log(f"Preparing statements (source={SOURCE_TABLE}, summary={SUMMARY_TABLE})")
        ps_ids = session.prepare(f"SELECT DISTINCT id FROM {SOURCE_TABLE}")

        # Pull symbol, name in addition to minimal columns; partition-local scan per id
        ps_rows_for_id = session.prepare(
            f"""
            SELECT date, price_usd, volume_24h, market_cap, symbol, name
            FROM {SOURCE_TABLE}
            WHERE id = ?
            """
        )

        # Try an UPSERT that includes symbol/name; if the table lacks these columns, fall back.
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

        # 1) list all coins
        t0 = perf_counter()
        log("Querying distinct coin ids…")
        coin_ids = [row.id for row in session.execute(ps_ids)]
        t1 = perf_counter()
        log(f"Found {len(coin_ids)} coin ids (took {(t1 - t0):.2f}s)")

        # Optional CSV
        rows_for_csv = []
        now_ts = datetime.now(timezone.utc)

        total_rows_scanned = 0
        total_rows_upserted = 0
        total_missing_any = 0

        for i, coin_id in enumerate(coin_ids, 1):
            if i == 1 or i % LOG_EVERY_COINS == 0:
                log(f"[{i}/{len(coin_ids)}] Starting {coin_id}…")

            coin_start = perf_counter()
            dates = []
            have_price = 0
            have_vol = 0
            have_mcap = 0

            # capture symbol/name (first non-null seen)
            symbol = None
            name = None

            qstart = perf_counter()
            rs = session.execute(ps_rows_for_id, [coin_id])
            qtime_first = None
            row_count = 0
            for row in rs:
                if qtime_first is None:
                    qtime_first = perf_counter() - qstart
                    if SHOW_QUERY_TIMES:
                        log(f"  {coin_id}: time to first row {qtime_first:.2f}s")

                d = to_pydate(row.date)
                dates.append(d)
                row_count += 1

                if symbol is None and getattr(row, "symbol", None):
                    symbol = row.symbol
                if name is None and getattr(row, "name", None):
                    name = row.name

                if is_present_price(row.price_usd):
                    have_price += 1
                if is_present_volume(row.volume_24h):
                    have_vol += 1
                if is_present_mcap(row.market_cap):
                    have_mcap += 1

                if LOG_EVERY_ROWS > 0 and (row_count % LOG_EVERY_ROWS == 0):
                    log(f"  {coin_id}: scanned {row_count} rows so far…")

            scan_time = perf_counter() - coin_start
            total_rows_scanned += row_count

            if not dates:
                log(f"  {coin_id}: no rows (skipping). Scan time {scan_time:.2f}s")
                if SLEEP_BETWEEN_COINS_SEC:
                    sleep(SLEEP_BETWEEN_COINS_SEC)
                continue

            first_day = min(dates)
            last_day  = max(dates)
            have_any  = len(dates)

            expected_days = (last_day - first_day).days + 1

            missing_any    = max(0, expected_days - have_any)
            missing_price  = max(0, expected_days - have_price)
            missing_volume = max(0, expected_days - have_vol)
            missing_mcap   = max(0, expected_days - have_mcap)

            total_missing_any += missing_any

            # 3) upsert summary row
            ust = perf_counter()
            if upsert_includes_meta:
                session.execute(
                    ps_upsert,
                    [
                        coin_id,
                        first_day, last_day, expected_days,
                        have_any, missing_any,
                        have_price, missing_price,
                        have_vol, missing_volume,
                        have_mcap, missing_mcap,
                        symbol, name,
                        now_ts,
                    ],
                )
            else:
                session.execute(
                    ps_upsert,
                    [
                        coin_id,
                        first_day, last_day, expected_days,
                        have_any, missing_any,
                        have_price, missing_price,
                        have_vol, missing_volume,
                        have_mcap, missing_mcap,
                        now_ts,
                    ],
                )
            upsert_time = perf_counter() - ust
            total_rows_upserted += 1

            log(
                "  {cid}: rows={rows}, span={f}..{l} ({days} days), "
                "have_any={have_any}, miss_any={miss_any}, "
                "symbol={sym}, name={nm} | scan={scan:.2f}s, upsert={ins:.2f}s".format(
                    cid=coin_id,
                    rows=row_count,
                    f=first_day,
                    l=last_day,
                    days=expected_days,
                    have_any=have_any,
                    miss_any=missing_any,
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
                    "have_price": have_price,
                    "missing_price": missing_price,
                    "have_volume": have_vol,
                    "missing_volume": missing_volume,
                    "have_mcap": have_mcap,
                    "missing_mcap": missing_mcap,
                    "updated_at": now_ts.isoformat(),
                })

            if SLEEP_BETWEEN_COINS_SEC:
                sleep(SLEEP_BETWEEN_COINS_SEC)

        # CSV write at the end
        if WRITE_CSV and rows_for_csv:
            import csv
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
