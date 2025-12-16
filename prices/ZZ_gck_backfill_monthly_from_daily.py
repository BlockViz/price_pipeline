#!/usr/bin/env python3
# One-time backfill: build gecko_candles_monthly from gecko_candles_daily_contin

import os
from time import perf_counter
from datetime import datetime, timezone, date, timedelta
from typing import Dict, Any, Optional, Iterable, Tuple

from astra_connect.connect import get_session, AstraConfig
from cassandra.query import SimpleStatement

AstraConfig.from_env()  # load .env or process env

TOP_N = int(os.getenv("TOP_N", "200"))
FETCH_SIZE = int(os.getenv("FETCH_SIZE", "1000"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SEC", "40"))
PROGRESS_EVERY = int(os.getenv("PROGRESS_EVERY", "20"))
VERBOSE = os.getenv("VERBOSE", "0") == "1"
HEARTBEAT_SEC = float(os.getenv("HEARTBEAT_SEC", "15"))

KS = os.getenv("KEYSPACE", os.getenv("ASTRA_KEYSPACE", "default_keyspace")).strip()
TABLE_DAILY = f"{KS}.gecko_candles_daily_contin"
TABLE_MONTHLY = f"{KS}.gecko_candles_monthly"
TABLE_LIVE = f"{KS}.gecko_prices_live"


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{now_str()}] {msg}", flush=True)


def ym_tag(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def month_bounds(d: date) -> Tuple[datetime, datetime]:
    start = datetime(d.year, d.month, 1, tzinfo=timezone.utc)
    if d.month == 12:
        nxt = datetime(d.year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        nxt = datetime(d.year, d.month + 1, 1, tzinfo=timezone.utc)
    return start, nxt


def sanitize_num(x: Any, fallback: Optional[float] = None) -> Optional[float]:
    try:
        if x is None:
            return fallback
        return float(x)
    except Exception:
        return fallback


def to_pydate(d) -> Optional[date]:
    """Best-effort convert Cassandra Date/py datetime/date into python date."""
    if d is None:
        return None
    if isinstance(d, date) and not isinstance(d, datetime):
        return d
    if isinstance(d, datetime):
        return d.date()
    try:
        return d.date()  # Cassandra util Date
    except Exception:
        pass
    try:
        return date.fromisoformat(str(d)[:10])
    except Exception:
        return None


def aggregate_months(rows: Iterable[Any]) -> Dict[str, Dict[str, Any]]:
    """Return map year_month -> aggregated candle from ascending daily rows."""
    months: Dict[str, Dict[str, Any]] = {}
    for r in sorted(rows, key=lambda rr: getattr(rr, "date", date.min)):
        d_raw = getattr(r, "date", None)
        d = to_pydate(d_raw)
        if d is None:
            continue
        ym = ym_tag(d)
        bucket = months.setdefault(
            ym,
            {
                "open": None,
                "high": None,
                "low": None,
                "close": None,
                "volume": 0.0,
                "market_cap": None,
                "market_cap_rank": None,
                "circulating_supply": None,
                "total_supply": None,
                "last_updated": None,
                "symbol": None,
                "name": None,
            },
        )

        # Price candidates
        price_open = sanitize_num(getattr(r, "open", None))
        price_close = sanitize_num(getattr(r, "close", None))
        price_fallback = sanitize_num(getattr(r, "price_usd", None))
        price_for_open = price_open if price_open is not None else (price_close if price_close is not None else price_fallback)
        price_for_highlow = sanitize_num(getattr(r, "high", None), price_close if price_close is not None else price_fallback)
        price_for_low = sanitize_num(getattr(r, "low", None), price_close if price_close is not None else price_fallback)
        price_for_close = price_close if price_close is not None else (price_fallback if price_fallback is not None else price_for_open)

        if bucket["open"] is None and price_for_open is not None:
            bucket["open"] = price_for_open
        if price_for_highlow is not None:
            bucket["high"] = price_for_highlow if bucket["high"] is None else max(bucket["high"], price_for_highlow)
        if price_for_low is not None:
            bucket["low"] = price_for_low if bucket["low"] is None else min(bucket["low"], price_for_low)
        if price_for_close is not None:
            bucket["close"] = price_for_close

        bucket["volume"] += sanitize_num(getattr(r, "volume_24h", None), 0.0) or 0.0

        mc = sanitize_num(getattr(r, "market_cap", None))
        if mc is not None:
            bucket["market_cap"] = mc
        rnk = getattr(r, "market_cap_rank", None)
        if rnk is not None:
            try:
                bucket["market_cap_rank"] = int(rnk)
            except Exception:
                pass
        circ = sanitize_num(getattr(r, "circulating_supply", None))
        if circ is not None:
            bucket["circulating_supply"] = circ
        tot = sanitize_num(getattr(r, "total_supply", None))
        if tot is not None:
            bucket["total_supply"] = tot

        lu = getattr(r, "last_updated", None)
        if lu is not None:
            if isinstance(lu, datetime) and lu.tzinfo is None:
                lu = lu.replace(tzinfo=timezone.utc)
            if bucket["last_updated"] is None or (lu and lu > bucket["last_updated"]):
                bucket["last_updated"] = lu

        if bucket["symbol"] is None:
            bucket["symbol"] = getattr(r, "symbol", None)
        if bucket["name"] is None:
            bucket["name"] = getattr(r, "name", None)

    return months


def main():
    log("Connecting to Astra…")
    session, cluster = get_session(return_cluster=True)
    log(f"Connected. keyspace='{session.keyspace}'")

    sel_live = SimpleStatement(
        f"SELECT id, symbol, name, market_cap_rank FROM {TABLE_LIVE}",
        fetch_size=FETCH_SIZE,
    )
    sel_daily = session.prepare(
        f"SELECT date, open, high, low, close, price_usd, volume_24h, market_cap, market_cap_rank, "
        f"circulating_supply, total_supply, last_updated, symbol, name "
        f"FROM {TABLE_DAILY} WHERE id=?"
    )
    ins_monthly = session.prepare(
        f"INSERT INTO {TABLE_MONTHLY} "
        f"(id, year_month, symbol, name, open, high, low, close, volume, "
        f" market_cap, market_cap_rank, circulating_supply, total_supply, candle_source, last_updated) "
        f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )

    coins = list(session.execute(sel_live, timeout=REQUEST_TIMEOUT))
    coins = [c for c in coins if isinstance(getattr(c, "market_cap_rank", None), int) and c.market_cap_rank > 0]
    coins.sort(key=lambda r: r.market_cap_rank)
    coins = coins[:TOP_N]
    log(f"Loaded top {len(coins)} coins (TOP_N={TOP_N})")

    wrote = 0
    start_ts = perf_counter()
    last_hb = [start_ts]

    def heartbeat(tag: str = "") -> None:
        now = perf_counter()
        if (now - last_hb[0]) >= HEARTBEAT_SEC:
            last_hb[0] = now
            log(f"[hb] {tag} still working (coins_done={idx-1 if 'idx' in locals() else 0} of {len(coins)}, wrote={wrote})")

    for idx, coin in enumerate(coins, 1):
        if idx == 1 or idx % PROGRESS_EVERY == 0 or idx == len(coins):
            log(f"→ Coin {idx}/{len(coins)}: {getattr(coin, 'symbol', '?')} ({getattr(coin, 'id', '?')})")

        try:
            daily_rows = list(session.execute(sel_daily, [coin.id], timeout=REQUEST_TIMEOUT))
        except Exception as e:
            log(f"[READ-ERR] {coin.id}: {e}")
            continue

        if VERBOSE:
            log(f"  rows={len(daily_rows)}")
        month_aggs = aggregate_months(daily_rows)
        if VERBOSE:
            log(f"  months_built={len(month_aggs)}")
        for ym, agg in sorted(month_aggs.items()):
            if agg["open"] is None or agg["close"] is None:
                continue  # skip empty months

            start_dt, end_dt = month_bounds(date(int(ym[:4]), int(ym[5:]), 1))
            last_upd = agg["last_updated"] or (end_dt - timedelta(seconds=1))
            try:
                session.execute(
                    ins_monthly,
                    [
                        coin.id,
                        ym,
                        agg.get("symbol") or getattr(coin, "symbol", None),
                        agg.get("name") or getattr(coin, "name", None),
                        agg["open"],
                        agg["high"],
                        agg["low"],
                        agg["close"],
                        float(agg.get("volume", 0.0)),
                        agg.get("market_cap"),
                        agg.get("market_cap_rank"),
                        agg.get("circulating_supply"),
                        agg.get("total_supply"),
                        "daily_backfill",
                        last_upd,
                    ],
                    timeout=REQUEST_TIMEOUT,
                )
                wrote += 1
                if VERBOSE:
                    log(f"    wrote {ym} O={agg['open']} H={agg['high']} L={agg['low']} C={agg['close']} V={agg['volume']}")
            except Exception as e:
                log(f"[WRITE-ERR] {coin.id} {ym}: {e}")

        heartbeat(tag=getattr(coin, "symbol", coin.id))

    elapsed = perf_counter() - start_ts
    log(f"Done. wrote={wrote} elapsed={elapsed:.1f}s")
    try:
        cluster.shutdown()
    except Exception as e:
        log(f"Shutdown warning: {e}")


if __name__ == "__main__":
    main()
