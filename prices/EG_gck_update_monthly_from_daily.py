#!/usr/bin/env python3
# Update gecko_candles_monthly using daily candles + live price for current month close

import os
from datetime import datetime, date, timedelta, timezone
from typing import Any, Dict, Iterable, Optional, Tuple

from astra_connect.connect import get_session, AstraConfig
from cassandra.query import SimpleStatement

AstraConfig.from_env()  # load .env or process env

TOP_N = int(os.getenv("TOP_N", "300"))
FETCH_SIZE = int(os.getenv("FETCH_SIZE", "500"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SEC", "40"))
PROGRESS_EVERY = int(os.getenv("PROGRESS_EVERY", "20"))

KS = os.getenv("KEYSPACE", os.getenv("ASTRA_KEYSPACE", "default_keyspace")).strip()
TABLE_DAILY = f"{KS}.gecko_candles_daily_contin"
TABLE_MONTHLY = f"{KS}.gecko_candles_monthly"
TABLE_LIVE = f"{KS}.gecko_prices_live"
FINALIZE_GRACE_DAYS = int(os.getenv("FINALIZE_GRACE_DAYS", "1"))  # only try on day 1 by default

def should_finalize_prev_month(today: date) -> bool:
    # day 1 only (or widen to first N days if you want)
    return today.day <= FINALIZE_GRACE_DAYS



def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def ym_tag(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def next_month_start(d: date) -> date:
    return date(d.year + (1 if d.month == 12 else 0), 1 if d.month == 12 else d.month + 1, 1)

def is_month_final(session, sel_ps, coin_id: str, ym: str) -> bool:
    try:
        row = session.execute(sel_ps, [coin_id, ym], timeout=REQUEST_TIMEOUT).one()
        if not row:
            return False
        return getattr(row, "candle_source", None) == "daily_final" and getattr(row, "close", None) is not None
    except Exception:
        return False

def sanitize_num(x: Any, fallback: Optional[float] = None) -> Optional[float]:
    try:
        if x is None:
            return fallback
        return float(x)
    except Exception:
        return fallback


def equalish(a: Any, b: Any, tol: float = 1e-12) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    try:
        return abs(float(a) - float(b)) <= tol
    except Exception:
        return False


def aggregate_month(rows: Iterable[Any]) -> Optional[Dict[str, Any]]:
    """Aggregate a single month from ascending daily rows."""
    rows_sorted = sorted(rows, key=lambda r: getattr(r, "date", date.min))
    if not rows_sorted:
        return None

    agg: Dict[str, Any] = {
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
    }

    for r in rows_sorted:
        price_open = sanitize_num(getattr(r, "open", None))
        price_close = sanitize_num(getattr(r, "close", None))
        price_fallback = sanitize_num(getattr(r, "price_usd", None))
        price_for_open = price_open if price_open is not None else (price_close if price_close is not None else price_fallback)
        price_for_high = sanitize_num(getattr(r, "high", None), price_close if price_close is not None else price_fallback)
        price_for_low = sanitize_num(getattr(r, "low", None), price_close if price_close is not None else price_fallback)
        price_for_close = price_close if price_close is not None else (price_fallback if price_fallback is not None else price_for_open)

        if agg["open"] is None and price_for_open is not None:
            agg["open"] = price_for_open
        if price_for_high is not None:
            agg["high"] = price_for_high if agg["high"] is None else max(agg["high"], price_for_high)
        if price_for_low is not None:
            agg["low"] = price_for_low if agg["low"] is None else min(agg["low"], price_for_low)
        if price_for_close is not None:
            agg["close"] = price_for_close

        agg["volume"] += sanitize_num(getattr(r, "volume_24h", None), 0.0) or 0.0

        mc = sanitize_num(getattr(r, "market_cap", None))
        if mc is not None:
            agg["market_cap"] = mc
        rnk = getattr(r, "market_cap_rank", None)
        if rnk is not None:
            try:
                agg["market_cap_rank"] = int(rnk)
            except Exception:
                pass
        circ = sanitize_num(getattr(r, "circulating_supply", None))
        if circ is not None:
            agg["circulating_supply"] = circ
        tot = sanitize_num(getattr(r, "total_supply", None))
        if tot is not None:
            agg["total_supply"] = tot

        lu = getattr(r, "last_updated", None)
        if isinstance(lu, datetime) and lu.tzinfo is None:
            lu = lu.replace(tzinfo=timezone.utc)
        if lu is not None and (agg["last_updated"] is None or lu > agg["last_updated"]):
            agg["last_updated"] = lu

    if agg["open"] is None or agg["close"] is None:
        return None
    return agg


def apply_live_to_partial_month(agg: Dict[str, Any], live: Any) -> Dict[str, Any]:
    """Adjust current-month aggregate so close reflects the freshest live price."""
    out = dict(agg)
    live_price = sanitize_num(getattr(live, "price_usd", None))
    live_vol = sanitize_num(getattr(live, "volume_24h", None))
    live_mcap = sanitize_num(getattr(live, "market_cap", None))
    live_rank = getattr(live, "market_cap_rank", None)
    live_circ = sanitize_num(getattr(live, "circulating_supply", None))
    live_tot = sanitize_num(getattr(live, "total_supply", None))
    live_lu = getattr(live, "last_updated", None)
    if isinstance(live_lu, datetime) and live_lu.tzinfo is None:
        live_lu = live_lu.replace(tzinfo=timezone.utc)

    if live_price is not None:
        if out.get("open") is None:
            out["open"] = live_price
        out["close"] = live_price
        if out.get("high") is None or live_price > out["high"]:
            out["high"] = live_price
        if out.get("low") is None or live_price < out["low"]:
            out["low"] = live_price

    if (out.get("volume") is None or out["volume"] == 0.0) and live_vol is not None:
        out["volume"] = live_vol
    if out.get("market_cap") is None and live_mcap is not None:
        out["market_cap"] = live_mcap
    if out.get("market_cap_rank") is None and live_rank is not None:
        try:
            out["market_cap_rank"] = int(live_rank)
        except Exception:
            pass
    if out.get("circulating_supply") is None and live_circ is not None:
        out["circulating_supply"] = live_circ
    if out.get("total_supply") is None and live_tot is not None:
        out["total_supply"] = live_tot

    if live_lu is not None and (out.get("last_updated") is None or live_lu > out["last_updated"]):
        out["last_updated"] = live_lu

    return out


def write_monthly_row(session, ins_ps, sel_ps, coin, ym: str, agg: Dict[str, Any], candle_source: str, default_last_upd: datetime) -> bool:
    existing = None
    try:
        existing = session.execute(sel_ps, [coin.id, ym], timeout=REQUEST_TIMEOUT).one()
    except Exception:
        existing = None

    last_upd = agg.get("last_updated") or default_last_upd

    if existing:
        unchanged = all(
            [
                equalish(getattr(existing, "open", None), agg.get("open")),
                equalish(getattr(existing, "high", None), agg.get("high")),
                equalish(getattr(existing, "low", None), agg.get("low")),
                equalish(getattr(existing, "close", None), agg.get("close")),
                equalish(getattr(existing, "volume", None), agg.get("volume")),
                equalish(getattr(existing, "market_cap", None), agg.get("market_cap")),
                getattr(existing, "market_cap_rank", None) == agg.get("market_cap_rank"),
                getattr(existing, "candle_source", None) == candle_source,
            ]
        )
        if unchanged:
            return False

    session.execute(
        ins_ps,
        [
            coin.id,
            ym,
            getattr(coin, "symbol", None),
            getattr(coin, "name", None),
            agg.get("open"),
            agg.get("high"),
            agg.get("low"),
            agg.get("close"),
            float(agg.get("volume", 0.0) or 0.0),
            agg.get("market_cap"),
            agg.get("market_cap_rank"),
            agg.get("circulating_supply"),
            agg.get("total_supply"),
            candle_source,
            last_upd,
        ],
        timeout=REQUEST_TIMEOUT,
    )
    return True


def main():
    print(f"[{now_str()}] Connecting to Astra…")
    session, cluster = get_session(return_cluster=True)
    print(f"[{now_str()}] Connected. keyspace='{session.keyspace}'")

    sel_live = SimpleStatement(
        f"SELECT id, symbol, name, market_cap_rank, price_usd, market_cap, volume_24h, "
        f"circulating_supply, total_supply, last_updated "
        f"FROM {TABLE_LIVE}",
        fetch_size=FETCH_SIZE,
    )

    sel_daily_range = session.prepare(
        f"SELECT date, open, high, low, close, price_usd, volume_24h, market_cap, market_cap_rank, "
        f"circulating_supply, total_supply, last_updated "
        f"FROM {TABLE_DAILY} WHERE id=? AND date>=? AND date<?"
    )

    sel_monthly_one = session.prepare(
        f"SELECT open, high, low, close, volume, market_cap, market_cap_rank, candle_source "
        f"FROM {TABLE_MONTHLY} WHERE id=? AND year_month=? LIMIT 1"
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
    print(f"[{now_str()}] Processing top {len(coins)} coins (TOP_N={TOP_N})")

    now_utc = datetime.now(timezone.utc)
    today = now_utc.date()
    current_month_start = date(today.year, today.month, 1)
    prev_month_end = current_month_start - timedelta(days=1)
    prev_month_start = date(prev_month_end.year, prev_month_end.month, 1)
    next_month_start_dt = next_month_start(current_month_start)
    finalize_prev = should_finalize_prev_month(today)

    wrote = errors = unchanged = 0

    for idx, coin in enumerate(coins, 1):
        if idx == 1 or idx % PROGRESS_EVERY == 0 or idx == len(coins):
            print(f"[{now_str()}] → Coin {idx}/{len(coins)}: {getattr(coin, 'symbol', '?')} ({getattr(coin, 'id', '?')})")

        try:
            rows_curr = list(
                session.execute(
                    sel_daily_range, [coin.id, current_month_start, next_month_start_dt], timeout=REQUEST_TIMEOUT
                )
            )
        except Exception as e:
            errors += 1
            print(f"[{now_str()}] [READ-ERR-CURR] {coin.id}: {e}")
            continue

        # Finalize previous month (only near month boundary, and only if not already final)
        if finalize_prev:
            prev_ym = ym_tag(prev_month_start)

            if not is_month_final(session, sel_monthly_one, coin.id, prev_ym):
                try:
                    rows_prev = list(
                        session.execute(
                            sel_daily_range, [coin.id, prev_month_start, current_month_start], timeout=REQUEST_TIMEOUT
                        )
                    )
                except Exception as e:
                    errors += 1
                    print(f"[{now_str()}] [READ-ERR-PREV] {coin.id}: {e}")
                    rows_prev = []

                agg_prev = aggregate_month(rows_prev)
                if agg_prev:
                    prev_month_end_dt = datetime(
                        prev_month_end.year, prev_month_end.month, prev_month_end.day, 23, 59, 59, tzinfo=timezone.utc
                    )
                    try:
                        changed = write_monthly_row(
                            session, ins_monthly, sel_monthly_one, coin, prev_ym, agg_prev, "daily_final", prev_month_end_dt
                        )
                        wrote += int(changed)
                        unchanged += int(not changed)
                    except Exception as e:
                        errors += 1
                        print(f"[{now_str()}] [WRITE-ERR] {coin.id} {prev_ym}: {e}")

        # Current month (partial + live close)
        agg_curr = aggregate_month(rows_curr) or {
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
        }
        agg_curr = apply_live_to_partial_month(agg_curr, coin)
        if agg_curr.get("open") is not None and agg_curr.get("close") is not None:
            curr_ym = ym_tag(current_month_start)
            month_end_dt = datetime(
                next_month_start_dt.year, next_month_start_dt.month, next_month_start_dt.day, tzinfo=timezone.utc
            ) - timedelta(seconds=1)
            try:
                changed = write_monthly_row(
                    session,
                    ins_monthly,
                    sel_monthly_one,
                    coin,
                    curr_ym,
                    agg_curr,
                    "daily_partial_live",
                    month_end_dt,
                )
                wrote += int(changed)
                unchanged += int(not changed)
            except Exception as e:
                errors += 1
                print(f"[{now_str()}] [WRITE-ERR] {coin.id} {curr_ym}: {e}")

    print(f"[{now_str()}] Done. wrote={wrote} unchanged={unchanged} errors={errors}")
    try:
        cluster.shutdown()
    except Exception as e:
        print(f"[{now_str()}] Shutdown warning: {e}")


if __name__ == "__main__":
    main()
