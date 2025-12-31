#!/usr/bin/env python3
# prices/CC_gck_append_10m_from_live.py
#
# Append 10-minute slots into:
#   - gecko_prices_10m_7d      (IF NOT EXISTS; from rolling or carry)
#   - gecko_market_cap_10m_7d  (category aggregates per slot; upsert with ranks)
#
# Reads from:
#   - gecko_prices_live         (for the list of assets, incl. category)
#   - gecko_prices_live_rolling (for latest points per slot / carry)
#
# Notes:
# - Uses shared Astra connector (astra_connect.connect).
# - Works with .env locally or pure env vars in CI/CD.
# - “Carry” uses the latest point before the slot if the slot is empty,
#   capped by ALLOW_CARRY_MAX_SLOTS consecutive slots.

import os, sys, traceback
from datetime import datetime, timedelta, timezone
from typing import Tuple, List, Dict, Any

# ───────────────────────── Astra connector ─────────────────────────
from astra_connect.connect import get_session, AstraConfig
AstraConfig.from_env()

# ───────────────────────── Config ─────────────────────────
TOP_N              = int(os.getenv("TOP_N", "300"))

REQUEST_TIMEOUT    = int(os.getenv("REQUEST_TIMEOUT_SEC", "30"))
FETCH_SIZE         = int(os.getenv("FETCH_SIZE", "500"))

SLOT_MINUTES       = int(os.getenv("SLOT_MINUTES", "10"))
SLOT_DELAY_SEC     = int(os.getenv("SLOT_DELAY_SEC", "120"))
SLOTS_BACKFILL     = int(os.getenv("SLOTS_BACKFILL", 4))  # default: 6h safety window
ALLOW_CARRY_MAX_SLOTS = int(os.getenv("ALLOW_CARRY_MAX_SLOTS", 4))  # allow carry across the 6h window

# ───────────────────────── Optional gapfill mode ─────────────────────────
# Designed for hourly/2-hourly runs (NOT every 6 minutes).
GAPFILL_ENABLED     = os.getenv("GAPFILL_ENABLED", "0") == "1"
GAPFILL_HOURS       = int(os.getenv("GAPFILL_HOURS", "12"))
# If 1, attempt a once-per-hour lock row in Cassandra to avoid duplicate gapfill runs.
GAPFILL_LOCK_ENABLED = os.getenv("GAPFILL_LOCK_ENABLED", "1") == "1"
GAPFILL_LOCK_JOB     = os.getenv("GAPFILL_LOCK_JOB", "append_10m_gapfill").strip() or "append_10m_gapfill"
# If 0 (default), do NOT write category aggregates in gapfill mode (avoids incorrect partial totals).
GAPFILL_WRITE_AGG    = os.getenv("GAPFILL_WRITE_AGG", "0") == "1"

# Tables (defaults match your schema)
TABLE_LATEST       = os.getenv("TABLE_LATEST", "gecko_prices_live")
TABLE_ROLLING      = os.getenv("TABLE_ROLLING", "gecko_prices_live_rolling")
TABLE_OUT          = os.getenv("TABLE_OUT", "gecko_prices_10m_7d")
TABLE_MCAP_OUT     = os.getenv("TABLE_MCAP_10M", "gecko_market_cap_10m_7d")

def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def to_utc(x: datetime) -> datetime:
    if x.tzinfo is None:
        return x.replace(tzinfo=timezone.utc)
    return x.astimezone(timezone.utc)

def floor_slot(dt_utc: datetime, minutes: int = SLOT_MINUTES) -> datetime:
    dt_utc = to_utc(dt_utc)
    return dt_utc.replace(minute=(dt_utc.minute // minutes) * minutes, second=0, microsecond=0)

def slot_start_now() -> datetime:
    now_ = datetime.now(timezone.utc) - timedelta(seconds=SLOT_DELAY_SEC)
    return floor_slot(now_)

def last_n_slots_oldest_first(n: int) -> List[Tuple[datetime, datetime]]:
    end = slot_start_now() + timedelta(minutes=SLOT_MINUTES)
    slots: List[Tuple[datetime, datetime]] = []
    for _ in range(n):
        start = end - timedelta(minutes=SLOT_MINUTES)
        slots.append((start, end))
        end = start
    slots.reverse()
    return slots

def expected_slots_for_last_hours(hours: int) -> List[Tuple[datetime, datetime]]:
    """
    Build contiguous slot windows covering the last N hours up to 'now' (delayed by SLOT_DELAY_SEC),
    in chronological order.
    """
    end = slot_start_now() + timedelta(minutes=SLOT_MINUTES)
    start = floor_slot(end - timedelta(hours=hours))
    slots: List[Tuple[datetime, datetime]] = []
    cur = start
    while cur < end:
        nxt = cur + timedelta(minutes=SLOT_MINUTES)
        slots.append((cur, nxt))
        cur = nxt
    return slots

def hour_bucket(dt_utc: datetime) -> str:
    dt_utc = to_utc(dt_utc)
    return dt_utc.strftime("%Y-%m-%dT%H")

# ───────────────────────── Session & prepared statements ─────────────────────────
print(f"[{now_str()}] Connecting to Astra…")
session, cluster = get_session(return_cluster=True)
print(f"[{now_str()}] Connected. keyspace='{session.keyspace}'")

from cassandra.query import SimpleStatement

SEL_COINS = SimpleStatement(
    f"SELECT id, symbol, name, market_cap_rank, category FROM {TABLE_LATEST}",
    fetch_size=FETCH_SIZE
)

# Latest point within the slot (rolling is clustered on last_updated)
SEL_IN_SLOT_PS = session.prepare(
    f"""
    SELECT last_updated, price_usd, market_cap, volume_24h,
           market_cap_rank, circulating_supply, total_supply
    FROM {TABLE_ROLLING}
    WHERE id=? AND last_updated>=? AND last_updated<? LIMIT 1
    """
)

# Carry: latest point before the slot start
SEL_PREV_PS = session.prepare(
    f"""
    SELECT last_updated, price_usd, market_cap, volume_24h,
           market_cap_rank, circulating_supply, total_supply
    FROM {TABLE_ROLLING}
    WHERE id=? AND last_updated<? LIMIT 1
    """
)

# Insert into 10m table — IF NOT EXISTS
INS_10M_IF_NOT_EXISTS_PS = session.prepare(
    f"""
    INSERT INTO {TABLE_OUT}
      (id, ts, symbol, name, price_usd, market_cap, volume_24h,
       market_cap_rank, circulating_supply, total_supply, last_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS
    """
)

# Existing 10m slots for an id in a time window (used in gapfill mode)
SEL_10M_EXISTING_TS_RANGE_PS = session.prepare(
    f"""
    SELECT ts
    FROM {TABLE_OUT}
    WHERE id=? AND ts>=? AND ts<?
    """
)

# Aggregates: gecko_market_cap_10m_7d(category, ts, last_updated, market_cap, market_cap_rank, volume_24h)
INS_MCAP_10M_UPSERT = session.prepare(
    f"""
    INSERT INTO {TABLE_MCAP_OUT}
      (category, ts, last_updated, market_cap, market_cap_rank, volume_24h)
    VALUES (?, ?, ?, ?, ?, ?)
    """
)

if GAPFILL_ENABLED and GAPFILL_LOCK_ENABLED:
    # Lightweight, safe schema init (IF NOT EXISTS) for the hourly lock.
    # If you prefer not to do schema DDL here, you can create this table once manually and remove this block.
    session.execute(
        f"""
        CREATE TABLE IF NOT EXISTS job_locks (
            job text,
            bucket text,
            created_at timestamp,
            PRIMARY KEY (job, bucket)
        )
        """
    )
    INS_LOCK_IF_NOT_EXISTS_PS = session.prepare(
        "INSERT INTO job_locks (job, bucket, created_at) VALUES (?, ?, ?) IF NOT EXISTS"
    )

def try_acquire_gapfill_lock() -> bool:
    """
    Returns True if gapfill should run now (lock acquired or lock disabled).
    """
    if not GAPFILL_ENABLED:
        return False
    if not GAPFILL_LOCK_ENABLED:
        return True
    bucket = hour_bucket(datetime.now(timezone.utc))
    try:
        res = session.execute(
            INS_LOCK_IF_NOT_EXISTS_PS,
            [GAPFILL_LOCK_JOB, bucket, datetime.now(timezone.utc)],
            timeout=REQUEST_TIMEOUT
        ).one()
        applied = bool(getattr(res, "applied", True)) if res is not None else True
        if not applied:
            print(f"[{now_str()}] [gapfill] lock already held for bucket={bucket} job={GAPFILL_LOCK_JOB} → skip gapfill")
        return applied
    except Exception as e:
        print(f"[{now_str()}] [gapfill][WARN] lock failed ({e}) → skip gapfill for safety")
        return False

def compute_category_ranks(cat_totals: Dict[str, Dict[str, float]]) -> Dict[str, int]:
    """
    Input: cat_totals[category] = {'market_cap': float, 'volume_24h': float, 'last_updated': datetime}
    Output: ranks per category (ALL=0; others 1..N by market_cap desc; ties stable)
    """
    items = [(cat, float(vals.get('market_cap', 0.0))) for cat, vals in cat_totals.items() if cat != "ALL"]
    items.sort(key=lambda x: x[1], reverse=True)
    ranks: Dict[str, int] = {cat: i + 1 for i, (cat, _m) in enumerate(items)}
    ranks["ALL"] = 0
    return ranks

def ensure_all_bucket(catmap: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Rebuild ALL from the other categories to avoid persisting a stale/partial ALL.
    last_updated = max of categories.
    """
    non_all = [(cat, vals) for cat, vals in catmap.items() if cat != "ALL"]
    if not non_all:
        return catmap

    latest_lu = None
    total_mcap = 0.0
    total_vol = 0.0
    for _cat, vals in non_all:
        lu = vals.get("last_updated")
        if lu is not None and (latest_lu is None or lu > latest_lu):
            latest_lu = lu
        total_mcap += float(vals.get("market_cap") or 0.0)
        total_vol += float(vals.get("volume_24h") or 0.0)

    catmap["ALL"] = {
        "market_cap": total_mcap,
        "volume_24h": total_vol,
        "last_updated": latest_lu,
    }
    return catmap

# ───────────────────────── Main logic ─────────────────────────
def main():
    # Decide run mode early (append is always allowed; gapfill is optional)
    run_gapfill = try_acquire_gapfill_lock()
    run_mode = "gapfill" if run_gapfill else "append"

    # Pick coins
    coins_rows = list(session.execute(SEL_COINS, timeout=REQUEST_TIMEOUT))
    coins = [r for r in coins_rows if isinstance(r.market_cap_rank, int) and r.market_cap_rank > 0]
    coins.sort(key=lambda r: r.market_cap_rank)
    coins = coins[:TOP_N]
    print(f"[{now_str()}] Mode={run_mode} | Processing top {len(coins)} coins from {TABLE_LATEST}")

    if run_mode == "gapfill":
        slots = expected_slots_for_last_hours(GAPFILL_HOURS)
    else:
        slots = last_n_slots_oldest_first(SLOTS_BACKFILL)
    if slots:
        print(f"[{now_str()}] Slots (oldest→newest): {slots[0][0]} .. {slots[-1][1]} (count={len(slots)})")

    # slot_totals[slot_start][category] = {'market_cap': float, 'volume_24h': float, 'last_updated': datetime}
    slot_totals: Dict[datetime, Dict[str, Dict[str, Any]]] = {}

    def bump_slot_total(slot_start: datetime, category: str, mcap_value: float, vol_value: float, last_upd: datetime) -> None:
        catmap = slot_totals.setdefault(slot_start, {})
        entry = catmap.setdefault(category, {"market_cap": 0.0, "volume_24h": 0.0, "last_updated": last_upd})
        entry["market_cap"] += float(mcap_value or 0.0)
        entry["volume_24h"] += float(vol_value or 0.0)
        if last_upd and (entry["last_updated"] is None or last_upd > entry["last_updated"]):
            entry["last_updated"] = last_upd

    wrote = skipped = 0

    for ci, c in enumerate(coins, 1):
        sym = getattr(c, "symbol", None)
        name = getattr(c, "name", None)
        mkr = getattr(c, "market_cap_rank", None)
        coin_id = getattr(c, "id")
        print(f"[{now_str()}] → [{ci}/{len(coins)}] {sym} ({coin_id}) rank={mkr}")
        coin_category = (getattr(c, 'category', None) or 'Other').strip() or 'Other'
        carry_used = 0
        # In gapfill mode, discover which 10m slots already exist for this coin in the window.
        existing_ts: set[datetime] = set()
        if run_mode == "gapfill" and slots:
            w_start = slots[0][0]
            w_end   = slots[-1][1]
            try:
                rs = session.execute(
                    SEL_10M_EXISTING_TS_RANGE_PS,
                    [coin_id, w_start, w_end],
                    timeout=REQUEST_TIMEOUT
                )
                for r in rs:
                    ts0 = getattr(r, "ts", None)
                    if isinstance(ts0, datetime):
                        existing_ts.add(to_utc(ts0))
                print(f"[{now_str()}]    [gapfill] existing_10m_in_window={len(existing_ts)} "
                      f"window={w_start}..{w_end}")
            except Exception as e:
                print(f"[{now_str()}]    [gapfill][WARN] failed to read existing 10m range: {e}")
                existing_ts = set()

        for si, (start, end) in enumerate(slots, 1):
            # print each slot with minimal noise
            print(f"    slot {si}/{len(slots)} {start} → {end}")

            # gapfill mode: skip if this slot already exists
            if run_mode == "gapfill":
                if start in existing_ts:
                    print("        exists → skip")
                    skipped += 1
                    continue

            # try to read a row *inside* the slot window
            try:
                in_slot = session.execute(SEL_IN_SLOT_PS, [coin_id, start, end], timeout=REQUEST_TIMEOUT).one()
            except Exception as e:
                print(f"        [READ-ERR] in-slot {sym} {start}→{end}: {e} (skip)"); skipped += 1; continue

            if in_slot and in_slot.price_usd is not None:
                price = float(in_slot.price_usd)
                mcap  = float(in_slot.market_cap) if in_slot.market_cap is not None else 0.0
                vol   = float(in_slot.volume_24h) if in_slot.volume_24h is not None else 0.0
                rank  = int(in_slot.market_cap_rank) if in_slot.market_cap_rank is not None else None
                circ  = float(in_slot.circulating_supply) if in_slot.circulating_supply is not None else None
                totl  = float(in_slot.total_supply) if in_slot.total_supply is not None else None
                source = "hist-in-slot"
                # reset carry streak when we have a true in-slot point
                carry_used = 0
            else:
                if carry_used >= ALLOW_CARRY_MAX_SLOTS:
                    print("        carry cap reached → skip"); skipped += 1; continue
                # otherwise carry: grab latest point *before* slot start
                try:
                    prev = session.execute(SEL_PREV_PS, [coin_id, start], timeout=REQUEST_TIMEOUT).one()
                except Exception as e:
                    print(f"        [READ-ERR] prev {sym} < {start}: {e} (skip)"); skipped += 1; continue

                if prev and prev.price_usd is not None:
                    price = float(prev.price_usd)
                    mcap  = float(prev.market_cap) if prev.market_cap is not None else 0.0
                    vol   = float(prev.volume_24h) if prev.volume_24h is not None else 0.0
                    rank  = int(prev.market_cap_rank) if prev.market_cap_rank is not None else None
                    circ  = float(prev.circulating_supply) if prev.circulating_supply is not None else None
                    totl  = float(prev.total_supply) if prev.total_supply is not None else None
                    source = "hist-carry"
                    carry_used += 1
                else:
                    print("        no history for slot (and no previous) → skip")
                    skipped += 1
                    continue

            # clamp last_updated to slot end (represents slot's EoS)
            slot_last_upd = end - timedelta(seconds=1)

            try:
                result = session.execute(
                    INS_10M_IF_NOT_EXISTS_PS,
                    [coin_id, start, sym, name, price, mcap, vol, rank, circ, totl, slot_last_upd],
                    timeout=REQUEST_TIMEOUT
                ).one()
                applied = bool(getattr(result, 'applied', True)) if result is not None else True

                # accumulate aggregates only in normal append mode, OR if explicitly enabled for gapfill
                if (run_mode != "gapfill") or GAPFILL_WRITE_AGG:
                    bump_slot_total(start, coin_category, mcap, vol, slot_last_upd)
                    bump_slot_total(start, 'ALL',          mcap, vol, slot_last_upd)

                print(f"        insert {'applied' if applied else 'skipped'} "
                      f"({source}, price={price}, mcap={mcap}, vol={vol}, rank={rank}, circ={circ}, totl={totl}, last_upd={slot_last_upd})")

                if applied: wrote += 1
                else:       skipped += 1

            except Exception as e:
                print(f"        [WRITE-ERR] insert {sym} {start}: {e} (skip)")
                traceback.print_exc()
                skipped += 1

    # Write category aggregates (with ranks)
    if slot_totals and ((run_mode != "gapfill") or GAPFILL_WRITE_AGG):
        print(f"[{now_str()}] [mcap-10m] writing aggregates for {len(slot_totals)} slots into {TABLE_MCAP_OUT}")
        agg_written = 0
        for slot_start in sorted(slot_totals.keys()):
            catmap = slot_totals[slot_start]  # Dict[str, {market_cap, volume_24h, last_updated}]
            catmap = ensure_all_bucket(catmap)
            # compute ranks for this slot (ALL=0; others by market_cap desc)
            ranks = compute_category_ranks(catmap)
            # write in defined order: ALL first, then alphabetical for determinism
            for category in sorted(catmap.keys(), key=lambda c: (0 if c == "ALL" else 1, c.lower())):
                totals = catmap[category]
                last_upd = totals.get('last_updated') or (slot_start + timedelta(minutes=SLOT_MINUTES) - timedelta(seconds=1))
                rank_value = ranks.get(category, None)
                try:
                    session.execute(
                        INS_MCAP_10M_UPSERT,
                        [category, slot_start, last_upd, float(totals['market_cap']), rank_value, float(totals['volume_24h'])],
                        timeout=REQUEST_TIMEOUT
                    )
                    agg_written += 1
                except Exception as e:
                    print(f"        [mcap-10m] insert failed for category='{category}' slot={slot_start}: {e}")
        print(f"[{now_str()}] [mcap-10m] rows_written={agg_written}")
    else:
        if run_mode == "gapfill" and not GAPFILL_WRITE_AGG:
            print(f"[{now_str()}] [mcap-10m] skipped aggregates (gapfill mode; GAPFILL_WRITE_AGG=0)")
        else:
            print(f"[{now_str()}] [mcap-10m] no aggregates captured (coins={len(coins)})")

    print(f"[{now_str()}] [10m] wrote={wrote} skipped={skipped}")

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
