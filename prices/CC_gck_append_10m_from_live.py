#!/usr/bin/env python3
# prices/02_append_10m_from_live.py
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

# Ensure consistent CWD (so relative files like secure-connect.zip work)
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
BUNDLE         = os.getenv("ASTRA_BUNDLE_PATH", "secure-connect.zip")
ASTRA_TOKEN    = os.getenv("ASTRA_TOKEN")
KEYSPACE       = os.getenv("ASTRA_KEYSPACE", "default_keyspace")
TOP_N          = int(os.getenv("TOP_N", "200"))

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SEC", "30"))
CONNECT_TIMEOUT = int(os.getenv("CONNECT_TIMEOUT_SEC", "15"))
FETCH_SIZE      = int(os.getenv("FETCH_SIZE", "500"))

SLOT_MINUTES    = int(os.getenv("SLOT_MINUTES", "10"))
SLOT_DELAY_SEC  = int(os.getenv("SLOT_DELAY_SEC", "120"))
SLOTS_BACKFILL  = int(os.getenv("SLOTS_BACKFILL", "12"))       # ~2h
ALLOW_CARRY_MAX_SLOTS = int(os.getenv("ALLOW_CARRY_MAX_SLOTS", "1"))

# Gecko tables (defaults match your new schema)
TABLE_LATEST    = os.getenv("TABLE_LATEST", "gecko_prices_live")
TABLE_ROLLING   = os.getenv("TABLE_ROLLING", "gecko_prices_live_rolling")
TABLE_OUT       = os.getenv("TABLE_OUT", "gecko_prices_10m_7d")
TABLE_MCAP_OUT  = os.getenv("TABLE_MCAP_10M", "gecko_market_cap_10m_7d")

if not ASTRA_TOKEN:
    raise SystemExit("Missing ASTRA_TOKEN")

def now_str(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def floor_slot(dt_utc, minutes=SLOT_MINUTES):
    return dt_utc.replace(minute=(dt_utc.minute // minutes) * minutes, second=0, microsecond=0)

def slot_start_now():
    # slight delay so we don't cut into an actively filling slot
    now_ = datetime.now(timezone.utc) - timedelta(seconds=SLOT_DELAY_SEC)
    return floor_slot(now_)

def last_n_slots_oldest_first(n):
    end = slot_start_now() + timedelta(minutes=SLOT_MINUTES)
    slots = []
    for _ in range(n):
        start = end - timedelta(minutes=SLOT_MINUTES)
        slots.append((start, end))
        end = start
    slots.reverse()
    return slots

print(f"[{now_str()}] Booting…")
print(f"[{now_str()}] Connecting to Astra (bundle='{BUNDLE}', keyspace='{KEYSPACE}')")

auth = PlainTextAuthProvider("token", ASTRA_TOKEN)
exec_profile = ExecutionProfile(load_balancing_policy=RoundRobinPolicy(), request_timeout=REQUEST_TIMEOUT)

cluster = Cluster(
    cloud={"secure_connect_bundle": BUNDLE},
    auth_provider=auth,
    execution_profiles={EXEC_PROFILE_DEFAULT: exec_profile},
    connect_timeout=CONNECT_TIMEOUT,
)

try:
    s = cluster.connect(KEYSPACE)
    print(f"[{now_str()}] Connected.")

    # gecko live table uses market_cap_rank
    SEL_COINS = SimpleStatement(
        f"SELECT id, symbol, name, market_cap_rank, category FROM {TABLE_LATEST}",
        fetch_size=FETCH_SIZE
    )

    # Latest point within the slot (rolling is clustered DESC on last_updated)
    SEL_IN_SLOT_PS = s.prepare(
        f"SELECT last_updated, price_usd, market_cap, volume_24h, "
        f"       market_cap_rank, circulating_supply, total_supply "
        f"FROM {TABLE_ROLLING} "
        f"WHERE id=? AND last_updated>=? AND last_updated<? LIMIT 1"
    )

    # Carry: latest point before the slot start
    SEL_PREV_PS = s.prepare(
        f"SELECT last_updated, price_usd, market_cap, volume_24h, "
        f"       market_cap_rank, circulating_supply, total_supply "
        f"FROM {TABLE_ROLLING} "
        f"WHERE id=? AND last_updated<? LIMIT 1"
    )

    # Insert into 10m table — includes symbol & name + new fields
    INS_10M_IF_NOT_EXISTS_PS = s.prepare(
        f"INSERT INTO {TABLE_OUT} "
        f"(id, ts, symbol, name, price_usd, market_cap, volume_24h, "
        f" market_cap_rank, circulating_supply, total_supply, last_updated) "
        f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS"
    )

    INS_MCAP_10M_UPSERT = s.prepare(
        f"INSERT INTO {TABLE_MCAP_OUT} (category, ts, id, name, symbol, market_cap, market_cap_rank, volume_24h, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )

    def main():
        coins_rows = list(s.execute(SEL_COINS, timeout=REQUEST_TIMEOUT))
        # keep only ranked coins with positive rank
        coins = [r for r in coins_rows if isinstance(r.market_cap_rank, int) and r.market_cap_rank > 0]
        coins.sort(key=lambda r: r.market_cap_rank)
        coins = coins[:TOP_N]
        print(f"[{now_str()}] Processing top {len(coins)} coins from {TABLE_LATEST}")

        slots = last_n_slots_oldest_first(SLOTS_BACKFILL)
        if slots:
            print(f"[{now_str()}] Slots (oldest→newest): {slots[0][0]} .. {slots[-1][1]} (count={len(slots)})")

        slot_totals = {}

        def bump_slot_total(slot_start, category, mcap_value, vol_value, last_upd) -> None:
            key = (slot_start, category)
            entry = slot_totals.setdefault(key, {"market_cap": 0.0, "volume_24h": 0.0, "last_updated": last_upd})
            entry["market_cap"] += mcap_value
            entry["volume_24h"] += vol_value
            if last_upd and (entry["last_updated"] is None or last_upd > entry["last_updated"]):
                entry["last_updated"] = last_upd

        wrote = skipped = 0

        for ci, c in enumerate(coins, 1):
            sym = getattr(c, "symbol", None)
            name = getattr(c, "name", None)
            mkr = getattr(c, "market_cap_rank", None)
            print(f"[{now_str()}] → [{ci}/{len(coins)}] {sym} ({c.id}) rank={mkr}")
            coin_category = (getattr(c, 'category', None) or 'Other').strip() or 'Other'
            carry_used = 0

            for si, (start, end) in enumerate(slots, 1):
                print(f"    slot {si}/{len(slots)} {start} → {end}")

                try:
                    in_slot = s.execute(SEL_IN_SLOT_PS, [c.id, start, end], timeout=REQUEST_TIMEOUT).one()
                except (OperationTimedOut, ReadTimeout, ReadFailure) as e:
                    print(f"        [TIMEOUT] in-slot read {sym} {start}→{end}: {e} (skip)"); skipped += 1; continue
                except DriverException as e:
                    print(f"        [ERROR] in-slot read {sym} {start}→{end}: {e} (skip)"); skipped += 1; continue

                if in_slot and in_slot.price_usd is not None:
                    price = float(in_slot.price_usd)
                    mcap  = float(in_slot.market_cap) if in_slot.market_cap is not None else 0.0
                    vol   = float(in_slot.volume_24h) if in_slot.volume_24h is not None else 0.0
                    rank  = int(in_slot.market_cap_rank) if in_slot.market_cap_rank is not None else None
                    circ  = float(in_slot.circulating_supply) if in_slot.circulating_supply is not None else None
                    totl  = float(in_slot.total_supply) if in_slot.total_supply is not None else None
                    source = "hist-in-slot"
                else:
                    if carry_used >= ALLOW_CARRY_MAX_SLOTS:
                        print("        carry cap reached → skip"); skipped += 1; continue
                    try:
                        prev = s.execute(SEL_PREV_PS, [c.id, start], timeout=REQUEST_TIMEOUT).one()
                    except (OperationTimedOut, ReadTimeout, ReadFailure) as e:
                        print(f"        [TIMEOUT] prev read {sym} < {start}: {e} (skip)"); skipped += 1; continue
                    except DriverException as e:
                        print(f"        [ERROR] prev read {sym} < {start}: {e} (skip)"); skipped += 1; continue

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
                    result = s.execute(
                        INS_10M_IF_NOT_EXISTS_PS,
                        [c.id, start, sym, name, price, mcap, vol, rank, circ, totl, slot_last_upd],
                        timeout=REQUEST_TIMEOUT
                    ).one()
                    applied = bool(getattr(result, 'applied', True)) if result is not None else True
                    bump_slot_total(start, coin_category, mcap, vol, slot_last_upd)
                    bump_slot_total(start, 'ALL', mcap, vol, slot_last_upd)
                    print(f"        insert {'applied' if applied else 'skipped'} "
                          f"({source}, price={price}, mcap={mcap}, vol={vol}, rank={rank}, circ={circ}, totl={totl}, last_upd={slot_last_upd})")
                    if applied: wrote += 1
                    else:       skipped += 1
                except (WriteTimeout, OperationTimedOut) as e:
                    print(f"        [TIMEOUT] insert {sym} {start}: {e} (skip)"); skipped += 1
                except DriverException as e:
                    print(f"        [ERROR] insert {sym} {start}: {e} (skip)"); skipped += 1

        if slot_totals:
            print(f"[{now_str()}] [mcap-10m] writing {len(slot_totals)} aggregates into {TABLE_MCAP_OUT}")
            agg_written = 0
            for (slot_start, category), totals in sorted(slot_totals.items(), key=lambda kv: (kv[0][0], 0 if kv[0][1] == 'ALL' else 1, kv[0][1].lower())):
                last_upd = totals.get('last_updated') or (slot_start + timedelta(minutes=SLOT_MINUTES) - timedelta(seconds=1))
                cat_id = f"CATEGORY::{category}"
                display_name = 'All Categories' if category == 'ALL' else category
                symbol_val = 'ALL' if category == 'ALL' else category.upper().replace(' ', '_')
                rank_value = 0 if category == 'ALL' else None
                try:
                    s.execute(
                        INS_MCAP_10M_UPSERT,
                        [category, slot_start, cat_id, display_name, symbol_val, totals['market_cap'], rank_value, totals['volume_24h'], last_upd],
                        timeout=REQUEST_TIMEOUT
                    )
                    agg_written += 1
                except (WriteTimeout, OperationTimedOut, DriverException) as e:
                    print(f"        [mcap-10m] insert failed for category='{category}' slot={slot_start}: {e}")
            print(f"[{now_str()}] [mcap-10m] rows_written={agg_written}")
        else:
            print(f"[{now_str()}] [mcap-10m] no aggregates captured (coins={len(coins)})")

        print(f"[{now_str()}] [10m] wrote={wrote} skipped={skipped}")

    if __name__ == "__main__":
        try:
            main()
        except KeyboardInterrupt:
            print(f"\n[{now_str()}] KeyboardInterrupt received. Cleaning up…")

finally:
    print(f"[{now_str()}] Shutting down Cassandra connection…")
    try:
        cluster.shutdown()
    except Exception as e:
        print(f"[{now_str()}] Error during shutdown: {e}")
    print(f"[{now_str()}] Done.")
