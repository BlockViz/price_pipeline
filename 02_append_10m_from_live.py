import os, time
from datetime import datetime, timedelta, timezone

from cassandra import OperationTimedOut, ReadTimeout, ReadFailure, WriteTimeout, DriverException
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy
from cassandra.query import SimpleStatement
from dotenv import load_dotenv

load_dotenv()

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

TABLE_LATEST    = os.getenv("TABLE_LATEST", "prices_live")
TABLE_ROLLING   = os.getenv("TABLE_ROLLING", "prices_live_rolling")
TABLE_OUT       = os.getenv("TABLE_OUT", "prices_10m_7d")

if not ASTRA_TOKEN:
    raise SystemExit("Missing ASTRA_TOKEN")

def now_str(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def floor_slot(dt_utc, minutes=SLOT_MINUTES):
    return dt_utc.replace(minute=(dt_utc.minute // minutes) * minutes, second=0, microsecond=0)

def slot_start_now():
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

    SEL_COINS = SimpleStatement(
        f"SELECT id, symbol, name, rank FROM {TABLE_LATEST}", fetch_size=FETCH_SIZE
    )
    SEL_IN_SLOT_PS = s.prepare(
        f"SELECT last_updated, price_usd, market_cap, volume_24h "
        f"FROM {TABLE_ROLLING} WHERE id=? AND last_updated>=? AND last_updated<? LIMIT 1"
    )
    SEL_PREV_PS = s.prepare(
        f"SELECT last_updated, price_usd, market_cap, volume_24h "
        f"FROM {TABLE_ROLLING} WHERE id=? AND last_updated<? LIMIT 1"
    )
    INS_10M_IF_NOT_EXISTS_PS = s.prepare(
        f"INSERT INTO {TABLE_OUT} "
        f"(id, ts, symbol, name, rank, price_usd, market_cap, volume_24h, last_updated) "
        f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS"
    )

    def main():
        coins_rows = list(s.execute(SEL_COINS, timeout=REQUEST_TIMEOUT))
        coins = [r for r in coins_rows if isinstance(r.rank, int) and r.rank > 0]
        coins.sort(key=lambda r: r.rank)
        coins = coins[:TOP_N]
        print(f"[{now_str()}] Processing top {len(coins)} coins")

        slots = last_n_slots_oldest_first(SLOTS_BACKFILL)
        print(f"[{now_str()}] Slots (oldest→newest): {slots[0][0]} .. {slots[-1][1]} (count={len(slots)})")

        wrote = skipped = 0

        for ci, c in enumerate(coins, 1):
            print(f"[{now_str()}] → [{ci}/{len(coins)}] {c.symbol} ({c.id}) rank={c.rank}")
            carry_used = 0

            for si, (start, end) in enumerate(slots, 1):
                print(f"    slot {si}/{len(slots)} {start} → {end}")

                try:
                    in_slot = s.execute(SEL_IN_SLOT_PS, [c.id, start, end], timeout=REQUEST_TIMEOUT).one()
                except (OperationTimedOut, ReadTimeout, ReadFailure) as e:
                    print(f"        [TIMEOUT] in-slot read {c.symbol} {start}→{end}: {e} (skip)"); skipped += 1; continue
                except DriverException as e:
                    print(f"        [ERROR] in-slot read {c.symbol} {start}→{end}: {e} (skip)"); skipped += 1; continue

                if in_slot and in_slot.price_usd is not None:
                    price = float(in_slot.price_usd)
                    mcap  = float(in_slot.market_cap) if in_slot.market_cap is not None else 0.0
                    vol   = float(in_slot.volume_24h) if in_slot.volume_24h is not None else 0.0
                    source = "hist-in-slot"
                else:
                    if carry_used >= ALLOW_CARRY_MAX_SLOTS:
                        print("        carry cap reached → skip"); skipped += 1; continue
                    try:
                        prev = s.execute(SEL_PREV_PS, [c.id, start], timeout=REQUEST_TIMEOUT).one()
                    except (OperationTimedOut, ReadTimeout, ReadFailure) as e:
                        print(f"        [TIMEOUT] prev read {c.symbol} < {start}: {e} (skip)"); skipped += 1; continue
                    except DriverException as e:
                        print(f"        [ERROR] prev read {c.symbol} < {start}: {e} (skip)"); skipped += 1; continue

                    if prev and prev.price_usd is not None:
                        price = float(prev.price_usd)
                        mcap  = float(prev.market_cap) if prev.market_cap is not None else 0.0
                        vol   = float(prev.volume_24h) if prev.volume_24h is not None else 0.0
                        source = "hist-carry"
                        carry_used += 1
                    else:
                        print("        no history for slot (and no previous) → skip")
                        skipped += 1
                        continue

                # clamp last_updated to slot end
                slot_last_upd = end - timedelta(seconds=1)

                try:
                    applied = s.execute(
                        INS_10M_IF_NOT_EXISTS_PS,
                        [c.id, start, c.symbol, c.name, c.rank, price, mcap, vol, slot_last_upd],
                        timeout=REQUEST_TIMEOUT
                    ).one().applied
                    print(f"        insert {'applied' if applied else 'skipped'} "
                          f"({source}, price={price}, mcap={mcap}, vol={vol}, last_upd={slot_last_upd})")
                    if applied: wrote += 1
                    else:       skipped += 1
                except (WriteTimeout, OperationTimedOut) as e:
                    print(f"        [TIMEOUT] insert {c.symbol} {start}: {e} (skip)"); skipped += 1
                except DriverException as e:
                    print(f"        [ERROR] insert {c.symbol} {start}: {e} (skip)"); skipped += 1

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
