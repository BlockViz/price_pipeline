import os
import time
from datetime import datetime, timedelta, timezone

from cassandra import OperationTimedOut, ReadTimeout, ReadFailure, WriteTimeout, DriverException
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy
from cassandra.query import SimpleStatement
from dotenv import load_dotenv

load_dotenv()

# ---- Config ----
BUNDLE = os.getenv("ASTRA_BUNDLE_PATH", "secure-connect.zip")
ASTRA_TOKEN = os.getenv("ASTRA_TOKEN")
KEYSPACE = os.getenv("ASTRA_KEYSPACE", "default_keyspace")
TOP_N = int(os.getenv("TOP_N", "110"))

# Request & connection timeouts (seconds)
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SEC", "30"))
CONNECT_TIMEOUT = int(os.getenv("CONNECT_TIMEOUT_SEC", "15"))

# slotting/backfill
SLOT_DELAY_SEC = int(os.getenv("SLOT_DELAY_SEC", "120"))  # 2 min guard
SLOTS_BACKFILL = int(os.getenv("SLOTS_BACKFILL", "8"))    # last 8 slots (~2h)

# fetch paging (keep small so a single page can't block forever)
FETCH_SIZE = int(os.getenv("FETCH_SIZE", "500"))

if not ASTRA_TOKEN:
    raise SystemExit("Missing ASTRA_TOKEN")

def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print(f"[{now_str()}] Booting…")
print(f"[{now_str()}] Connecting to Astra (bundle='{BUNDLE}', keyspace='{KEYSPACE}')")

auth = PlainTextAuthProvider("token", ASTRA_TOKEN)

# Enforce a real default request timeout via an ExecutionProfile
exec_profile = ExecutionProfile(
    load_balancing_policy=RoundRobinPolicy(),
    request_timeout=REQUEST_TIMEOUT,
)

cluster = Cluster(
    cloud={"secure_connect_bundle": BUNDLE},
    auth_provider=auth,
    execution_profiles={EXEC_PROFILE_DEFAULT: exec_profile},
    connect_timeout=CONNECT_TIMEOUT,
)

try:
    s = cluster.connect(KEYSPACE)
    print(f"[{now_str()}] Connected to keyspace: {KEYSPACE}")

    # --- Statements ---
    SEL_LIVE = SimpleStatement("""
      SELECT id, symbol, name, rank, price_usd, market_cap, volume_24h, last_updated
      FROM prices_live
    """)
    # Ensure live read also pages sanely
    SEL_LIVE.fetch_size = FETCH_SIZE

    SEL_15M_RANGE_PS = s.prepare("""
      SELECT ts, price_usd, market_cap, volume_24h
      FROM prices_15m_7d
      WHERE id=? AND ts>=? AND ts<?
    """)

    SEL_PREV_POINT_PS = s.prepare("""
      SELECT ts, price_usd, market_cap, volume_24h
      FROM prices_15m_7d
      WHERE id=? AND ts<? LIMIT 1
    """)

    INS_15M_IF_NOT_EXISTS_PS = s.prepare("""
      INSERT INTO prices_15m_7d
        (id, ts, symbol, name, rank, price_usd, market_cap, volume_24h, last_updated)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      IF NOT EXISTS
    """)

    # --- time helpers ---
    def floor_15m(dt_utc):
        return dt_utc.replace(minute=(dt_utc.minute // 15) * 15, second=0, microsecond=0)

    def slot_start_now():
        now_ = datetime.now(timezone.utc) - timedelta(seconds=SLOT_DELAY_SEC)
        return floor_15m(now_)

    def last_n_slots(n):
        end = slot_start_now() + timedelta(minutes=15)  # end-exclusive of most recent slot
        for _ in range(n):
            start = end - timedelta(minutes=15)
            yield (start, end)
            end = start

    # --- main ---
    def main():
        print(f"[{now_str()}] Fetching live coins… (timeout={REQUEST_TIMEOUT}s)")
        t0 = time.time()
        coins_rows = list(s.execute(SEL_LIVE, timeout=REQUEST_TIMEOUT))
        print(f"[{now_str()}] Got {len(coins_rows)} rows from prices_live in {time.time()-t0:.2f}s")

        coins = [r for r in coins_rows if isinstance(r.rank, int) and r.rank > 0]
        coins.sort(key=lambda r: r.rank)
        coins = coins[:TOP_N]
        print(f"[{now_str()}] Processing top {len(coins)} coins (TOP_N={TOP_N})")

        slots = list(last_n_slots(SLOTS_BACKFILL))
        print(f"[{now_str()}] Slots to backfill: {SLOTS_BACKFILL} → " +
              f"{slots[0][0]}..{slots[-1][1]} (oldest first)")

        wrote = skipped = empty = 0

        for ci, c in enumerate(coins, 1):
            print(f"[{now_str()}] → [{ci}/{len(coins)}] {c.symbol} ({c.id}) rank={c.rank}")
            for si, (start, end) in enumerate(slots, 1):
                print(f"    slot {si}/{len(slots)} {start} → {end}")
                # --- range read with strict paging & timeout ---
                try:
                    bound = SEL_15M_RANGE_PS.bind([c.id, start, end])
                    bound.fetch_size = FETCH_SIZE
                    t_read = time.time()
                    pts_rows = list(s.execute(bound, timeout=REQUEST_TIMEOUT))
                    dt_read = time.time() - t_read
                    print(f"        read ok: {len(pts_rows)} rows in {dt_read:.2f}s")
                except (OperationTimedOut, ReadTimeout, ReadFailure) as e:
                    print(f"        [TIMEOUT] range read {c.symbol} {start}→{end}: {e} (skipping slot)")
                    skipped += 1
                    continue
                except DriverException as e:
                    print(f"        [ERROR] range read {c.symbol} {start}→{end}: {e} (skipping slot)")
                    skipped += 1
                    continue

                prices, mcaps, vols = [], [], []
                for p in pts_rows:
                    if p.price_usd is not None:
                        prices.append(float(p.price_usd))
                    if p.market_cap is not None:
                        mcaps.append(float(p.market_cap))
                    if p.volume_24h is not None:
                        vols.append(float(p.volume_24h))

                if prices:
                    price = prices[-1]
                    mcap  = mcaps[-1] if mcaps else None
                    vol   = vols[-1] if vols else None
                else:
                    # carry-forward previous
                    try:
                        prev_bound = SEL_PREV_POINT_PS.bind([c.id, start])
                        prev_bound.fetch_size = 1
                        t_prev = time.time()
                        prev = s.execute(prev_bound, timeout=REQUEST_TIMEOUT).one()
                        print(f"        prev-point lookup in {time.time()-t_prev:.2f}s "
                              f"{'hit' if prev else 'miss'}")
                    except (OperationTimedOut, ReadTimeout, ReadFailure) as e:
                        print(f"        [TIMEOUT] prev read {c.symbol} < {start}: {e} (using live fallback)")
                        prev = None
                    except DriverException as e:
                        print(f"        [ERROR] prev read {c.symbol} < {start}: {e} (using live fallback)")
                        prev = None

                    if prev and prev.price_usd is not None:
                        price = float(prev.price_usd)
                        mcap  = float(prev.market_cap) if prev.market_cap is not None else None
                        vol   = float(prev.volume_24h) if prev.volume_24h is not None else None
                    else:
                        # very first slot for this coin
                        price = float(c.price_usd or 0.0)
                        mcap  = float(c.market_cap or 0.0)
                        vol   = float(c.volume_24h or 0.0)
                        empty += 1

                last_upd = end - timedelta(seconds=1)

                # --- insert with timeout ---
                try:
                    t_ins = time.time()
                    applied = s.execute(
                        INS_15M_IF_NOT_EXISTS_PS.bind(
                            [c.id, start, c.symbol, c.name, c.rank, price, mcap, vol, last_upd]
                        ),
                        timeout=REQUEST_TIMEOUT
                    ).one().applied
                    print(f"        insert {'applied' if applied else 'skipped'} in {time.time()-t_ins:.2f}s")
                    if applied:
                        wrote += 1
                    else:
                        skipped += 1
                except (WriteTimeout, OperationTimedOut) as e:
                    print(f"        [TIMEOUT] insert {c.symbol} {start}: {e} (count as skipped)")
                    skipped += 1
                except DriverException as e:
                    print(f"        [ERROR] insert {c.symbol} {start}: {e} (count as skipped)")
                    skipped += 1

        print(f"[{now_str()}] [15m] slots={SLOTS_BACKFILL} wrote={wrote} skipped={skipped} empty={empty}")

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
