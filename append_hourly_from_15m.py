import os
from datetime import datetime, timedelta, timezone
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from cassandra import OperationTimedOut, ReadTimeout, Unavailable
from dotenv import load_dotenv

load_dotenv()

BUNDLE = os.getenv("ASTRA_BUNDLE_PATH", "secure-connect.zip")
ASTRA_TOKEN = os.getenv("ASTRA_TOKEN")
KEYSPACE = os.getenv("ASTRA_KEYSPACE", "default_keyspace")
TOP_N = int(os.getenv("TOP_N", "110"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SEC", "30"))

if not ASTRA_TOKEN:
    raise SystemExit("Missing ASTRA_TOKEN")

auth = PlainTextAuthProvider("token", ASTRA_TOKEN)
cluster = Cluster(cloud={"secure_connect_bundle": BUNDLE}, auth_provider=auth)
s = cluster.connect(KEYSPACE)

# Top coins
SEL_LIVE = SimpleStatement("""
  SELECT id, symbol, name, rank FROM prices_live
""")

# 15m points within the target hour
SEL_15M_HOUR = s.prepare("""
  SELECT ts, price_usd, market_cap, volume_24h
  FROM prices_15m_7d
  WHERE id=? AND ts>=? AND ts<?
""")

# Idempotent insert
INS_IF_NOT_EXISTS = s.prepare("""
  INSERT INTO candles_hourly_30d
    (id, ts, open, high, low, close, volume, marketcap, source)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
  IF NOT EXISTS
""")

def prev_hour_bounds_utc() -> tuple[datetime, datetime]:
    now = datetime.now(timezone.utc)
    this_hour = now.replace(minute=0, second=0, microsecond=0)
    start = this_hour - timedelta(hours=1)
    end = this_hour
    return start, end

def main():
    start, end = prev_hour_bounds_utc()
    coins = list(s.execute(SEL_LIVE, timeout=REQUEST_TIMEOUT))
    coins = [c for c in coins if isinstance(c.rank, int) and c.rank > 0]
    coins.sort(key=lambda c: c.rank)
    coins = coins[:TOP_N]

    wrote = skipped = empty = errors = 0

    for c in coins:
        try:
            pts = s.execute(SEL_15M_HOUR, [c.id, start, end], timeout=REQUEST_TIMEOUT)
            prices, mcaps, vols = [], [], []
            for p in pts:
                if p.price_usd is not None: prices.append(float(p.price_usd))
                if p.market_cap is not None: mcaps.append(float(p.market_cap))
                if p.volume_24h is not None: vols.append(float(p.volume_24h))

            if not prices:
                empty += 1
                continue

            o = prices[0]
            h = max(prices)
            l = min(prices)
            cl = prices[-1]
            mcap = mcaps[-1] if mcaps else None
            vol  = vols[-1] if vols else None

            res = s.execute(INS_IF_NOT_EXISTS,
                            [c.id, start, o, h, l, cl, vol, mcap, "15m_derived"],
                            timeout=REQUEST_TIMEOUT)
            # LWT result: was_applied True if inserted, False if already there
            if getattr(res.one(), "applied", False):
                wrote += 1
            else:
                skipped += 1

        except (OperationTimedOut, ReadTimeout, Unavailable) as e:
            errors += 1
            print(f"[warn] {c.id} hour {start.isoformat()} timeout: {e}")
        except Exception as e:
            errors += 1
            print(f"[warn] {c.id} hour {start.isoformat()} failed: {e}")

    print(f"[hourly] {start.isoformat()} → wrote={wrote}, already={skipped}, empty={empty}, errors={errors}")

if __name__ == "__main__":
    try:
        main()
    finally:
        cluster.shutdown()
