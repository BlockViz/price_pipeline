import os
from datetime import datetime, timezone, timedelta
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, SimpleStatement
from cassandra import ConsistencyLevel
from dotenv import load_dotenv

load_dotenv()

BUNDLE = os.getenv("ASTRA_BUNDLE_PATH", "secure-connect.zip")
ASTRA_TOKEN = os.getenv("ASTRA_TOKEN")
KEYSPACE = os.getenv("ASTRA_KEYSPACE", "default_keyspace")

if not ASTRA_TOKEN:
    raise SystemExit("Missing ASTRA_TOKEN")

auth = PlainTextAuthProvider("token", ASTRA_TOKEN)
cluster = Cluster(cloud={"secure_connect_bundle": BUNDLE}, auth_provider=auth)
session = cluster.connect(KEYSPACE)

SEL_LAST = session.prepare("SELECT ts FROM prices_15m_7d WHERE id = ? LIMIT 1")
INS = session.prepare("""
  INSERT INTO prices_15m_7d (id, ts, symbol, name, rank, price_usd, market_cap, volume_24h, last_updated)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

def main():
    # read current snapshot
    rows = session.execute(SimpleStatement(
        "SELECT id, symbol, name, rank, price_usd, market_cap, volume_24h, last_fetched, last_updated FROM prices_live",
        fetch_size=500
    ))

    inserted = 0; skipped = 0
    for r in rows:
        new_ts = (r.last_fetched or datetime.now(timezone.utc)).replace(microsecond=0)
        # check latest stored ts for this id
        prev = session.execute(SEL_LAST, [r.id]).one()
        if prev and prev.ts and (new_ts - prev.ts) < timedelta(minutes=10):
            skipped += 1
            continue

        session.execute(INS, (
            r.id, new_ts, r.symbol, r.name, r.rank,
            float(r.price_usd or 0.0),
            float(r.market_cap or 0.0),
            float(r.volume_24h or 0.0),
            r.last_updated
        ))
        inserted += 1

    print(f"15m append: inserted={inserted}, skipped={skipped}")

if __name__ == "__main__":
    try:
        main()
    finally:
        cluster.shutdown()
