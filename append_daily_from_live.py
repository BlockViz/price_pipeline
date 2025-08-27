import os
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
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

# We’ll check existence with a SELECT by (id, date) and only INSERT if missing.
SEL_EXISTS = session.prepare("SELECT id FROM prices_daily WHERE id = ? AND date = ? LIMIT 1")
INS = session.prepare("""
  INSERT INTO prices_daily (id, date, symbol, name, rank, price_usd, market_cap, volume_24h, last_updated)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

def main():
    berlin = ZoneInfo("Europe/Berlin")
    now_berlin = datetime.now(berlin)
    if now_berlin.hour != 15:
        print(f"Daily append skipped: it is {now_berlin.isoformat()} (run only at 15:00 Europe/Berlin).")
        return

    # Use the 'date' for Berlin’s local date at 15:00
    target_date = now_berlin.date()

    rows = session.execute(SimpleStatement(
        "SELECT id, symbol, name, rank, price_usd, market_cap, volume_24h, last_updated FROM prices_live",
        fetch_size=500
    ))

    inserted = 0; skipped = 0
    for r in rows:
        # check if already present
        exists = session.execute(SEL_EXISTS, [r.id, target_date]).one()
        if exists:
            skipped += 1
            continue
        session.execute(INS, (
            r.id, target_date, r.symbol, r.name, r.rank,
            float(r.price_usd or 0.0),
            float(r.market_cap or 0.0),
            float(r.volume_24h or 0.0),
            r.last_updated
        ))
        inserted += 1

    print(f"Daily append (15:00 Europe/Berlin): inserted={inserted}, skipped={skipped}, date={target_date}")

if __name__ == "__main__":
    try:
        main()
    finally:
        cluster.shutdown()
