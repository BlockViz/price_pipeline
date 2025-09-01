import os
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

# ---- Time gate BEFORE any imports that connect to Astra ----
GRACE_HOURS = os.getenv("GRACE_HOURS", "15,16")  # Berlin hours allowed to run
ALLOWED = {int(h.strip()) for h in GRACE_HOURS.split(",") if h.strip().isdigit()}

berlin = ZoneInfo("Europe/Berlin")
now_berlin = datetime.now(berlin)

if now_berlin.hour not in ALLOWED:
    print(f"Daily append skipped early: it's {now_berlin.isoformat()} (allowed hours Berlin: {sorted(ALLOWED)}).")
    raise SystemExit(0)

# ---- Only now import Cassandra and connect ----
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement

BUNDLE = os.getenv("ASTRA_BUNDLE_PATH", "secure-connect.zip")
ASTRA_TOKEN = os.getenv("ASTRA_TOKEN")
KEYSPACE = os.getenv("ASTRA_KEYSPACE", "default_keyspace")
if not ASTRA_TOKEN:
    raise SystemExit("Missing ASTRA_TOKEN")

auth = PlainTextAuthProvider("token", ASTRA_TOKEN)
cluster = Cluster(cloud={"secure_connect_bundle": BUNDLE}, auth_provider=auth)
session = cluster.connect(KEYSPACE)

SEL_EXISTS = session.prepare("SELECT id FROM prices_daily WHERE id=? AND date=? LIMIT 1")
INS = session.prepare("""
  INSERT INTO prices_daily (id, date, symbol, name, rank, price_usd, market_cap, volume_24h, last_updated)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

def main():
    target_date = now_berlin.date()

    rows = session.execute(SimpleStatement(
        "SELECT id, symbol, name, rank, price_usd, market_cap, volume_24h, last_updated FROM prices_live",
        fetch_size=500
    ))

    inserted = skipped = 0
    for r in rows:
        if session.execute(SEL_EXISTS, [r.id, target_date]).one():
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

    print(f"Daily append ({now_berlin.strftime('%Y-%m-%d %H:%M %Z')}): inserted={inserted}, skipped={skipped}, date={target_date}")

if __name__ == "__main__":
    try:
        main()
    finally:
        cluster.shutdown()
