import os, time, requests
from datetime import datetime, timedelta, timezone
from dateutil import parser
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv
load_dotenv()


ASTRA_HOST = os.getenv("ASTRA_HOST")
ASTRA_PORT = int(os.getenv("ASTRA_PORT", "29042"))
ASTRA_CLIENT_ID = os.getenv("ASTRA_CLIENT_ID")
ASTRA_CLIENT_SECRET = os.getenv("ASTRA_CLIENT_SECRET")
KEYSPACE = os.getenv("ASTRA_KEYSPACE", "default_keyspace")
ASSETS = [a.strip() for a in os.getenv("ASSETS", "btc-bitcoin,eth-ethereum").split(",")]
DAILY_START = os.getenv("DAILY_START", "2017-01-01")

for k in ["ASTRA_HOST","ASTRA_PORT","ASTRA_CLIENT_ID","ASTRA_CLIENT_SECRET","ASTRA_KEYSPACE"]:
    v = os.getenv(k)
    if not v:
        raise SystemExit(f"Missing env var {k}. Check your .env or GitHub Secrets.")
if "://" in os.getenv("ASTRA_HOST",""):
    raise SystemExit("ASTRA_HOST must be a hostname only (no https://). Use the CQL API host.")


auth = PlainTextAuthProvider(username=ASTRA_CLIENT_ID, password=ASTRA_CLIENT_SECRET)

# TLS with certifi CA bundle
ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
ssl_context.load_verify_locations(cafile=certifi.where())
ssl_context.check_hostname = True
ssl_context.verify_mode = ssl.CERT_REQUIRED

print(f"Connecting to {ASTRA_HOST}:{ASTRA_PORT} with TLS …")
cluster = Cluster(
    [ASTRA_HOST],
    port=ASTRA_PORT,
    auth_provider=auth_provider,
    ssl_context=ssl_context,
    protocol_version=4
)
session = cluster.connect(KEYSPACE)

ins_hourly = session.prepare("""
INSERT INTO prices_hourly_7d
(asset, ts, open, high, low, close, volume, marketcap)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""")
ins_daily = session.prepare("""
INSERT INTO prices_daily
(asset, date, open, high, low, close, volume, marketcap)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""")

def paprika_hist(coin_id: str, start_iso: str, end_iso: str, interval: str):
    url = f"https://api.coinpaprika.com/v1/tickers/{coin_id}/historical"
    params = dict(start=start_iso, end=end_iso, interval=interval)
    r = requests.get(url, params=params, timeout=30)
    if r.status_code == 429:
        time.sleep(2)
        r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def f(x): return float(x) if x is not None else 0.0

def upsert_hourly_last_7d(coin_id: str):
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=7)
    data = paprika_hist(coin_id, start.isoformat(), now.isoformat(), "1h")
    for row in data:
        ts = parser.isoparse(row["time_open"])
        session.execute(ins_hourly, (
            coin_id, ts,
            f(row.get("open")), f(row.get("high")), f(row.get("low")), f(row.get("close")),
            f(row.get("volume")), f(row.get("market_cap")),
        ))

def upsert_daily_append_only(coin_id: str):
    now = datetime.now(timezone.utc)
    data = paprika_hist(coin_id, f"{DAILY_START}T00:00:00Z", now.isoformat(), "1d")
    for row in data:
        d = parser.isoparse(row["time_open"]).date()
        session.execute(ins_daily, (
            coin_id, d,
            f(row.get("open")), f(row.get("high")), f(row.get("low")), f(row.get("close")),
            f(row.get("volume")), f(row.get("market_cap")),
        ))

def main():
    for coin in ASSETS:
        print("Hourly 7d:", coin);  upsert_hourly_last_7d(coin)
        print("Daily append:", coin); upsert_daily_append_only(coin)
    print("Done.")

if __name__ == "__main__":
    try: main()
    finally: cluster.shutdown()
