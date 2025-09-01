import os, csv
from datetime import datetime, timezone
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from dotenv import load_dotenv

load_dotenv()

BUNDLE = os.getenv("ASTRA_BUNDLE_PATH", "secure-connect.zip")
ASTRA_TOKEN = os.getenv("ASTRA_TOKEN")
KEYSPACE = os.getenv("ASTRA_KEYSPACE", "default_keyspace")
CATEGORY_FILE = os.getenv("CATEGORY_FILE", "category_mapping.csv")

if not ASTRA_TOKEN:
    raise SystemExit("Missing ASTRA_TOKEN")

auth = PlainTextAuthProvider("token", ASTRA_TOKEN)
cluster = Cluster(cloud={"secure_connect_bundle": BUNDLE}, auth_provider=auth)
s = cluster.connect(KEYSPACE)

UP_S = s.prepare("""
  INSERT INTO asset_categories_by_symbol (symbol, category, updated_at)
  VALUES (?, ?, ?)
""")
UP_ID = s.prepare("""
  INSERT INTO asset_categories_by_id (id, symbol, category, updated_at)
  VALUES (?, ?, ?, ?)
""")

# Pull current ids for symbols from prices_live (so we can fill the by_id table)
SYM_TO_ID = {}
for r in s.execute(SimpleStatement("SELECT id, symbol FROM prices_live")):
    if r.symbol:
        SYM_TO_ID[r.symbol.upper()] = r.id

def autodetect_and_load(path: str) -> dict:
    m = {}
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(2048); f.seek(0)
        for delim in [",", ";", "\t", "|"]:
            f.seek(0)
            dr = csv.DictReader(f, delimiter=delim)
            headers = [h.strip().lower() for h in (dr.fieldnames or [])]
            if "symbol" in headers and "category" in headers:
                s_key = dr.fieldnames[headers.index("symbol")]
                c_key = dr.fieldnames[headers.index("category")]
                for row in dr:
                    sym = (row.get(s_key) or "").strip().upper()
                    cat = (row.get(c_key) or "").strip() or "Other"
                    if sym:
                        m[sym] = cat
                break
    return m

def main():
    mapping = autodetect_and_load(CATEGORY_FILE)
    now = datetime.now(timezone.utc)
    up_s = up_id = 0

    for sym, cat in mapping.items():
        s.execute(UP_S, [sym, cat, now]); up_s += 1
        coin_id = SYM_TO_ID.get(sym)
        if coin_id:
            s.execute(UP_ID, [coin_id, sym, cat, now]); up_id += 1

    print(f"Loaded categories: by_symbol={up_s}, by_id={up_id} (from {CATEGORY_FILE})")

if __name__ == "__main__":
    try:
        main()
    finally:
        cluster.shutdown()
