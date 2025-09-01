import os, time, requests
from datetime import datetime, timezone
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, SimpleStatement, ConsistencyLevel
from dotenv import load_dotenv
from cassandra import OperationTimedOut, ReadTimeout, Unavailable
import csv


load_dotenv()

# ---- Category mapping (CSV) ----
# Resolve file relative to this script by default
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CATEGORY_FILE = os.getenv("CATEGORY_FILE", os.path.join(BASE_DIR, "category_mapping.csv"))

def load_category_map(path: str) -> dict:
    m = {}
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            sample = f.read(2048)
            f.seek(0)
            # try common delimiters: comma, semicolon, tab, pipe
            for delim in [",", ";", "\t", "|"]:
                f.seek(0)
                reader = csv.DictReader(f, delimiter=delim)
                headers = [h.strip().lower() for h in (reader.fieldnames or [])]
                if "symbol" in headers and "category" in headers:
                    sym_key = reader.fieldnames[headers.index("symbol")]
                    cat_key = reader.fieldnames[headers.index("category")]
                    for row in reader:
                        sym = (row.get(sym_key) or "").strip().upper()
                        cat = (row.get(cat_key) or "").strip()
                        if sym:
                            m[sym] = cat or "Other"
                    print(f"[category] loaded {len(m)} rows from {path} using delimiter {repr(delim)}")
                    break
            else:
                print(f"[category] header not found in {path}. Expected columns: Symbol,Category")
    except FileNotFoundError:
        print(f"[category] file not found: {path} — defaulting to 'Other'")
    except Exception as e:
        print(f"[category] failed to read {path}: {e} — defaulting to 'Other'")
    return m

CATEGORY_MAP = load_category_map(CATEGORY_FILE)

def category_for(symbol: str) -> str:
    if not symbol:
        return "Other"
    return CATEGORY_MAP.get(symbol.upper(), "Other")


# ---- Config ----
BUNDLE = os.getenv("ASTRA_BUNDLE_PATH")
ASTRA_TOKEN = os.getenv("ASTRA_TOKEN")
KEYSPACE = os.getenv("ASTRA_KEYSPACE", "default_keyspace")

TOP_N = int(os.getenv("TOP_N", "100"))
RETRIES = int(os.getenv("RETRIES", "3"))
BACKOFF_MIN = int(os.getenv("BACKOFF_MIN", "5"))
REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "60"))  # was ~10–20 default
TRUNCATE_ATTEMPTS    = int(os.getenv("TRUNCATE_ATTEMPTS", "5"))

# Swap safety: require at least this many rows before touching live
MIN_EXPECTED = int(os.getenv("MIN_EXPECTED", max(50, TOP_N // 2)))

if not BUNDLE or not ASTRA_TOKEN or not KEYSPACE:
    raise SystemExit("Missing ASTRA_BUNDLE_PATH / ASTRA_TOKEN / ASTRA_KEYSPACE")

# ---- Connect (SCB + token auth) ----
auth = PlainTextAuthProvider(username="token", password=ASTRA_TOKEN)
cluster = Cluster(cloud={"secure_connect_bundle": BUNDLE}, auth_provider=auth)
session = cluster.connect(KEYSPACE)

# warm-up: touch a tiny query to “wake” the cluster
try:
    session.execute("SELECT release_version FROM system.local", timeout=REQUEST_TIMEOUT_SEC)
except Exception as e:
    print(f"[warmup] non-fatal: {e}")


# Prepared inserts (tmp & live have identical columns)
INS_COLS = """
  id, symbol, name, rank, category,
  price_usd, market_cap, volume_24h,
  last_fetched, last_updated,
  price_1h_ago, chg_1h,
  price_6h_ago, chg_6h,
  price_12h_ago, chg_12h,
  price_24h_ago, chg_24h,
  price_7d_ago, chg_7d,
  price_30d_ago, chg_30d,
  price_1y_ago, chg_1y,
  ath_date, ath_price,
  total_supply, max_supply
"""

ins_tmp = session.prepare(f"INSERT INTO prices_live_tmp ({INS_COLS}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
ins_live = session.prepare(f"INSERT INTO prices_live ({INS_COLS}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")

def exec_with_retry(stmt, params=None, timeout=REQUEST_TIMEOUT_SEC, attempts=TRUNCATE_ATTEMPTS, label=""):
    last = None
    for i in range(1, attempts + 1):
        try:
            return session.execute(stmt, params or [], timeout=timeout)
        except (OperationTimedOut, ReadTimeout, Unavailable) as e:
            last = e
            wait = min(2 ** i, 20)  # 2s,4s,8s,16s,20s...
            print(f"[{label or 'CQL'}] attempt {i}/{attempts} failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)
    raise last

def f(x):
    return float(x) if x is not None else None

def safe_iso_to_dt(s):
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def prior_price_from_change(price_now, chg_frac):
    # price_now = price_prior * (1 + chg)  => price_prior = price_now / (1 + chg)
    if price_now is None or chg_frac is None:
        return None
    denom = 1.0 + chg_frac
    if abs(denom) < 1e-12:
        return None
    return price_now / denom

def fetch_ranked(limit: int):
    """Fetch, filter valid ranks, sort by rank asc, take TOP_N. Retries on 402/429/5xx/timeouts."""
    url = "https://api.coinpaprika.com/v1/tickers?quotes=USD"
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(url, timeout=30)
            if r.status_code in (402, 429) or 500 <= r.status_code < 600:
                raise requests.HTTPError(f"{r.status_code} from CoinPaprika", response=r)
            r.raise_for_status()
            data = r.json()
            good = [d for d in data if isinstance(d.get("rank"), int) and d["rank"] > 0]
            good.sort(key=lambda x: x["rank"])
            return good[:limit]
        except (requests.HTTPError, requests.ConnectionError, requests.Timeout) as e:
            last_err = e
            if attempt < RETRIES:
                wait_s = BACKOFF_MIN * 60
                print(f"[Attempt {attempt}/{RETRIES}] API error: {e}. Retrying in {BACKOFF_MIN} min…")
                time.sleep(wait_s)
            else:
                print(f"[Attempt {attempt}/{RETRIES}] Giving up.")
                raise
    raise last_err or RuntimeError("Unknown fetch error")

def write_rows(rows, prepared_stmt):
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    count = 0
    for c in rows:
        q = c.get("quotes", {}).get("USD", {}) or {}
        price_now = f(q.get("price"))

        chg_1h  = (float(q["percent_change_1h"])/100.0)   if q.get("percent_change_1h")  is not None else None
        chg_6h  = (float(q["percent_change_6h"])/100.0)   if q.get("percent_change_6h")  is not None else None
        chg_12h = (float(q["percent_change_12h"])/100.0)  if q.get("percent_change_12h") is not None else None
        chg_24h = (float(q["percent_change_24h"])/100.0)  if q.get("percent_change_24h") is not None else None
        chg_7d  = (float(q["percent_change_7d"])/100.0)   if q.get("percent_change_7d")  is not None else None
        chg_30d = (float(q["percent_change_30d"])/100.0)  if q.get("percent_change_30d") is not None else None
        chg_1y  = (float(q["percent_change_1y"])/100.0)   if q.get("percent_change_1y")  is not None else None

        batch.add(prepared_stmt, (
            c.get("id"),
            c.get("symbol"),
            c.get("name"),
            int(c["rank"]) if c.get("rank") is not None else None,
            category_for(c.get("symbol")),
            price_now,
            f(q.get("market_cap")),
            f(q.get("volume_24h")),
            NOW,
            safe_iso_to_dt(c.get("last_updated")),
            prior_price_from_change(price_now, chg_1h),   chg_1h,
            prior_price_from_change(price_now, chg_6h),   chg_6h,
            prior_price_from_change(price_now, chg_12h),  chg_12h,
            prior_price_from_change(price_now, chg_24h),  chg_24h,
            prior_price_from_change(price_now, chg_7d),   chg_7d,
            prior_price_from_change(price_now, chg_30d),  chg_30d,
            prior_price_from_change(price_now, chg_1y),   chg_1y,
            safe_iso_to_dt(q.get("ath_date")),
            f(q.get("ath_price")),
            f(c.get("total_supply")),
            f(c.get("max_supply")),
        ))
        count += 1
        if count % 40 == 0:
            session.execute(batch); batch.clear()
    if len(batch) > 0:
        session.execute(batch)
    return count

def main():
    global NOW
    NOW = datetime.now(timezone.utc)

    # 1) Fetch FIRST
    rows = fetch_ranked(TOP_N)           # rank-sorted, unique by id
    ids = [r.get("id") for r in rows]
    if len(set(ids)) != len(ids):
        print("Warning: duplicate CoinPaprika IDs in fetched set (unexpected).")

    # 2) Write to TMP
    exec_with_retry("TRUNCATE prices_live_tmp", label="TRUNCATE tmp")
    wrote = write_rows(rows, ins_tmp)

    # 3) Verify TMP rowcount (COUNT(*) is OK at ~100 rows)
    #    If you prefer to avoid COUNT(*), select ids and count client-side.
    tmp_count = session.execute(SimpleStatement("SELECT COUNT(*) FROM prices_live_tmp")).one()[0]
    if tmp_count < MIN_EXPECTED:
        raise SystemExit(f"Aborting swap: tmp has only {tmp_count} rows (MIN_EXPECTED={MIN_EXPECTED}). Live table left untouched.")

    # 4) Swap into LIVE
    exec_with_retry("TRUNCATE prices_live", label="TRUNCATE live")
    
    # Read back from tmp and copy to live
    tmp_rows = session.execute(SimpleStatement("SELECT " + INS_COLS + " FROM prices_live_tmp", fetch_size=500))
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    copied = 0
    for r in tmp_rows:
        batch.add(ins_live, (
            r.id, r.symbol, r.name, r.rank, r.category,
            r.price_usd, r.market_cap, r.volume_24h,
            r.last_fetched, r.last_updated,
            r.price_1h_ago, r.chg_1h,
            r.price_6h_ago, r.chg_6h,
            r.price_12h_ago, r.chg_12h,
            r.price_24h_ago, r.chg_24h,
            r.price_7d_ago, r.chg_7d,
            r.price_30d_ago, r.chg_30d,
            r.price_1y_ago, r.chg_1y,
            r.ath_date, r.ath_price,
            r.total_supply, r.max_supply,
        ))
        copied += 1
        if copied % 40 == 0:
            session.execute(batch); batch.clear()
    if len(batch) > 0:
        session.execute(batch)

    # 5) Clean TMP (optional)
    session.execute("TRUNCATE prices_live_tmp")

    print(f"Snapshot OK ✅ wrote {wrote} to tmp, copied {copied} → prices_live at {NOW.isoformat()}")

if __name__ == "__main__":
    try:
        main()
    finally:
        cluster.shutdown()
