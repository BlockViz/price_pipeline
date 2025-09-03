# 01_load_prices_live.py
import os, time, csv, requests, random
from datetime import datetime, timezone

from cassandra import OperationTimedOut, ReadTimeout, Unavailable, DriverException, WriteTimeout
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy
from cassandra.query import BatchStatement, SimpleStatement, ConsistencyLevel
from dotenv import load_dotenv

load_dotenv()

# ───────────────────────── Config ─────────────────────────
BUNDLE       = os.getenv("ASTRA_BUNDLE_PATH")
ASTRA_TOKEN  = os.getenv("ASTRA_TOKEN")
KEYSPACE     = os.getenv("ASTRA_KEYSPACE", "default_keyspace")

TOP_N        = int(os.getenv("TOP_N", "200"))     # top 200
RETRIES      = int(os.getenv("RETRIES", "3"))
BACKOFF_MIN  = int(os.getenv("BACKOFF_MIN", "5"))
MAX_BACKOFF  = int(os.getenv("MAX_BACKOFF_MIN", "30"))

REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "60"))
CONNECT_TIMEOUT_SEC = int(os.getenv("CONNECT_TIMEOUT_SEC", "15"))
FETCH_SIZE          = int(os.getenv("FETCH_SIZE", "500"))
BATCH_FLUSH_EVERY   = int(os.getenv("BATCH_FLUSH_EVERY", "40"))

# tables
TABLE_LATEST  = os.getenv("TABLE_LATEST", "prices_live")            # 1-row per id “view”
TABLE_HIST    = os.getenv("TABLE_HISTORY", "prices_live_rolling")   # rolling history keyed by (id, last_updated)
WRITE_LATEST  = os.getenv("WRITE_LATEST", "1") == "1"

if not BUNDLE or not ASTRA_TOKEN or not KEYSPACE:
    raise SystemExit("Missing ASTRA_BUNDLE_PATH / ASTRA_TOKEN / ASTRA_KEYSPACE")

def now_str(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ─────────────── Category mapping ───────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CATEGORY_FILE = os.getenv("CATEGORY_FILE", os.path.join(BASE_DIR, "category_mapping.csv"))

def load_category_map(path: str) -> dict:
    m = {}
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
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
                    print(f"[{now_str()}] [category] loaded {len(m)} rows from {path}")
                    break
            else:
                print(f"[{now_str()}] [category] header not found in {path} (need Symbol,Category)")
    except FileNotFoundError:
        print(f"[{now_str()}] [category] file not found: {path} — defaulting to 'Other'")
    except Exception as e:
        print(f"[{now_str()}] [category] failed to read {path}: {e} — defaulting to 'Other'")
    return m

CATEGORY_MAP = load_category_map(CATEGORY_FILE)
def category_for(sym: str) -> str: return CATEGORY_MAP.get((sym or "").upper(), "Other")

# ───────────────────────── Connect ─────────────────────────
print(f"[{now_str()}] Connecting to Astra (bundle='{BUNDLE}', keyspace='{KEYSPACE}')")
auth = PlainTextAuthProvider(username="token", password=ASTRA_TOKEN)
exec_profile = ExecutionProfile(load_balancing_policy=RoundRobinPolicy(),
                                request_timeout=REQUEST_TIMEOUT_SEC)
cluster = Cluster(
    cloud={"secure_connect_bundle": BUNDLE},
    auth_provider=auth,
    execution_profiles={EXEC_PROFILE_DEFAULT: exec_profile},
    connect_timeout=CONNECT_TIMEOUT_SEC,
)
session = cluster.connect(KEYSPACE)
print(f"[{now_str()}] Connected.")

# ───────────────────────── Helpers ─────────────────────────
def f(x): return float(x) if x is not None else None
def safe_iso_to_dt(s):
    if not s: return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))
def to_utc(dt):
    if dt is None: return None
    if dt.tzinfo is None: return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def exec_with_retry(stmt, params=None, timeout=REQUEST_TIMEOUT_SEC, attempts=3, label="CQL"):
    last = None
    for i in range(1, attempts + 1):
        try:
            return session.execute(stmt, params or [], timeout=timeout)
        except (OperationTimedOut, ReadTimeout, Unavailable, WriteTimeout, DriverException) as e:
            last = e
            wait = min(2 ** i, MAX_BACKOFF)
            print(f"[{now_str()}] [{label}] attempt {i}/{attempts} failed: {e} — retry in {wait}s")
            time.sleep(wait)
    raise last

def prior(p, chg):
    return (p / (1.0 + chg)) if (p is not None and chg is not None and abs(1+chg) > 1e-12) else None

# ───────────────────────── Fetch ─────────────────────────
def fetch_ranked(limit: int):
    url = "https://api.coinpaprika.com/v1/tickers?quotes=USD"
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            print(f"[{now_str()}] [fetch] attempt {attempt}/{RETRIES} → {url}")
            t0 = time.time()
            r = requests.get(url, timeout=30)
            dt = time.time() - t0
            if r.status_code in (402, 429) or 500 <= r.status_code < 600:
                raise requests.HTTPError(f"{r.status_code} from CoinPaprika", response=r)
            r.raise_for_status()
            data = r.json()
            good = [d for d in data if isinstance(d.get("rank"), int) and d["rank"] > 0]
            good.sort(key=lambda x: x["rank"])
            got = good[:limit]
            print(f"[{now_str()}] [fetch] got {len(got)} tickers in {dt:.2f}s")
            return got
        except (requests.HTTPError, requests.ConnectionError, requests.Timeout) as e:
            last_err = e
            if attempt < RETRIES:
                wait_min = min(BACKOFF_MIN * (2 ** (attempt - 1)), MAX_BACKOFF)
                jitter = random.uniform(0, 0.3 * wait_min)
                wait_s = int((wait_min + jitter) * 60)
                print(f"[{now_str()}] [fetch] error: {e}. Retrying in ~{wait_s}s…")
                time.sleep(wait_s)
            else:
                print(f"[{now_str()}] [fetch] giving up after {attempt} attempts.")
                raise
    raise last_err or RuntimeError("Unknown fetch error")

# ───────────────────────── Statements ─────────────────────────
BASE_COLS = [
    "id","symbol","name","rank","category",
    "price_usd","market_cap","volume_24h",
    "last_fetched","last_updated",
    "price_1h_ago","chg_1h",
    "price_6h_ago","chg_6h",
    "price_12h_ago","chg_12h",
    "price_24h_ago","chg_24h",
    "price_7d_ago","chg_7d",
    "price_30d_ago","chg_30d",
    "price_1y_ago","chg_1y",
    "ath_date","ath_price",
    "total_supply","max_supply",
]
# history columns: (id, last_updated) + the rest (no duplicates)
HIST_COLS = ["id","last_updated"] + [c for c in BASE_COLS if c not in ("id","last_updated")]

def placeholders(n): return ",".join(["?"]*n)

# latest 1-row-per-coin “view”
INS_LATEST_UPSERT = session.prepare(
    f"INSERT INTO {TABLE_LATEST} ({','.join(BASE_COLS)}) VALUES ({placeholders(len(BASE_COLS))})"
)

# exact-timestamp rolling history (conditional; DO NOT BATCH ACROSS PARTITIONS)
INS_HIST_IF_NOT_EXISTS = session.prepare(
    f"INSERT INTO {TABLE_HIST} ({','.join(HIST_COLS)}) VALUES ({placeholders(len(HIST_COLS))}) IF NOT EXISTS"
)

# most recent history point for a coin
SEL_LAST_UPD = session.prepare(
    f"SELECT last_updated FROM {TABLE_HIST} WHERE id=? ORDER BY last_updated DESC LIMIT 1"
)

# ───────────────────────── Run once ─────────────────────────
def run_once():
    now_ts = datetime.now(timezone.utc)
    rows = fetch_ranked(TOP_N)

    # Only batch the non-conditional latest upserts
    batch_latest = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    w_hist = w_lat = 0

    for c in rows:
        q = c.get("quotes", {}).get("USD", {}) or {}
        lu = to_utc(safe_iso_to_dt(c.get("last_updated")))
        if not lu:
            continue  # skip asset with no timestamp

        # Only append if this coin's last_updated advanced
        prev = session.execute(SEL_LAST_UPD, [c.get("id")], timeout=REQUEST_TIMEOUT_SEC).one()
        prev_upd = to_utc(prev.last_updated) if prev and getattr(prev, "last_updated", None) else None
        if prev_upd and lu <= prev_upd:
            continue

        price_now = f(q.get("price"))
        chg_1h  = (float(q["percent_change_1h"])/100.0)   if q.get("percent_change_1h")  is not None else None
        chg_6h  = (float(q["percent_change_6h"])/100.0)   if q.get("percent_change_6h")  is not None else None
        chg_12h = (float(q["percent_change_12h"])/100.0)  if q.get("percent_change_12h") is not None else None
        chg_24h = (float(q["percent_change_24h"])/100.0)  if q.get("percent_change_24h") is not None else None
        chg_7d  = (float(q["percent_change_7d"])/100.0)   if q.get("percent_change_7d")  is not None else None
        chg_30d = (float(q["percent_change_30d"])/100.0)  if q.get("percent_change_30d") is not None else None
        chg_1y  = (float(q["percent_change_1y"])/100.0)   if q.get("percent_change_1y")  is not None else None

        base_vals = [
            c.get("id"),
            c.get("symbol"),
            c.get("name"),
            int(c["rank"]) if c.get("rank") is not None else None,
            category_for(c.get("symbol")),
            price_now,
            f(q.get("market_cap")),
            f(q.get("volume_24h")),
            now_ts,               # when we fetched it
            lu,                   # CoinPaprika's timestamp
            prior(price_now, chg_1h),   chg_1h,
            prior(price_now, chg_6h),   chg_6h,
            prior(price_now, chg_12h),  chg_12h,
            prior(price_now, chg_24h),  chg_24h,
            prior(price_now, chg_7d),   chg_7d,
            prior(price_now, chg_30d),  chg_30d,
            prior(price_now, chg_1y),   chg_1y,
            safe_iso_to_dt(q.get("ath_date")),
            f(q.get("ath_price")),
            f(c.get("total_supply")),
            f(c.get("max_supply")),
        ]

        # VALUES for history must follow HIST_COLS order
        col_to_val = dict(zip(BASE_COLS, base_vals))
        hist_vals = [col_to_val["id"], col_to_val["last_updated"]] + \
                    [col_to_val[col] for col in BASE_COLS if col not in ("id","last_updated")]

        # 1) exact history point (conditional insert) — execute individually (NO batch)
        try:
            applied = session.execute(INS_HIST_IF_NOT_EXISTS, hist_vals, timeout=REQUEST_TIMEOUT_SEC).one().applied
            if applied:
                w_hist += 1
        except (WriteTimeout, OperationTimedOut, DriverException) as e:
            print(f"[{now_str()}] [hist] insert failed for {c.get('symbol')}@{lu}: {e}")

        # 2) latest “view” (optional) — safe to batch (no IF)
        if WRITE_LATEST:
            batch_latest.add(INS_LATEST_UPSERT, base_vals)
            w_lat += 1
            if (w_lat % BATCH_FLUSH_EVERY) == 0:
                session.execute(batch_latest); batch_latest.clear()

    # final flush
    if WRITE_LATEST and len(batch_latest):
        session.execute(batch_latest)

    print(f"[{now_str()}] wrote history={w_hist}"
          f"{' latest='+str(w_lat) if WRITE_LATEST else ''}")

# ───────────────────────── Entrypoint ─────────────────────────
def main():
    try:
        run_once()
    finally:
        print(f"[{now_str()}] Shutting down…")
        try:
            cluster.shutdown()
        except Exception as e:
            print(f"[{now_str()}] Error during shutdown: {e}")
        print(f"[{now_str()}] Done.")

if __name__ == "__main__":
    main()
