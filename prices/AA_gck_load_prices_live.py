#!/usr/bin/env python3
# prices/01_gecko_load_prices_live.py  (category ONLY in LIVE, NOT in ROLLING)
import os, time, csv, requests, random
import sys, pathlib
from datetime import datetime, timezone

# ───────────────────── Repo root & helpers ─────────────────────
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.append(str(_REPO_ROOT))

try:
    from paths import rel, chdir_repo_root
except Exception:
    def rel(*parts: str) -> pathlib.Path:
        return _REPO_ROOT.joinpath(*parts)
    def chdir_repo_root() -> None:
        os.chdir(_REPO_ROOT)

chdir_repo_root()

# ───────────────────────── 3rd-party ─────────────────────────
from cassandra import OperationTimedOut, ReadTimeout, Unavailable, DriverException, WriteTimeout
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy
from cassandra.query import BatchStatement, SimpleStatement, ConsistencyLevel
from dotenv import load_dotenv

# Load .env from repo root explicitly
load_dotenv(dotenv_path=rel(".env"))

# ───────────────────────── Config ─────────────────────────
BUNDLE       = os.getenv("ASTRA_BUNDLE_PATH")
ASTRA_TOKEN  = os.getenv("ASTRA_TOKEN")
KEYSPACE     = os.getenv("ASTRA_KEYSPACE", "default_keyspace")

TOP_N        = int(os.getenv("TOP_N", "200"))
RETRIES      = int(os.getenv("RETRIES", "3"))
BACKOFF_MIN  = int(os.getenv("BACKOFF_MIN", "5"))
MAX_BACKOFF  = int(os.getenv("MAX_BACKOFF_MIN", "30"))

REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "60"))
CONNECT_TIMEOUT_SEC = int(os.getenv("CONNECT_TIMEOUT_SEC", "15"))
FETCH_SIZE          = int(os.getenv("FETCH_SIZE", "500"))
BATCH_FLUSH_EVERY   = int(os.getenv("BATCH_FLUSH_EVERY", "40"))

# tables (CoinGecko clean slate)
TABLE_LATEST  = os.getenv("TABLE_GECKO_LIVE", "gecko_prices_live")
TABLE_HIST    = os.getenv("TABLE_GECKO_ROLLING", "gecko_prices_live_rolling")

# categories
CATEGORY_FILE = os.getenv("CATEGORY_FILE", str(rel("prices", "category_mapping.csv")))

# CoinGecko
API_TIER   = (os.getenv("COINGECKO_API_TIER") or "demo").strip().lower()  # "demo" | "pro"
API_KEY    = (os.getenv("COINGECKO_API_KEY") or "").strip()
if API_KEY.lower().startswith("api key:"):
    API_KEY = API_KEY.split(":", 1)[1].strip()

BASE = os.getenv(
    "COINGECKO_BASE_URL",
    "https://api.coingecko.com/api/v3" if API_TIER == "demo" else "https://pro-api.coingecko.com/api/v3"
)
HDR  = "x-cg-demo-api-key" if API_TIER == "demo" else "x-cg-pro-api-key"
QS   = "x_cg_demo_api_key" if API_TIER == "demo" else "x_cg_pro_api_key"

if not BUNDLE or not ASTRA_TOKEN or not KEYSPACE:
    raise SystemExit("Missing ASTRA_BUNDLE_PATH / ASTRA_TOKEN / ASTRA_KEYSPACE")
if not API_KEY:
    raise SystemExit("Missing COINGECKO_API_KEY")

def now_str(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ─────────────── Category mapping (repo-relative) ───────────────
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
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)

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

def http_get(path, params=None):
    url = f"{BASE}{path}"
    params = dict(params or {})
    headers = {HDR: API_KEY}
    params[QS] = API_KEY
    for attempt in range(1, RETRIES + 1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=30)
            if r.status_code in (402, 429) or 500 <= r.status_code < 600:
                raise requests.HTTPError(f"{r.status_code} from CoinGecko", response=r)
            r.raise_for_status()
            return r.json()
        except (requests.HTTPError, requests.ConnectionError, requests.Timeout) as e:
            if attempt < RETRIES:
                wait_s = min(BACKOFF_MIN * (2 ** (attempt - 1)), MAX_BACKOFF)
                print(f"[{now_str()}] [fetch] error: {e}. Retrying in ~{wait_s}s…")
                time.sleep(wait_s)
            else:
                print(f"[{now_str()}] [fetch] giving up after {attempt} attempts.")
                raise

# ───────────────────────── Statements ─────────────────────────
# LIVE table includes category; ROLLING table does NOT include category.
LIVE_COLS = [
    "id","symbol","name","category",
    "market_cap_rank",
    "price_usd","market_cap","volume_24h",
    "last_fetched","last_updated",
    "ath_date","ath_price",
    "circulating_supply","total_supply","max_supply",
    "change_pct_1h","change_pct_24h","change_pct_7d","change_pct_30d","change_pct_1y",
    "vs_currency",
]

ROLLING_COLS = [
    "id","last_updated",
    "symbol","name",
    "market_cap_rank",
    "price_usd","market_cap","volume_24h",
    "last_fetched",
    "ath_date","ath_price",
    "circulating_supply","total_supply","max_supply",
    "change_pct_1h","change_pct_24h","change_pct_7d","change_pct_30d","change_pct_1y",
    "vs_currency",
]

def placeholders(n): return ",".join(["?"]*n)

INS_LIVE_UPSERT = session.prepare(
    f"INSERT INTO {TABLE_LATEST} ({','.join(LIVE_COLS)}) VALUES ({placeholders(len(LIVE_COLS))})"
)
INS_ROLLING_IF_NOT_EXISTS = session.prepare(
    f"INSERT INTO {TABLE_HIST} ({','.join(ROLLING_COLS)}) VALUES ({placeholders(len(ROLLING_COLS))}) IF NOT EXISTS"
)

# ───────────────────────── Fetch ─────────────────────────
def fetch_top_markets(limit: int) -> list:
    per_page = min(250, max(1, limit))
    params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": 1,
        "price_change_percentage": "1h,24h,7d,30d,1y",
        "locale": "en",
    }
    data = http_get("/coins/markets", params=params)
    data = data[:limit]
    print(f"[{now_str()}] [fetch] got {len(data)} markets from CoinGecko")
    return data

def pct(d: dict, key: str):
    try:
        v = d.get(key)
        return float(v) if v is not None else None
    except Exception:
        return None

# ───────────────────────── Run once ─────────────────────────
def run_once():
    now_ts = datetime.now(timezone.utc)
    rows = fetch_top_markets(TOP_N)

    # --- DEBUG right after fetching markets ---
    print(f"[{now_str()}] [debug] preparing to upsert {len(rows)} rows → live={TABLE_LATEST}, rolling={TABLE_HIST}")
    if rows:
        sample = rows[:5]
        preview = [(r.get("id"), (r.get("symbol") or "").upper(), r.get("market_cap_rank"), r.get("last_updated")) for r in sample]
        print(f"[{now_str()}] [debug] sample[0:5]: {preview}")
    t_loop_start = time.time()


    batch_latest = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    w_hist = w_lat = 0

    for idx, c in enumerate(rows, 1):
        ...
        # add rolling, add live to batch, maybe flush

        if idx % 20 == 0:
            print(f"[{now_str()}] [progress] rows={idx}/{len(rows)}; "
                f"live_batch_size={len(batch_latest)}; "
                f"w_lat={w_lat} w_hist={w_hist}")

        if w_lat % BATCH_FLUSH_EVERY == 0:
            session.execute(batch_latest); batch_latest.clear()
            print(f"[{now_str()}] [progress] live batch FLUSHED at w_lat={w_lat}")

        
        gid   = c.get("id")
        sym   = (c.get("symbol") or "").upper()
        name  = c.get("name")
        rank  = c.get("market_cap_rank")
        price = f(c.get("current_price"))
        mcap  = f(c.get("market_cap"))
        vol   = f(c.get("total_volume"))
        lu    = safe_iso_to_dt(c.get("last_updated"))
        if not gid or lu is None:
            continue

        ath_date = safe_iso_to_dt(c.get("ath_date"))
        ath_price = f(c.get("ath"))

        circ = f(c.get("circulating_supply"))
        totl = f(c.get("total_supply"))
        maxs = f(c.get("max_supply"))

        ch1h  = pct(c, "price_change_percentage_1h_in_currency")
        ch24h = pct(c, "price_change_percentage_24h_in_currency")
        ch7d  = pct(c, "price_change_percentage_7d_in_currency")
        ch30d = pct(c, "price_change_percentage_30d_in_currency")
        ch1y  = pct(c, "price_change_percentage_1y_in_currency")

        # LIVE row (with category)
        live_vals = [
            gid, sym, name, category_for(sym),
            int(rank) if rank is not None else None,
            price, mcap, vol,
            now_ts, lu,
            ath_date, ath_price,
            circ, totl, maxs,
            ch1h, ch24h, ch7d, ch30d, ch1y,
            "usd",
        ]

        # ROLLING row (NO category)
        rolling_vals = [
            gid, lu,
            sym, name,
            int(rank) if rank is not None else None,
            price, mcap, vol,
            now_ts,
            ath_date, ath_price,
            circ, totl, maxs,
            ch1h, ch24h, ch7d, ch30d, ch1y,
            "usd",
        ]

        try:
            applied = session.execute(INS_ROLLING_IF_NOT_EXISTS, rolling_vals, timeout=REQUEST_TIMEOUT_SEC).one().applied
            if applied:
                w_hist += 1
        except (WriteTimeout, OperationTimedOut, DriverException) as e:
            print(f"[{now_str()}] [rolling] insert failed for {sym}@{lu}: {e}")

        batch_latest.add(INS_LIVE_UPSERT, live_vals)
        w_lat += 1
        if (w_lat % BATCH_FLUSH_EVERY) == 0:
            session.execute(batch_latest); batch_latest.clear()

    if len(batch_latest):
        session.execute(batch_latest)

    print(f"[{now_str()}] wrote rolling={w_hist} latest={w_lat}")

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
