#!/usr/bin/env python3
# prices/AA_gck_load_prices_live.py
# Loads CoinGecko top markets into:
#   - gecko_prices_live (with category + change_pct_* + price_*_ago)
#   - gecko_prices_live_rolling (NO category, but DOES include change_pct_* + price_*_ago)
# Also writes category market-cap aggregates into:
#   - gecko_market_cap_live (category PK)
#   - gecko_market_cap_live_ranked (partition=MCAP_RANK_BUCKET, cluster by market_cap_rank)

import os, time, csv, requests
import sys, pathlib
from datetime import datetime, timezone, date

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
from cassandra.query import BatchStatement, ConsistencyLevel
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
BATCH_FLUSH_EVERY   = int(os.getenv("BATCH_FLUSH_EVERY", "40"))
VERBOSE_MODE        = os.getenv("VERBOSE_MODE", "0") == "1"

# tables (CoinGecko)
TABLE_LATEST  = os.getenv("TABLE_GECKO_LIVE", "gecko_prices_live")
TABLE_HIST    = os.getenv("TABLE_GECKO_ROLLING", "gecko_prices_live_rolling")

# category MCAP tables (match provided schemas: no id/name/symbol)
TABLE_MCAP_LIVE         = os.getenv("TABLE_GECKO_MCAP_LIVE", "gecko_market_cap_live")
TABLE_MCAP_LIVE_RANKED  = os.getenv("TABLE_GECKO_MCAP_LIVE_RANKED", "gecko_market_cap_live_ranked")
MCAP_RANK_BUCKET        = os.getenv("GECKO_MCAP_BUCKET", "categories")  # used as partition key 'category' in *_ranked

# categories (Symbol -> Category; live only)
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
def f(x):
    try:
        return float(x) if x is not None else None
    except Exception:
        return None

def safe_iso_to_dt(s):
    if not s: return None
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def price_ago_from_pct(now_price, pct):
    """pct is percent (e.g., +3.5), returns baseline price or None."""
    try:
        if now_price is None or pct is None:
            return None
        denom = 1.0 + (float(pct) / 100.0)
        if denom == 0:
            return None
        return float(now_price) / denom
    except Exception:
        return None

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
# LIVE includes category & price_*_ago
LIVE_COLS = [
    "id","symbol","name","category",
    "market_cap_rank",
    "price_usd","market_cap","volume_24h",
    "last_updated","last_fetched",
    "ath_price","ath_date",
    "circulating_supply","total_supply","max_supply",
    "change_pct_1h","change_pct_24h","change_pct_7d","change_pct_30d","change_pct_1y",
    "price_1h_ago","price_24h_ago","price_7d_ago","price_30d_ago","price_1y_ago",
    "vs_currency",
]

# ROLLING has NO category, but DOES include price_*_ago
ROLLING_COLS = [
    "id","last_updated",
    "symbol","name",
    "market_cap_rank",
    "price_usd","market_cap","volume_24h",
    "last_fetched",
    "ath_price","ath_date",
    "circulating_supply","total_supply","max_supply",
    "change_pct_1h","change_pct_24h","change_pct_7d","change_pct_30d","change_pct_1y",
    "price_1h_ago","price_24h_ago","price_7d_ago","price_30d_ago","price_1y_ago",
    "vs_currency",
]

def placeholders(n): return ",".join(["?"]*n)

INS_LIVE_UPSERT = session.prepare(
    f"INSERT INTO {TABLE_LATEST} ({','.join(LIVE_COLS)}) VALUES ({placeholders(len(LIVE_COLS))})"
)
INS_ROLLING_IF_NOT_EXISTS = session.prepare(
    f"INSERT INTO {TABLE_HIST} ({','.join(ROLLING_COLS)}) VALUES ({placeholders(len(ROLLING_COLS))}) IF NOT EXISTS"
)

# Updated to match your schemas (no id/name/symbol)
INS_MCAP_LIVE_UPSERT = session.prepare(
    f"INSERT INTO {TABLE_MCAP_LIVE} (category,last_updated,market_cap,market_cap_rank,volume_24h) VALUES (?,?,?,?,?)"
)
# In *_ranked the partition key is named 'category'. We use MCAP_RANK_BUCKET as that partition value.
DEL_MCAP_RANKED_BUCKET = session.prepare(
    f"DELETE FROM {TABLE_MCAP_LIVE_RANKED} WHERE category=?"
)
INS_MCAP_RANKED = session.prepare(
    f"INSERT INTO {TABLE_MCAP_LIVE_RANKED} (category,market_cap_rank,last_updated,market_cap,volume_24h) VALUES (?,?,?,?,?)"
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

    # Debug preview
    if rows:
        sample = rows[:5]
        preview = [(r.get("id"), (r.get("symbol") or "").upper(), r.get("market_cap_rank"), r.get("last_updated")) for r in sample]
        print(f"[{now_str()}] [debug] sample[0:5]: {preview}")

    batch_latest = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    w_hist = w_lat = 0
    category_totals = {}

    def bump_total(cat_name: str, mcap_value: float, vol_value: float, last_upd) -> None:
        entry = category_totals.setdefault(cat_name, {"market_cap": 0.0, "volume_24h": 0.0, "last_updated": last_upd})
        entry["market_cap"] += mcap_value
        entry["volume_24h"] += vol_value
        if last_upd and (entry["last_updated"] is None or last_upd > entry["last_updated"]):
            entry["last_updated"] = last_upd

    for idx, c in enumerate(rows, 1):
        gid   = c.get("id")
        sym   = (c.get("symbol") or "").upper()
        name  = c.get("name")
        rank  = c.get("market_cap_rank")
        price = f(c.get("current_price"))
        mcap  = f(c.get("market_cap"))
        vol   = f(c.get("total_volume"))
        lu    = safe_iso_to_dt(c.get("last_updated"))
        if not gid or lu is None:
            if VERBOSE_MODE:
                print(f"[{now_str()}] [skip] missing id/last_updated for row idx={idx}")
            continue

        ath_price = f(c.get("ath"))
        ath_date  = safe_iso_to_dt(c.get("ath_date"))

        circ = f(c.get("circulating_supply"))
        totl = f(c.get("total_supply"))
        maxs = f(c.get("max_supply"))

        # Percent changes from CG (percent units, e.g. 3.4 = +3.4%)
        ch1h  = pct(c, "price_change_percentage_1h_in_currency")
        ch24h = pct(c, "price_change_percentage_24h_in_currency")
        ch7d  = pct(c, "price_change_percentage_7d_in_currency")
        ch30d = pct(c, "price_change_percentage_30d_in_currency")
        ch1y  = pct(c, "price_change_percentage_1y_in_currency")

        # Derive baselines from pct where possible
        p1h  = price_ago_from_pct(price, ch1h)
        p24h = price_ago_from_pct(price, ch24h)
        p7d  = price_ago_from_pct(price, ch7d)
        p30d = price_ago_from_pct(price, ch30d)
        p1y  = price_ago_from_pct(price, ch1y)

        # LIVE row (with category + price_*_ago)
        cat = category_for(sym)

        mcap_total = float(mcap) if mcap is not None else 0.0
        vol_total = float(vol) if vol is not None else 0.0
        bump_total(cat, mcap_total, vol_total, lu)
        bump_total("ALL", mcap_total, vol_total, lu)

        live_vals = [
            gid, sym, name, cat,
            int(rank) if rank is not None else None,
            price, mcap, vol,
            lu, now_ts,
            ath_price, ath_date,
            circ, totl, maxs,
            ch1h, ch24h, ch7d, ch30d, ch1y,
            p1h, p24h, p7d, p30d, p1y,
            "usd",
        ]

        # ROLLING row (NO category, but includes price_*_ago)
        rolling_vals = [
            gid, lu,
            sym, name,
            int(rank) if rank is not None else None,
            price, mcap, vol,
            now_ts,
            ath_price, ath_date,
            circ, totl, maxs,
            ch1h, ch24h, ch7d, ch30d, ch1y,
            p1h, p24h, p7d, p30d, p1y,
            "usd",
        ]

        # Write rolling (IF NOT EXISTS to avoid dupes at the same last_updated)
        try:
            applied = session.execute(
                INS_ROLLING_IF_NOT_EXISTS, rolling_vals, timeout=REQUEST_TIMEOUT_SEC
            ).one().applied
            if applied:
                w_hist += 1
        except (WriteTimeout, OperationTimedOut, DriverException) as e:
            print(f"[{now_str()}] [rolling] insert failed for {sym}@{lu}: {e}")

        # Buffer live upsert
        batch_latest.add(INS_LIVE_UPSERT, live_vals)
        w_lat += 1

        # Progress + periodic flush
        if (idx % 20 == 0) or VERBOSE_MODE:
            print(f"[{now_str()}] [progress] rows={idx}/{len(rows)}; live_batch={len(batch_latest)} w_lat={w_lat} w_hist={w_hist}")
        if (w_lat % BATCH_FLUSH_EVERY) == 0:
            session.execute(batch_latest); batch_latest.clear()
            if VERBOSE_MODE:
                print(f"[{now_str()}] [flush] live batch flushed at w_lat={w_lat}")

    if len(batch_latest):
        session.execute(batch_latest)

    # ─────────────── Category MCAP aggregates (new schemas) ───────────────
    if category_totals:
        totals_items = []
        for cat_name, totals in category_totals.items():
            last_upd = totals.get("last_updated") or now_ts
            totals_items.append((cat_name, float(totals["market_cap"]), float(totals["volume_24h"]), last_upd))

        # Sort for stable output; compute ranks by market cap
        totals_items.sort(key=lambda entry: (0 if entry[0] == "ALL" else 1, entry[0].lower()))
        ranked_entries = [entry for entry in totals_items if entry[0] != "ALL"]
        ranked_entries.sort(key=lambda entry: entry[1], reverse=True)
        ranks = {cat: idx + 1 for idx, (cat, *_rest) in enumerate(ranked_entries)}
        if "ALL" in category_totals:
            ranks["ALL"] = 0

        print(f"[{now_str()}] [mcap-live] writing {len(totals_items)} category aggregates into {TABLE_MCAP_LIVE}")
        live_written = 0
        for cat_name, total_mcap, total_vol, last_upd in totals_items:
            try:
                session.execute(
                    INS_MCAP_LIVE_UPSERT,
                    [cat_name, last_upd, total_mcap, ranks.get(cat_name), total_vol],
                    timeout=REQUEST_TIMEOUT_SEC,
                )
                live_written += 1
            except (WriteTimeout, OperationTimedOut, DriverException) as e:
                print(f"[{now_str()}] [mcap-live] failed for category='{cat_name}': {e}")
        print(f"[{now_str()}] [mcap-live] rows_written={live_written}")

        # Rebuild ranked table under single partition key value = MCAP_RANK_BUCKET
        try:
            session.execute(DEL_MCAP_RANKED_BUCKET, [MCAP_RANK_BUCKET], timeout=REQUEST_TIMEOUT_SEC)
        except (WriteTimeout, OperationTimedOut, DriverException) as e:
            print(f"[{now_str()}] [mcap-ranked] failed to clear category='{MCAP_RANK_BUCKET}': {e}")
        else:
            wrote_ranked = 0
            # Emit ALL first (rank 0) if present
            all_entry = next((entry for entry in totals_items if entry[0] == "ALL"), None)
            if all_entry:
                _, all_mcap, all_vol, all_upd = all_entry
                try:
                    session.execute(
                        INS_MCAP_RANKED,
                        [MCAP_RANK_BUCKET, 0, all_upd, all_mcap, all_vol],
                        timeout=REQUEST_TIMEOUT_SEC,
                    )
                    wrote_ranked += 1
                except (WriteTimeout, OperationTimedOut, DriverException) as e:
                    print(f"[{now_str()}] [mcap-ranked] failed for rank=0: {e}")

            # Emit ranked categories
            for cat_name, total_mcap, total_vol, last_upd in ranked_entries:
                try:
                    session.execute(
                        INS_MCAP_RANKED,
                        [MCAP_RANK_BUCKET, ranks[cat_name], last_upd, total_mcap, total_vol],
                        timeout=REQUEST_TIMEOUT_SEC,
                    )
                    wrote_ranked += 1
                except (WriteTimeout, OperationTimedOut, DriverException) as e:
                    print(f"[{now_str()}] [mcap-ranked] failed for category='{cat_name}' rank={ranks[cat_name]}: {e}")
            print(f"[{now_str()}] [mcap-ranked] category(partition)='{MCAP_RANK_BUCKET}' rows_written={wrote_ranked}")
    else:
        print(f"[{now_str()}] [mcap-live] no category aggregates computed (rows=0)")

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
