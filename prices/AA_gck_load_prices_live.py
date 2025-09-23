#!/usr/bin/env python3
# backend/prices/AA_gck_load_prices_all.py
# One-pass loader that refreshes 5 tables:
#   - gecko_prices_live                  (TRUNCATE → repopulate)
#   - gecko_prices_live_ranked           (DELETE bucket → repopulate from live buffer)
#   - gecko_prices_live_rolling          (append idempotently with IF NOT EXISTS)
#   - gecko_market_cap_live              (TRUNCATE → repopulate)
#   - gecko_market_cap_live_ranked       (DELETE category=MCAP_RANK_BUCKET → repopulate)
#
# Notes:
# - No hard staleness drop. We always keep Top N coins.
# - We log WARN if last_updated is older than STALE_WARN_MINUTES.
# - Safe refresh: only truncate/clear once we have enough fresh rows buffered.

import os, time, csv, requests
import sys, pathlib
from datetime import datetime, timezone, timedelta

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
from cassandra import OperationTimedOut, DriverException, WriteTimeout
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy
from cassandra.query import BatchStatement, ConsistencyLevel, SimpleStatement
from dotenv import load_dotenv

# Load .env from repo root explicitly
load_dotenv(dotenv_path=rel(".env"))

# ───────────────────────── Config ─────────────────────────
BUNDLE       = os.getenv("ASTRA_BUNDLE_PATH", "secure-connect.zip")
ASTRA_TOKEN  = os.getenv("ASTRA_TOKEN")
KEYSPACE     = os.getenv("ASTRA_KEYSPACE", "default_keyspace")

TOP_N        = int(os.getenv("TOP_N", "200"))
RETRIES      = int(os.getenv("RETRIES", "3"))
BACKOFF_MIN  = int(os.getenv("BACKOFF_MIN", "5"))
MAX_BACKOFF  = int(os.getenv("MAX_BACKOFF_MIN", "30"))

REQUEST_TIMEOUT_SEC   = int(os.getenv("REQUEST_TIMEOUT_SEC", "60"))
CONNECT_TIMEOUT_SEC   = int(os.getenv("CONNECT_TIMEOUT_SEC", "15"))
BATCH_FLUSH_EVERY     = int(os.getenv("BATCH_FLUSH_EVERY", "40"))
VERBOSE_MODE          = os.getenv("VERBOSE_MODE", "0") == "1"

# Warn if older than this; DO NOT drop
STALE_WARN_MINUTES    = int(os.getenv("STALE_WARN_MINUTES", "10"))

# Safety threshold before truncating live tables
REQUIRED_LIVE_MIN     = int(os.getenv("REQUIRED_LIVE_MIN", str(int(TOP_N * 0.7))))

# tables
TABLE_LIVE            = os.getenv("TABLE_GECKO_LIVE", "gecko_prices_live")
TABLE_LIVE_RANKED     = os.getenv("TABLE_GECKO_PRICES_LIVE_RANKED", "gecko_prices_live_ranked")
TABLE_ROLLING         = os.getenv("TABLE_GECKO_ROLLING", "gecko_prices_live_rolling")

TABLE_MCAP_LIVE       = os.getenv("TABLE_GECKO_MCAP_LIVE", "gecko_market_cap_live")
TABLE_MCAP_RANKED     = os.getenv("TABLE_GECKO_MCAP_LIVE_RANKED", "gecko_market_cap_live_ranked")
MCAP_RANK_BUCKET      = os.getenv("GECKO_MCAP_BUCKET", "categories")  # single-partition value

# ranked bucket for prices_live_ranked
RANK_BUCKET           = os.getenv("RANK_BUCKET", "all")
RANK_TOP_N            = int(os.getenv("RANK_TOP_N", str(TOP_N)))

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

def fmt_rank(r):
    try:
        return f"r={int(r)}" if r is not None and int(r) > 0 else "r=?"
    except Exception:
        return "r=?"

# ───────────────────────── Statements ─────────────────────────
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
    f"INSERT INTO {TABLE_LIVE} ({','.join(LIVE_COLS)}) VALUES ({placeholders(len(LIVE_COLS))})"
)
INS_ROLLING_IF_NOT_EXISTS = session.prepare(
    f"INSERT INTO {TABLE_ROLLING} ({','.join(ROLLING_COLS)}) VALUES ({placeholders(len(ROLLING_COLS))}) IF NOT EXISTS"
)

# prices_live_ranked
INS_PRICES_LIVE_RANKED = session.prepare(
    f"INSERT INTO {TABLE_LIVE_RANKED} ("
    f"  bucket, market_cap_rank, id, symbol, name, category, "
    f"  price_usd, market_cap, volume_24h, "
    f"  circulating_supply, total_supply, last_updated, "
    f"  change_pct_1h, change_pct_24h, change_pct_7d, change_pct_30d, change_pct_1y, "
    f"  price_1h_ago, price_24h_ago, price_7d_ago, price_30d_ago, price_1y_ago"
    f") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
)
DEL_PRICES_LIVE_RANKED_BUCKET = session.prepare(
    f"DELETE FROM {TABLE_LIVE_RANKED} WHERE bucket=?"
)

# Market-cap live & ranked
INS_MCAP_LIVE_UPSERT = session.prepare(
    f"INSERT INTO {TABLE_MCAP_LIVE} (category,last_updated,market_cap,market_cap_rank,volume_24h) VALUES (?,?,?,?,?)"
)
DEL_MCAP_RANKED_BUCKET = session.prepare(
    f"DELETE FROM {TABLE_MCAP_RANKED} WHERE category=?"
)
INS_MCAP_RANKED = session.prepare(
    f"INSERT INTO {TABLE_MCAP_RANKED} (category,market_cap_rank,last_updated,market_cap,volume_24h) VALUES (?,?,?,?,?)"
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
    warn_cutoff = now_ts - timedelta(minutes=STALE_WARN_MINUTES)

    rows = fetch_top_markets(TOP_N)

    # Debug preview
    if rows:
        sample = rows[:5]
        preview = [(r.get("id"), (r.get("symbol") or "").upper(), r.get("market_cap_rank"), r.get("last_updated")) for r in sample]
        print(f"[{now_str()}] [debug] sample[0:5]: {preview}")

    live_buffer = []
    w_hist = 0
    warn_count = 0
    missing_count = 0
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

        lu    = safe_iso_to_dt(c.get("last_updated"))
        if not gid or lu is None:
            missing_count += 1
            if VERBOSE_MODE:
                print(f"[{now_str()}] [skip-missing] id/last_updated missing for idx={idx}, id={gid}, {sym} {fmt_rank(rank)}")
            continue

        # Warn on staleness but DO NOT drop
        if lu < warn_cutoff:
            delta_min = round((now_ts - lu).total_seconds() / 60.0, 1)
            print(f"[{now_str()}] [stale-warn] {sym} {fmt_rank(rank)} lu={lu.isoformat()} ({delta_min}m old)")
            warn_count += 1

        price = f(c.get("current_price"))
        mcap  = f(c.get("market_cap"))
        vol   = f(c.get("total_volume"))

        ath_price = f(c.get("ath"))
        ath_date  = safe_iso_to_dt(c.get("ath_date"))

        circ = f(c.get("circulating_supply"))
        totl = f(c.get("total_supply"))
        maxs = f(c.get("max_supply"))

        # Percent changes from CG
        ch1h  = pct(c, "price_change_percentage_1h_in_currency")
        ch24h = pct(c, "price_change_percentage_24h_in_currency")
        ch7d  = pct(c, "price_change_percentage_7d_in_currency")
        ch30d = pct(c, "price_change_percentage_30d_in_currency")
        ch1y  = pct(c, "price_change_percentage_1y_in_currency")

        # Derived baselines
        p1h  = price_ago_from_pct(price, ch1h)
        p24h = price_ago_from_pct(price, ch24h)
        p7d  = price_ago_from_pct(price, ch7d)
        p30d = price_ago_from_pct(price, ch30d)
        p1y  = price_ago_from_pct(price, ch1y)

        cat = category_for(sym)

        mcap_total = float(mcap) if mcap is not None else 0.0
        vol_total = float(vol) if vol is not None else 0.0
        bump_total(cat, mcap_total, vol_total, lu)
        bump_total("ALL", mcap_total, vol_total, lu)

        # Buffer LIVE
        live_buffer.append([
            gid, sym, name, cat,
            int(rank) if rank is not None else None,
            price, mcap, vol,
            lu, now_ts,
            ath_price, ath_date,
            circ, totl, maxs,
            ch1h, ch24h, ch7d, ch30d, ch1y,
            p1h, p24h, p7d, p30d, p1y,
            "usd",
        ])

        # Write ROLLING (idempotent)
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
        try:
            res = session.execute(INS_ROLLING_IF_NOT_EXISTS, rolling_vals, timeout=REQUEST_TIMEOUT_SEC).one()
            if res and bool(getattr(res, "applied", True)):
                w_hist += 1
        except (WriteTimeout, OperationTimedOut, DriverException) as e:
            print(f"[{now_str()}] [rolling] insert failed for {sym}@{lu}: {e}")

        if (idx % 20 == 0) or VERBOSE_MODE:
            print(f"[{now_str()}] [progress] parsed={idx}/{len(rows)}; live_buf={len(live_buffer)} w_hist={w_hist}")

    # ───── Refresh gecko_prices_live (safe) ─────
    print(f"[{now_str()}] [live-buffer] prepared={len(live_buffer)} rows "
          f"(required_min={REQUIRED_LIVE_MIN}; missing_id/lu={missing_count}; stale_warns={warn_count})")
    if len(live_buffer) < REQUIRED_LIVE_MIN:
        print(f"[{now_str()}] [ABORT] Not enough rows to refresh {TABLE_LIVE}. Live table NOT cleared.")
        wrote_live = 0
    else:
        session.execute(SimpleStatement(f"TRUNCATE {TABLE_LIVE}"))
        print(f"[{now_str()}] TRUNCATED {TABLE_LIVE}")
        wrote_live = 0
        batch_live = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for i, vals in enumerate(live_buffer, 1):
            batch_live.add(INS_LIVE_UPSERT, vals)
            if (i % BATCH_FLUSH_EVERY) == 0:
                session.execute(batch_live); batch_live.clear()
            wrote_live += 1
        if len(batch_live):
            session.execute(batch_live)
        print(f"[{now_str()}] [live] inserted={wrote_live}")

    # ───── Build gecko_prices_live_ranked from the same buffer ─────
    def safe_rank_from_live(vals):
        r = vals[4]  # market_cap_rank position in live_vals
        try:
            return int(r) if r is not None and int(r) > 0 else 10**9
        except Exception:
            return 10**9

    rank_source = sorted(live_buffer, key=safe_rank_from_live)[:RANK_TOP_N]
    if len(rank_source) == 0:
        print(f"[{now_str()}] [prices_live_ranked] SKIP: no rows to write")
    else:
        session.execute(DEL_PRICES_LIVE_RANKED_BUCKET, [RANK_BUCKET], timeout=REQUEST_TIMEOUT_SEC)
        wrote_ranked_prices = 0
        batch_ranked = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for vals in rank_source:
            (gid, sym, name, cat, rnk,
             price, mcap, vol,
             lu, _now_ts,
             _ath_price, _ath_date,
             circ, totl, _maxs,
             ch1h, ch24h, ch7d, ch30d, ch1y,
             p1h, p24h, p7d, p30d, p1y,
             _vs) = vals

            batch_ranked.add(INS_PRICES_LIVE_RANKED, [
                RANK_BUCKET, int(rnk) if rnk is not None else None,
                gid, sym, name, cat,
                price, mcap, vol,
                circ, totl, lu,
                ch1h, ch24h, ch7d, ch30d, ch1y,
                p1h, p24h, p7d, p30d, p1y
            ])
            wrote_ranked_prices += 1
            if (wrote_ranked_prices % BATCH_FLUSH_EVERY) == 0:
                session.execute(batch_ranked); batch_ranked.clear()

        if len(batch_ranked):
            session.execute(batch_ranked)
        print(f"[{now_str()}] [prices_live_ranked] bucket='{RANK_BUCKET}' inserted={wrote_ranked_prices}")

    # ───── Market-cap aggregates (from buffer) ─────
    if category_totals:
        totals_items = []
        for cat_name, totals in category_totals.items():
            last_upd = totals.get("last_updated") or now_ts
            totals_items.append((cat_name, float(totals["market_cap"]), float(totals["volume_24h"]), last_upd))

        totals_items.sort(key=lambda entry: (0 if entry[0] == "ALL" else 1, entry[0].lower()))
        ranked_entries = [entry for entry in totals_items if entry[0] != "ALL"]
        ranked_entries.sort(key=lambda entry: entry[1], reverse=True)
        ranks = {cat: idx + 1 for idx, (cat, *_rest) in enumerate(ranked_entries)}
        if "ALL" in category_totals:
            ranks["ALL"] = 0

        # Refresh market_cap_live fully (safe because totals are ready)
        session.execute(SimpleStatement(f"TRUNCATE {TABLE_MCAP_LIVE}"))
        print(f"[{now_str()}] TRUNCATED {TABLE_MCAP_LIVE}")

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

        # Rebuild ranked (single partition = MCAP_RANK_BUCKET)
        try:
            session.execute(DEL_MCAP_RANKED_BUCKET, [MCAP_RANK_BUCKET], timeout=REQUEST_TIMEOUT_SEC)
        except (WriteTimeout, OperationTimedOut, DriverException) as e:
            print(f"[{now_str()}] [mcap-ranked] failed to clear category='{MCAP_RANK_BUCKET}': {e}")
        else:
            wrote_ranked = 0
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

    print(f"[{now_str()}] wrote: live={wrote_live}; ranked_prices={min(len(rank_source), RANK_TOP_N)}; "
          f"rolling_new={w_hist}; mcap_live_cats={len(category_totals)}; stale_warns={warn_count}")

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
