#!/usr/bin/env python3
# prices/BB_gck_update_prices_live_ranked.py
import os, sys, pathlib, time
from datetime import datetime

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
from cassandra.query import SimpleStatement, BatchStatement, ConsistencyLevel
from dotenv import load_dotenv

# Load .env from repo root explicitly
load_dotenv(dotenv_path=rel(".env"))

# ───────────────────────── Config ─────────────────────────
BUNDLE       = os.getenv("ASTRA_BUNDLE_PATH", "secure-connect.zip")
ASTRA_TOKEN  = os.getenv("ASTRA_TOKEN")
KEYSPACE     = os.getenv("ASTRA_KEYSPACE", "default_keyspace")

# tables
TABLE_SOURCE = os.getenv("TABLE_SOURCE", "gecko_prices_live")
TABLE_DEST   = os.getenv("TABLE_DEST",   "gecko_prices_live_ranked")
TABLE_CATS   = os.getenv("TABLE_CATS",   "asset_categories")
TABLE_MCAP_LIVE        = os.getenv("TABLE_GECKO_MCAP_LIVE", "gecko_market_cap_live")
TABLE_MCAP_LIVE_RANKED = os.getenv("TABLE_GECKO_MCAP_LIVE_RANKED", "gecko_market_cap_live_ranked")
MCAP_RANK_BUCKET       = os.getenv("GECKO_MCAP_BUCKET", "categories")

RANK_BUCKET  = os.getenv("RANK_BUCKET", "all")
RANK_TOP_N   = int(os.getenv("RANK_TOP_N", "200"))

REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "60"))
CONNECT_TIMEOUT_SEC = int(os.getenv("CONNECT_TIMEOUT_SEC", "15"))
FETCH_SIZE          = int(os.getenv("FETCH_SIZE", "1000"))
BATCH_FLUSH_EVERY   = int(os.getenv("BATCH_FLUSH_EVERY", "50"))

# optional env to control batch consistency (defaults to QUORUM)
CONSISTENCY_NAME = (os.getenv("CONSISTENCY") or "QUORUM").upper()
CONSISTENCY_MAP = {
    "ANY": ConsistencyLevel.ANY,
    "ONE": ConsistencyLevel.ONE,
    "TWO": ConsistencyLevel.TWO,
    "THREE": ConsistencyLevel.THREE,
    "QUORUM": ConsistencyLevel.QUORUM,
    "ALL": ConsistencyLevel.ALL,
    "LOCAL_QUORUM": ConsistencyLevel.LOCAL_QUORUM,
    "EACH_QUORUM": ConsistencyLevel.EACH_QUORUM,
    "LOCAL_ONE": ConsistencyLevel.LOCAL_ONE,
}
BATCH_CL = CONSISTENCY_MAP.get(CONSISTENCY_NAME, ConsistencyLevel.QUORUM)

if not BUNDLE or not ASTRA_TOKEN or not KEYSPACE:
    raise SystemExit("Missing ASTRA_BUNDLE_PATH / ASTRA_TOKEN / ASTRA_KEYSPACE")

def now_str(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

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

# ───────────────────────── Category map (from DB) ─────────────────────────
def load_category_map_from_db():
    """Return (id->category, symbol->category) dicts from TABLE_CATS."""
    id_to_cat, sym_to_cat = {}, {}
    stmt = SimpleStatement(
        f"SELECT id, symbol, category FROM {TABLE_CATS}",
        fetch_size=FETCH_SIZE
    )
    rows = session.execute(stmt, timeout=REQUEST_TIMEOUT_SEC)
    count = 0
    for r in rows:
        cid = (getattr(r, "id", None) or "").strip()
        sym = (getattr(r, "symbol", None) or "").strip()
        cat = (getattr(r, "category", None) or "").strip() or "Other"
        if cid:
            id_to_cat[cid.upper()] = cat
        if sym:
            sym_to_cat[sym.upper()] = cat
        count += 1
    print(f"[{now_str()}] [category] loaded {count} rows from {TABLE_CATS} "
          f"(id keys={len(id_to_cat)}, symbol keys={len(sym_to_cat)})")
    return id_to_cat, sym_to_cat

def category_for(id_to_cat, sym_to_cat, cid: str | None, sym: str | None) -> str:
    if cid:
        cat = id_to_cat.get(cid.upper())
        if cat:
            return cat
    if sym:
        cat = sym_to_cat.get(sym.upper())
        if cat:
            return cat
    return "Other"

# ───────────────────────── I/O statements ─────────────────────────
# Destination insert matches *new* schema (incl. change_pct_* and price_*_ago)
INS_RANKED = session.prepare(
    f"INSERT INTO {TABLE_DEST} ("
    f"  bucket, market_cap_rank, id, symbol, name, category, "
    f"  price_usd, market_cap, volume_24h, "
    f"  circulating_supply, total_supply, last_updated, "
    f"  change_pct_1h, change_pct_24h, change_pct_7d, change_pct_30d, change_pct_1y, "
    f"  price_1h_ago, price_24h_ago, price_7d_ago, price_30d_ago, price_1y_ago"
    f") VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
)

INS_MCAP_LIVE_UPSERT = session.prepare(
    f"INSERT INTO {TABLE_MCAP_LIVE} (category,id,name,symbol,market_cap,market_cap_rank,volume_24h,last_updated) VALUES (?,?,?,?,?,?,?,?)"
)
DEL_MCAP_RANKED_BUCKET = session.prepare(
    f"DELETE FROM {TABLE_MCAP_LIVE_RANKED} WHERE bucket=?"
)
INS_MCAP_RANKED = session.prepare(
    f"INSERT INTO {TABLE_MCAP_LIVE_RANKED} (bucket,market_cap_rank,category,id,name,symbol,market_cap,volume_24h,last_updated) VALUES (?,?,?,?,?,?,?,?,?)"
)

DEL_BUCKET = session.prepare(
    f"DELETE FROM {TABLE_DEST} WHERE bucket=?"
)

# Source select includes all fields we need
SEL_SOURCE = SimpleStatement(
    f"SELECT id, symbol, name, market_cap_rank, price_usd, market_cap, volume_24h, "
    f"circulating_supply, total_supply, last_updated, "
    f"change_pct_1h, change_pct_24h, change_pct_7d, change_pct_30d, change_pct_1y, "
    f"price_1h_ago, price_24h_ago, price_7d_ago, price_30d_ago, price_1y_ago "
    f"FROM {TABLE_SOURCE}",
    fetch_size=FETCH_SIZE
)

# ───────────────────────── Main ─────────────────────────
def main():
    id_to_cat, sym_to_cat = load_category_map_from_db()
    category_totals = {}

    def bump_total(cat_name: str, mcap_value: float, vol_value: float, last_upd) -> None:
        entry = category_totals.setdefault(cat_name, {"market_cap": 0.0, "volume_24h": 0.0, "last_updated": last_upd})
        entry["market_cap"] += mcap_value
        entry["volume_24h"] += vol_value
        if last_upd and (entry["last_updated"] is None or last_upd > entry["last_updated"]):
            entry["last_updated"] = last_upd

    # Read all current rows from source and sort by market_cap_rank
    rows = list(session.execute(SEL_SOURCE, timeout=REQUEST_TIMEOUT_SEC))

    def safe_rank(r):
        try:
            rk = getattr(r, "market_cap_rank", None)
            return int(rk) if rk is not None and int(rk) > 0 else 10**9
        except Exception:
            return 10**9

    rows.sort(key=safe_rank)
    top = rows[:RANK_TOP_N]

    # Refresh the destination bucket
    print(f"[{now_str()}] Deleting existing rows for bucket='{RANK_BUCKET}' in {TABLE_DEST} …")
    session.execute(DEL_BUCKET, [RANK_BUCKET], timeout=REQUEST_TIMEOUT_SEC)

    batch = BatchStatement(consistency_level=BATCH_CL)
    wrote = 0

    for r in top:
        cid   = getattr(r, "id", None)
        sym   = getattr(r, "symbol", None)
        name  = getattr(r, "name", None)
        rank  = safe_rank(r)
        price = getattr(r, "price_usd", None)
        mcap  = getattr(r, "market_cap", None)
        vol   = getattr(r, "volume_24h", None)
        circ  = getattr(r, "circulating_supply", None)
        tot   = getattr(r, "total_supply", None)
        lu    = getattr(r, "last_updated", None)

        ch1h  = getattr(r, "change_pct_1h", None)
        ch24h = getattr(r, "change_pct_24h", None)
        ch7d  = getattr(r, "change_pct_7d", None)
        ch30d = getattr(r, "change_pct_30d", None)
        ch1y  = getattr(r, "change_pct_1y", None)

        p1h   = getattr(r, "price_1h_ago", None)
        p24h  = getattr(r, "price_24h_ago", None)
        p7d   = getattr(r, "price_7d_ago", None)
        p30d  = getattr(r, "price_30d_ago", None)
        p1y   = getattr(r, "price_1y_ago", None)

        cat = category_for(id_to_cat, sym_to_cat, cid, sym)

        mcap_total = float(mcap) if mcap is not None else 0.0
        vol_total = float(vol) if vol is not None else 0.0
        bump_total(cat, mcap_total, vol_total, lu)
        bump_total("ALL", mcap_total, vol_total, lu)

        batch.add(INS_RANKED, [
            RANK_BUCKET, rank, cid, sym, name, cat,
            float(price) if price is not None else None,
            float(mcap) if mcap is not None else None,
            float(vol) if vol is not None else None,
            float(circ) if circ is not None else None,
            float(tot) if tot is not None else None,
            lu,
            float(ch1h) if ch1h is not None else None,
            float(ch24h) if ch24h is not None else None,
            float(ch7d) if ch7d is not None else None,
            float(ch30d) if ch30d is not None else None,
            float(ch1y) if ch1y is not None else None,
            float(p1h) if p1h is not None else None,
            float(p24h) if p24h is not None else None,
            float(p7d) if p7d is not None else None,
            float(p30d) if p30d is not None else None,
            float(p1y) if p1y is not None else None,
        ])
        wrote += 1
        if (wrote % BATCH_FLUSH_EVERY) == 0:
            session.execute(batch); batch.clear()

    if len(batch):
        session.execute(batch)

    if category_totals:
        totals_items = []
        for cat_name, totals in category_totals.items():
            last_upd = totals.get("last_updated") or datetime.utcnow()
            totals_items.append((cat_name, float(totals["market_cap"]), float(totals["volume_24h"]), last_upd))

        totals_items.sort(key=lambda entry: (0 if entry[0] == "ALL" else 1, entry[0].lower()))
        ranked_entries = [entry for entry in totals_items if entry[0] != "ALL"]
        ranked_entries.sort(key=lambda entry: entry[1], reverse=True)
        ranks = {cat: idx + 1 for idx, (cat, *_rest) in enumerate(ranked_entries)}
        if "ALL" in category_totals:
            ranks["ALL"] = 0

        print(f"[{now_str()}] [mcap-live] writing {len(totals_items)} category aggregates into {TABLE_MCAP_LIVE}")
        live_written = 0
        for cat_name, total_mcap, total_vol, last_upd in totals_items:
            cat_id = f"CATEGORY::{cat_name}"
            display_name = "All Categories" if cat_name == "ALL" else cat_name
            symbol_val = "ALL" if cat_name == "ALL" else cat_name.upper().replace(' ', '_')
            try:
                session.execute(
                    INS_MCAP_LIVE_UPSERT,
                    [cat_name, cat_id, display_name, symbol_val, total_mcap, ranks.get(cat_name), total_vol, last_upd],
                    timeout=REQUEST_TIMEOUT_SEC,
                )
                live_written += 1
            except (WriteTimeout, OperationTimedOut, DriverException) as e:
                print(f"[{now_str()}] [mcap-live] failed for category='{cat_name}': {e}")

        print(f"[{now_str()}] [mcap-live] rows_written={live_written}")

        try:
            session.execute(DEL_MCAP_RANKED_BUCKET, [MCAP_RANK_BUCKET], timeout=REQUEST_TIMEOUT_SEC)
        except (WriteTimeout, OperationTimedOut, DriverException) as e:
            print(f"[{now_str()}] [mcap-ranked] failed to clear bucket='{MCAP_RANK_BUCKET}': {e}")
        else:
            ranked_payload = []
            all_entry = next((entry for entry in totals_items if entry[0] == "ALL"), None)
            if all_entry:
                ranked_payload.append(("ALL", 0, all_entry[1], all_entry[2], all_entry[3]))
            ranked_payload.extend(
                [
                    (cat_name, ranks[cat_name], total_mcap, total_vol, last_upd)
                    for cat_name, total_mcap, total_vol, last_upd in ranked_entries
                ]
            )

            wrote_ranked = 0
            for cat_name, rank_value, total_mcap, total_vol, last_upd in ranked_payload:
                cat_id = f"CATEGORY::{cat_name}"
                display_name = "All Categories" if cat_name == "ALL" else cat_name
                symbol_val = "ALL" if cat_name == "ALL" else cat_name.upper().replace(' ', '_')
                try:
                    session.execute(
                        INS_MCAP_RANKED,
                        [MCAP_RANK_BUCKET, rank_value, cat_name, cat_id, display_name, symbol_val, total_mcap, total_vol, last_upd],
                        timeout=REQUEST_TIMEOUT_SEC,
                    )
                    wrote_ranked += 1
                except (WriteTimeout, OperationTimedOut, DriverException) as e:
                    print(f"[{now_str()}] [mcap-ranked] failed for category='{cat_name}' rank={rank_value}: {e}")
            print(f"[{now_str()}] [mcap-ranked] bucket='{MCAP_RANK_BUCKET}' rows_written={wrote_ranked}")
    else:
        print(f"[{now_str()}] [mcap-live] no category aggregates computed (rows=0)")

    print(f"[{now_str()}] Wrote rows={wrote} into {TABLE_DEST} for bucket='{RANK_BUCKET}' "
          f"(source={TABLE_SOURCE}, CL={CONSISTENCY_NAME})")

if __name__ == "__main__":
    try:
        main()
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass
