"""
Populate and refresh the `prices_live_by_rank` table from `prices_live`,
recomputing category from the canonical `asset_categories` table.

Target schema:
  CREATE TABLE default_keyspace.prices_live_by_rank (
    bucket text,
    rank int,
    id text,
    symbol text,
    name text,
    category text,
    price_usd double,
    market_cap double,
    volume_24h double,
    last_updated timestamp,
    PRIMARY KEY ((bucket), rank, id)
  ) WITH CLUSTERING ORDER BY (rank ASC, id ASC);
"""

import os, sys, pathlib, time
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
from cassandra.query import SimpleStatement, BatchStatement, ConsistencyLevel
from dotenv import load_dotenv

# Load .env from repo root explicitly
load_dotenv(dotenv_path=rel(".env"))

# ───────────────────────── Config ─────────────────────────
BUNDLE       = os.getenv("ASTRA_BUNDLE_PATH", "secure-connect.zip")
ASTRA_TOKEN  = os.getenv("ASTRA_TOKEN")
KEYSPACE     = os.getenv("ASTRA_KEYSPACE", "default_keyspace")

TABLE_SOURCE = os.getenv("TABLE_SOURCE", "prices_live")
TABLE_DEST   = os.getenv("TABLE_DEST",   "prices_live_by_rank")
TABLE_CATS   = os.getenv("TABLE_CATS",   "asset_categories")

RANK_BUCKET  = os.getenv("RANK_BUCKET", "all")
RANK_TOP_N   = int(os.getenv("RANK_TOP_N", "200"))

REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "60"))
CONNECT_TIMEOUT_SEC = int(os.getenv("CONNECT_TIMEOUT_SEC", "15"))
FETCH_SIZE          = int(os.getenv("FETCH_SIZE", "1000"))
BATCH_FLUSH_EVERY   = int(os.getenv("BATCH_FLUSH_EVERY", "50"))

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
def load_category_map_from_db() -> tuple[dict, dict]:
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

def category_for(id_to_cat: dict, sym_to_cat: dict, cid: str | None, sym: str | None) -> str:
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
INS_RANKED = session.prepare(
    f"INSERT INTO {TABLE_DEST} (bucket, rank, id, symbol, name, category, "
    f"price_usd, market_cap, volume_24h, last_updated) "
    f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
)

DEL_BUCKET = session.prepare(
    f"DELETE FROM {TABLE_DEST} WHERE bucket=?"
)

# Note: Cassandra cannot ORDER BY arbitrary columns; we scan and sort client-side.
SEL_SOURCE = SimpleStatement(
    f"SELECT id, symbol, name, rank, price_usd, market_cap, volume_24h, last_updated "
    f"FROM {TABLE_SOURCE}",
    fetch_size=FETCH_SIZE
)

# ───────────────────────── Main ─────────────────────────
def main():
    id_to_cat, sym_to_cat = load_category_map_from_db()

    # Read all current rows from source and sort by rank
    rows = list(session.execute(SEL_SOURCE, timeout=REQUEST_TIMEOUT_SEC))
    def safe_rank(r):
        try:
            rk = getattr(r, "rank", None)
            return int(rk) if rk is not None and int(rk) > 0 else 10**9
        except Exception:
            return 10**9

    rows.sort(key=safe_rank)
    top = rows[:RANK_TOP_N]

    # Refresh the destination bucket
    print(f"[{now_str()}] Deleting existing rows for bucket='{RANK_BUCKET}'…")
    session.execute(DEL_BUCKET, [RANK_BUCKET], timeout=REQUEST_TIMEOUT_SEC)

    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    w = 0

    for r in top:
        cid   = getattr(r, "id", None)
        sym   = getattr(r, "symbol", None)
        name  = getattr(r, "name", None)
        rank  = int(getattr(r, "rank", None) or 0)
        price = float(getattr(r, "price_usd", None) or 0.0) if getattr(r, "price_usd", None) is not None else None
        mcap  = float(getattr(r, "market_cap", None) or 0.0) if getattr(r, "market_cap", None) is not None else None
        vol   = float(getattr(r, "volume_24h", None) or 0.0) if getattr(r, "volume_24h", None) is not None else None
        lu    = getattr(r, "last_updated", None)

        cat = category_for(id_to_cat, sym_to_cat, cid, sym)

        batch.add(INS_RANKED, [
            RANK_BUCKET, rank, cid, sym, name, cat, price, mcap, vol, lu
        ])
        w += 1
        if (w % BATCH_FLUSH_EVERY) == 0:
            session.execute(batch); batch.clear()

    if len(batch):
        session.execute(batch)

    print(f"[{now_str()}] Wrote rows={w} into {TABLE_DEST} for bucket='{RANK_BUCKET}'")

if __name__ == "__main__":
    try:
        main()
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass
