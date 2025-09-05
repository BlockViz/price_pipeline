"""
Populate and refresh the `prices_live_by_rank` table from `prices_live`.

Schema target:
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

Strategy:
- Read all rows from `prices_live` (source), collect minimal fields.
- Sort client-side by `rank` asc and take top N.
- Delete the destination partition for `bucket` (default: 'all').
- Upsert rows in ranked order for that bucket.
"""

import os
import sys
import pathlib
from datetime import datetime
from typing import Any, Iterable

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

from dotenv import load_dotenv
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy
from cassandra.query import BatchStatement, SimpleStatement, ConsistencyLevel

load_dotenv(dotenv_path=rel(".env"))

BUNDLE       = os.getenv("ASTRA_BUNDLE_PATH", "secure-connect.zip")
ASTRA_TOKEN  = os.getenv("ASTRA_TOKEN")
KEYSPACE     = os.getenv("ASTRA_KEYSPACE", "default_keyspace")

TABLE_SOURCE = os.getenv("TABLE_SOURCE", "prices_live")
TABLE_DEST   = os.getenv("TABLE_DEST",   "prices_live_by_rank")

RANK_BUCKET  = os.getenv("RANK_BUCKET", "all")
RANK_TOP_N   = int(os.getenv("RANK_TOP_N", "200"))

REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "60"))
CONNECT_TIMEOUT_SEC = int(os.getenv("CONNECT_TIMEOUT_SEC", "15"))
FETCH_SIZE          = int(os.getenv("FETCH_SIZE", "1000"))
CONSISTENCY         = os.getenv("CONSISTENCY", "QUORUM").upper()

if not ASTRA_TOKEN:
    raise SystemExit("Missing ASTRA_TOKEN")

def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

print(f"[{now_str()}] Config: src='{TABLE_SOURCE}', dest='{TABLE_DEST}', bucket='{RANK_BUCKET}', topN={RANK_TOP_N}")
print(f"[{now_str()}] Connecting to Astra (bundle='{BUNDLE}', keyspace='{KEYSPACE}')")

auth = PlainTextAuthProvider("token", ASTRA_TOKEN)
exec_profile = ExecutionProfile(
    load_balancing_policy=RoundRobinPolicy(),
    request_timeout=REQUEST_TIMEOUT_SEC,
)
cluster = Cluster(
    cloud={"secure_connect_bundle": BUNDLE},
    auth_provider=auth,
    execution_profiles={EXEC_PROFILE_DEFAULT: exec_profile},
    connect_timeout=CONNECT_TIMEOUT_SEC,
)
session = cluster.connect(KEYSPACE)
print(f"[{now_str()}] Connected.")

CONS_MAP = {
    "ONE": ConsistencyLevel.ONE,
    "QUORUM": ConsistencyLevel.QUORUM,
    "LOCAL_QUORUM": ConsistencyLevel.LOCAL_QUORUM,
    "ALL": ConsistencyLevel.ALL,
}
CONS = CONS_MAP.get(CONSISTENCY, ConsistencyLevel.QUORUM)

SEL_SRC = SimpleStatement(
    f"SELECT id, symbol, name, rank, category, price_usd, market_cap, volume_24h, last_updated FROM {TABLE_SOURCE}",
    fetch_size=FETCH_SIZE,
)

INS_DEST = session.prepare(
    f"""
    INSERT INTO {TABLE_DEST}
      (bucket, rank, id, symbol, name, category, price_usd, market_cap, volume_24h, last_updated)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
)

DEL_BUCKET = session.prepare(
    f"DELETE FROM {TABLE_DEST} WHERE bucket=?"
)

def rows_iter(stmt: SimpleStatement) -> Iterable[Any]:
    result = session.execute(stmt, timeout=REQUEST_TIMEOUT_SEC)
    yield from result
    while result.has_more_pages:
        result = session.execute(stmt, timeout=REQUEST_TIMEOUT_SEC, paging_state=result.paging_state)
        yield from result

def main():
    # Read all, filter valid rank, sort and cap to top N
    items = []
    total = 0
    for r in rows_iter(SEL_SRC):
        total += 1
        rk = getattr(r, "rank", None)
        if rk is None:
            continue
        try:
            rk_i = int(rk)
        except Exception:
            continue
        if rk_i <= 0:
            continue
        items.append((rk_i, r))

    items.sort(key=lambda x: (x[0], getattr(x[1], "id", "")))
    top = items[:RANK_TOP_N]

    print(f"[{now_str()}] Source scanned: rows={total}, ranked={len(items)}, writing_top_n={len(top)}")

    # Replace destination partition
    session.execute(DEL_BUCKET, [RANK_BUCKET], timeout=REQUEST_TIMEOUT_SEC)
    print(f"[{now_str()}] Cleared destination bucket='{RANK_BUCKET}'")

    batch = BatchStatement(consistency_level=CONS)
    w = 0
    for rk_i, r in top:
        vals = [
            RANK_BUCKET,
            rk_i,
            getattr(r, "id", None),
            getattr(r, "symbol", None),
            getattr(r, "name", None),
            getattr(r, "category", None),
            getattr(r, "price_usd", None),
            getattr(r, "market_cap", None),
            getattr(r, "volume_24h", None),
            getattr(r, "last_updated", None),
        ]
        batch.add(INS_DEST, vals)
        w += 1
        if (w % 40) == 0:
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

