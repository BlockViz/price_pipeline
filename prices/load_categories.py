"""Load asset categories into a single canonical table keyed by coinpaprika id.
Reads CSV (id, symbol?, category) and upserts rows into asset_categories.
"""

import os, csv
from datetime import datetime, timezone
import sys, pathlib

# Repo root & helpers
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

from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy
from dotenv import load_dotenv

load_dotenv(dotenv_path=rel(".env"))

BUNDLE       = os.getenv("ASTRA_BUNDLE_PATH", "secure-connect.zip")
ASTRA_TOKEN  = os.getenv("ASTRA_TOKEN")
KEYSPACE     = os.getenv("ASTRA_KEYSPACE", "default_keyspace")

CATEGORY_FILE = os.getenv("CATEGORY_FILE", str(rel("prices", "category_mapping.csv")))

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SEC", "30"))
CONNECT_TIMEOUT = int(os.getenv("CONNECT_TIMEOUT_SEC", "15"))

if not ASTRA_TOKEN:
    raise SystemExit("Missing ASTRA_TOKEN")

def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)

log(f"Config: bundle='{BUNDLE}', keyspace='{KEYSPACE}'")
log(f"Config: category_file='{CATEGORY_FILE}', timeouts(connect={CONNECT_TIMEOUT}s, request={REQUEST_TIMEOUT}s)")
log("Connecting to Astra")

auth = PlainTextAuthProvider("token", ASTRA_TOKEN)
exec_profile = ExecutionProfile(
    load_balancing_policy=RoundRobinPolicy(),
    request_timeout=REQUEST_TIMEOUT,
)
cluster = Cluster(
    cloud={"secure_connect_bundle": BUNDLE},
    auth_provider=auth,
    execution_profiles={EXEC_PROFILE_DEFAULT: exec_profile},
    connect_timeout=CONNECT_TIMEOUT,
)
s = cluster.connect(KEYSPACE)
log("Connected.")

UP_CAT = s.prepare("""
  INSERT INTO asset_categories (id, symbol, category, updated_at, source)
  VALUES (?, ?, ?, ?, ?)
""")

def autodetect_and_load(path: str) -> list[dict]:
    """
    Return records with keys: id (coinpaprika), symbol, category.
    Tries common delimiters and is robust to header cases.
    """
    records: list[dict] = []
    total_rows = 0
    used_delim = None
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        for delim in [",", ";", "\t", "|"]:
            f.seek(0)
            dr = csv.DictReader(f, delimiter=delim)
            headers = [h.strip().lower() for h in (dr.fieldnames or [])]
            if "id" in headers and "category" in headers:
                used_delim = delim
                id_key = dr.fieldnames[headers.index("id")]
                sym_key = dr.fieldnames[headers.index("symbol")] if "symbol" in headers else None
                cat_key = dr.fieldnames[headers.index("category")]
                for row in dr:
                    total_rows += 1
                    idv = (row.get(id_key) or "").strip()
                    sym = (row.get(sym_key) or "").strip().upper() if sym_key else None
                    cat = (row.get(cat_key) or "").strip() or "Other"
                    if idv:
                        records.append({"id": idv, "symbol": sym, "category": cat})
                break
    log(f"CSV parsed: delimiter='{used_delim}', raw_rows={total_rows}, records_kept={len(records)}")
    # de-dup by id, last write wins but log conflicts
    dedup: dict[str, dict] = {}
    conflicts = 0
    for rec in records:
        cid = rec["id"]
        if cid in dedup and dedup[cid]["category"] != rec["category"]:
            conflicts += 1
        dedup[cid] = rec
    if conflicts:
        log(f"Warning: category conflicts for {conflicts} id(s); last value kept")
    return list(dedup.values())

def main():
    records = autodetect_and_load(CATEGORY_FILE)
    now = datetime.now(timezone.utc)
    up_count = 0
    none_symbol = 0

    for rec in records:
        cid = rec["id"]
        sym = rec.get("symbol") or None
        cat = rec["category"]
        if sym is None:
            none_symbol += 1
        s.execute(UP_CAT, [cid, sym, cat, now, "manual_csv"])
        up_count += 1

    log(f"Loaded categories: rows_upserted={up_count}, records_no_symbol={none_symbol} (from {CATEGORY_FILE})")

if __name__ == "__main__":
    try:
        main()
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass

