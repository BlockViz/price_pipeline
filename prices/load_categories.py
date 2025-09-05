# prices/load_categories.py
import os, csv
from datetime import datetime, timezone
import sys, pathlib

# ───────────────────── Repo root & helpers ─────────────────────
# Make the backend repo root importable (two levels up from this file)
_REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.append(str(_REPO_ROOT))

# Try to use shared helper; fall back if not present
try:
    from paths import rel, chdir_repo_root
except Exception:
    def rel(*parts: str) -> pathlib.Path:
        return _REPO_ROOT.joinpath(*parts)
    def chdir_repo_root() -> None:
        os.chdir(_REPO_ROOT)

# Ensure consistent CWD (so relative files like secure-connect.zip work)
chdir_repo_root()

# ───────────────────────── 3rd-party ─────────────────────────
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy
from cassandra.query import SimpleStatement
from dotenv import load_dotenv

# Load .env from repo root explicitly
load_dotenv(dotenv_path=rel(".env"))

# ───────────────────────── Config ─────────────────────────
BUNDLE       = os.getenv("ASTRA_BUNDLE_PATH", "secure-connect.zip")
ASTRA_TOKEN  = os.getenv("ASTRA_TOKEN")
KEYSPACE     = os.getenv("ASTRA_KEYSPACE", "default_keyspace")

# allow overriding the live table used for symbol→id mapping
TABLE_LIVE   = os.getenv("TABLE_LIVE", os.getenv("TABLE_LATEST", "prices_live"))

# default CSV path now lives under prices/
CATEGORY_FILE = os.getenv("CATEGORY_FILE", str(rel("prices", "category_mapping.csv")))

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SEC", "30"))
CONNECT_TIMEOUT = int(os.getenv("CONNECT_TIMEOUT_SEC", "15"))

if not ASTRA_TOKEN:
    raise SystemExit("Missing ASTRA_TOKEN")

# ───────────────────────── Connect ─────────────────────────
print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Connecting to Astra (bundle='{BUNDLE}', keyspace='{KEYSPACE}')")
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
print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Connected.")

# ───────────────────────── Statements ─────────────────────────
UP_S = s.prepare("""
  INSERT INTO asset_categories_by_symbol (symbol, category, updated_at)
  VALUES (?, ?, ?)
""")
UP_ID = s.prepare("""
  INSERT INTO asset_categories_by_id (id, symbol, category, updated_at)
  VALUES (?, ?, ?, ?)
""")

# Pull current ids for symbols from live table (so we can fill the by_id table)
SYM_TO_ID = {}
for r in s.execute(SimpleStatement(f"SELECT id, symbol FROM {TABLE_LIVE}")):
    if r.symbol:
        SYM_TO_ID[r.symbol.upper()] = r.id

# ───────────────────────── CSV loader ─────────────────────────
def autodetect_and_load(path: str) -> dict[str, str]:
    """
    Return mapping SYMBOL -> CATEGORY. Tries common delimiters and is robust to header cases.
    """
    m: dict[str, str] = {}
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
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

# ───────────────────────── Main ─────────────────────────
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
        try:
            cluster.shutdown()
        except Exception:
            pass
