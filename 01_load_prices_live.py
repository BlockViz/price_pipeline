import os, time, csv, requests, random
from datetime import datetime, timezone, timedelta

from cassandra import OperationTimedOut, ReadTimeout, Unavailable, DriverException, WriteTimeout
from cassandra.cluster import Cluster, EXEC_PROFILE_DEFAULT, ExecutionProfile
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy
from cassandra.query import BatchStatement, SimpleStatement, ConsistencyLevel
from dotenv import load_dotenv

load_dotenv()

# ---- Config ----
BUNDLE              = os.getenv("ASTRA_BUNDLE_PATH")
ASTRA_TOKEN         = os.getenv("ASTRA_TOKEN")
KEYSPACE            = os.getenv("ASTRA_KEYSPACE", "default_keyspace")

TOP_N               = int(os.getenv("TOP_N", "100"))
RETRIES             = int(os.getenv("RETRIES", "3"))
BACKOFF_MIN         = int(os.getenv("BACKOFF_MIN", "5"))           # API retry backoff (minutes)

REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "60"))   # CQL per-query timeout
CONNECT_TIMEOUT_SEC = int(os.getenv("CONNECT_TIMEOUT_SEC", "15"))   # driver connect timeout
FETCH_SIZE          = int(os.getenv("FETCH_SIZE", "500"))           # CQL page size
BATCH_FLUSH_EVERY   = int(os.getenv("BATCH_FLUSH_EVERY", "40"))     # rows per batch flush

TRUNCATE_ATTEMPTS   = int(os.getenv("TRUNCATE_ATTEMPTS", "5"))
MIN_EXPECTED        = int(os.getenv("MIN_EXPECTED", max(50, TOP_N // 2)))

# unattended run control
RUN_FOREVER         = os.getenv("RUN_FOREVER", "0") == "1"          # loop forever
WAIT_STALE_SEC      = int(os.getenv("WAIT_STALE_SEC", "60"))        # sleep if data not newer
MAX_BACKOFF_MIN     = int(os.getenv("MAX_BACKOFF_MIN", "30"))       # cap on exponential backoff (minutes)

if not BUNDLE or not ASTRA_TOKEN or not KEYSPACE:
    raise SystemExit("Missing ASTRA_BUNDLE_PATH / ASTRA_TOKEN / ASTRA_KEYSPACE")

def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

# ---- Category mapping (CSV) ----
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CATEGORY_FILE = os.getenv("CATEGORY_FILE", os.path.join(BASE_DIR, "category_mapping.csv"))

def load_category_map(path: str) -> dict:
    m = {}
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            sample = f.read(2048); f.seek(0)
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
                print(f"[{now_str()}] [category] header not found in {path}. Expected: Symbol,Category")
    except FileNotFoundError:
        print(f"[{now_str()}] [category] file not found: {path} — defaulting to 'Other'")
    except Exception as e:
        print(f"[{now_str()}] [category] failed to read {path}: {e} — defaulting to 'Other'")
    return m

CATEGORY_MAP = load_category_map(CATEGORY_FILE)

def category_for(symbol: str) -> str:
    if not symbol:
        return "Other"
    return CATEGORY_MAP.get(symbol.upper(), "Other")

# ---- Connect (SCB + token auth) ----
print(f"[{now_str()}] Connecting to Astra (bundle='{BUNDLE}', keyspace='{KEYSPACE}')")
auth = PlainTextAuthProvider(username="token", password=ASTRA_TOKEN)

exec_profile = ExecutionProfile(
    load_balancing_policy=RoundRobinPolicy(),
    request_timeout=REQUEST_TIMEOUT_SEC,   # enforce default timeout
)

cluster = Cluster(
    cloud={"secure_connect_bundle": BUNDLE},
    auth_provider=auth,
    execution_profiles={EXEC_PROFILE_DEFAULT: exec_profile},
    connect_timeout=CONNECT_TIMEOUT_SEC,
)

session = cluster.connect(KEYSPACE)
print(f"[{now_str()}] Connected.")

# Warm-up
try:
    session.execute("SELECT release_version FROM system.local", timeout=REQUEST_TIMEOUT_SEC)
    print(f"[{now_str()}] Warm-up OK.")
except Exception as e:
    print(f"[{now_str()}] [warmup] non-fatal: {e}")

# Column list used for inserts/selects
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

# --- helpers ---
def exec_with_retry(stmt, params=None, timeout=REQUEST_TIMEOUT_SEC, attempts=TRUNCATE_ATTEMPTS, label=""):
    last = None
    for i in range(1, attempts + 1):
        try:
            return session.execute(stmt, params or [], timeout=timeout)
        except (OperationTimedOut, ReadTimeout, Unavailable, WriteTimeout, DriverException) as e:
            last = e
            wait = min(2 ** i, MAX_BACKOFF_MIN)
            print(f"[{now_str()}] [{label or 'CQL'}] attempt {i}/{attempts} failed: {e}. Retrying in {wait}s…")
            time.sleep(wait)
    raise last

def f(x):
    return float(x) if x is not None else None

def safe_iso_to_dt(s):
    if not s:
        return None
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def to_utc_aware(dt):
    """Return a timezone-aware UTC datetime for comparisons."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)

def prior_price_from_change(price_now, chg_frac):
    if price_now is None or chg_frac is None:
        return None
    denom = 1.0 + chg_frac
    if abs(denom) < 1e-12:
        return None
    return price_now / denom

def fetch_ranked(limit: int):
    url = "https://api.coinpaprika.com/v1/tickers?quotes=USD"
    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            print(f"[{now_str()}] [fetch] attempt {attempt}/{RETRIES} → requesting {url}")
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
                wait_min = min(BACKOFF_MIN * (2 ** (attempt - 1)), MAX_BACKOFF_MIN)
                jitter = random.uniform(0, 0.3 * wait_min)
                wait_s = int((wait_min + jitter) * 60)
                print(f"[{now_str()}] [fetch] error: {e}. Retrying in ~{wait_s}s…")
                time.sleep(wait_s)
            else:
                print(f"[{now_str()}] [fetch] giving up after {attempt} attempts.")
                raise
    raise last_err or RuntimeError("Unknown fetch error")

def write_rows(rows, prepared_stmt, now_ts):
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    count = 0
    flushed = 0
    t0 = time.time()
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
            now_ts,
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
        if count % BATCH_FLUSH_EVERY == 0:
            session.execute(batch); batch.clear()
            flushed += BATCH_FLUSH_EVERY
            print(f"[{now_str()}] [tmp] inserted {flushed}/{len(rows)}…")
    if len(batch) > 0:
        session.execute(batch)
        flushed += len(batch)
    print(f"[{now_str()}] [tmp] inserted total {count} rows in {time.time()-t0:.2f}s")
    return count

def max_last_updated(rows):
    mx = None
    for c in rows:
        lu = c.get("last_updated")
        if lu:
            dt = safe_iso_to_dt(lu)
            if not mx or dt > mx:
                mx = dt
    return mx

def run_once():
    """One full fetch→swap cycle. Returns True if swap happened, False if skipped for staleness."""
    now_ts = datetime.now(timezone.utc)

    # Prepare statements inside the run, one time.
    print(f"[{now_str()}] Preparing CQL statements…")
    t_prep = time.time()
    ins_tmp  = session.prepare(f"INSERT INTO prices_live_tmp ({INS_COLS}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
    ins_live = session.prepare(f"INSERT INTO prices_live     ({INS_COLS}) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
    print(f"[{now_str()}] Prepared in {time.time()-t_prep:.2f}s")

    # 1) Fetch FIRST
    rows = fetch_ranked(TOP_N)
    ids = [r.get("id") for r in rows]
    if len(set(ids)) != len(ids):
        print(f"[{now_str()}] [warn] duplicate CoinPaprika IDs in fetched set (unexpected).")

    # 1b) Stale-guard: skip swap if API hasn't moved (normalize tz)
    fetched_max = to_utc_aware(max_last_updated(rows))
    try:
        stmt = SimpleStatement("SELECT MAX(last_updated) AS mx FROM prices_live", fetch_size=FETCH_SIZE)
        live_max_row = session.execute(stmt, timeout=REQUEST_TIMEOUT_SEC).one()
        live_max = to_utc_aware(getattr(live_max_row, "mx", None))
        print(f"[{now_str()}] [live] current MAX(last_updated) = {live_max}")
    except Exception as e:
        print(f"[{now_str()}] [live] max(last_updated) check failed (non-fatal): {e}")
        live_max = None

    if live_max and fetched_max and fetched_max <= (live_max + timedelta(seconds=60)):
        print(f"[{now_str()}] [live] skip swap: fetched max {fetched_max} not newer than live {live_max}")
        return False  # not newer yet

    # 2) Write to TMP
    print(f"[{now_str()}] Truncating prices_live_tmp…")
    t0 = time.time()
    exec_with_retry("TRUNCATE prices_live_tmp", label="TRUNCATE tmp")
    print(f"[{now_str()}] Truncated tmp in {time.time()-t0:.2f}s")

    print(f"[{now_str()}] Writing {len(rows)} rows to prices_live_tmp…")
    wrote = write_rows(rows, ins_tmp, now_ts)

    # 3) Verify TMP rowcount
    print(f"[{now_str()}] Verifying tmp rowcount…")
    t0 = time.time()
    tmp_count_stmt = SimpleStatement("SELECT COUNT(*) FROM prices_live_tmp", fetch_size=None)
    tmp_count = session.execute(tmp_count_stmt, timeout=REQUEST_TIMEOUT_SEC).one()[0]
    print(f"[{now_str()}] Tmp has {tmp_count} rows (check in {time.time()-t0:.2f}s)")
    if tmp_count < MIN_EXPECTED:
        raise SystemExit(f"Aborting swap: tmp has only {tmp_count} rows (MIN_EXPECTED={MIN_EXPECTED}). Live table left untouched.")

    # 4) Swap into LIVE
    print(f"[{now_str()}] Truncating prices_live…")
    t0 = time.time()
    exec_with_retry("TRUNCATE prices_live", label="TRUNCATE live")
    print(f"[{now_str()}] Truncated live in {time.time()-t0:.2f}s")

    print(f"[{now_str()}] Copying tmp → live…")
    t0 = time.time()
    select_tmp_stmt = SimpleStatement("SELECT " + INS_COLS + " FROM prices_live_tmp", fetch_size=FETCH_SIZE)
    tmp_rows = session.execute(select_tmp_stmt, timeout=REQUEST_TIMEOUT_SEC)

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
        if copied % BATCH_FLUSH_EVERY == 0:
            session.execute(batch); batch.clear()
            print(f"[{now_str()}] [live] copied {copied}/{tmp_count}…")
    if len(batch) > 0:
        session.execute(batch)

    print(f"[{now_str()}] [live] copy complete: {copied} rows in {time.time()-t0:.2f}s")

    # 5) Clean TMP (optional)
    print(f"[{now_str()}] Cleaning tmp…")
    t0 = time.time()
    session.execute("TRUNCATE prices_live_tmp")
    print(f"[{now_str()}] Tmp cleaned in {time.time()-t0:.2f}s")

    print(f"[{now_str()}] Snapshot OK ✅ wrote {wrote} to tmp, copied {copied} → prices_live at {now_ts.isoformat()}")
    return True

def main_loop():
    """Run once, or forever with retry/sleep semantics for scheduling."""
    attempt = 0
    while True:
        try:
            swapped = run_once()
            if not RUN_FOREVER:
                return
            # If no swap (data not newer), wait a short period and try again.
            if not swapped:
                print(f"[{now_str()}] Waiting {WAIT_STALE_SEC}s for newer data…")
                time.sleep(WAIT_STALE_SEC)
            else:
                # Normal cadence after a successful swap (you can control via WAIT_STALE_SEC)
                time.sleep(WAIT_STALE_SEC)
            attempt = 0  # reset backoff after success
        except KeyboardInterrupt:
            print(f"\n[{now_str()}] KeyboardInterrupt — exiting loop.")
            return
        except Exception as e:
            attempt += 1
            wait_min = min((2 ** (attempt - 1)) * BACKOFF_MIN, MAX_BACKOFF_MIN)
            jitter = random.uniform(0, 0.3 * wait_min)
            wait_s = int((wait_min + jitter) * 60)
            print(f"[{now_str()}] ERROR in run: {e}. Backing off ~{wait_s}s before retry (attempt {attempt}).")
            time.sleep(wait_s)

if __name__ == "__main__":
    try:
        main_loop()
    finally:
        print(f"[{now_str()}] Shutting down…")
        try:
            cluster.shutdown()
        except Exception as e:
            print(f"[{now_str()}] Error during shutdown: {e}")
        print(f"[{now_str()}] Done.")
