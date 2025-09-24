#!/usr/bin/env python3
# prices/dq_audit_10m_gaps.py
# Lists missing UTC days (e.g., 2025-09-18) in the 10-minute table over the last N days per coin.

import os, sys, time, pathlib, datetime as dt
from datetime import timezone
from typing import Iterable

# ── Repo root & helpers ───────────────────────────────────────
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

# ── 3rd-party ─────────────────────────────────────────────────
from dotenv import load_dotenv
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy
from cassandra.query import SimpleStatement

load_dotenv(dotenv_path=rel(".env"))

# ── Config (matches your main script defaults) ────────────────
BUNDLE       = os.getenv("ASTRA_BUNDLE_PATH", "secure-connect.zip")
ASTRA_TOKEN  = os.getenv("ASTRA_TOKEN")
KEYSPACE     = os.getenv("ASTRA_KEYSPACE", "default_keyspace")

TABLE_LIVE    = os.getenv("TABLE_LIVE", "gecko_prices_live")
TEN_MIN_TABLE = os.getenv("TEN_MIN_TABLE", "gecko_prices_10m_7d")

REQUEST_TIMEOUT = int(os.getenv("DQ_REQUEST_TIMEOUT_SEC", "30"))
CONNECT_TIMEOUT = int(os.getenv("DQ_CONNECT_TIMEOUT_SEC", "15"))
FETCH_SIZE      = int(os.getenv("DQ_FETCH_SIZE", "500"))

TOP_N      = int(os.getenv("TOP_N_DQ", "210"))       # how many coins to check (by market_cap_rank)
WINDOW_D   = int(os.getenv("DQ_WINDOW_10M_DAYS", "7"))  # usually 7

# ── Safety ────────────────────────────────────────────────────
if not ASTRA_TOKEN:
    raise SystemExit("Missing ASTRA_TOKEN in environment (.env)")

# ── Small time helpers (UTC) ──────────────────────────────────
def utcnow() -> dt.datetime:
    return dt.datetime.now(timezone.utc)

def day_bounds_utc(d: dt.date):
    start = dt.datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    end_excl = start + dt.timedelta(days=1)
    return start, end_excl

def date_seq(last_inclusive: dt.date, days: int) -> list[dt.date]:
    start = last_inclusive - dt.timedelta(days=days-1)
    return [start + dt.timedelta(days=i) for i in range(days)]

def to_date(x) -> dt.date:
    if isinstance(x, dt.datetime):
        return x.astimezone(timezone.utc).date()
    if isinstance(x, dt.date):
        return x
    # fallback for driver types
    return dt.date.fromisoformat(str(x)[:10])

# ── Connect ───────────────────────────────────────────────────
print(f"[{dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Connecting to Astra…")
auth = PlainTextAuthProvider("token", ASTRA_TOKEN)
exec_profile = ExecutionProfile(load_balancing_policy=RoundRobinPolicy(),
                                request_timeout=REQUEST_TIMEOUT)
cluster = Cluster(cloud={"secure_connect_bundle": BUNDLE},
                  auth_provider=auth,
                  execution_profiles={EXEC_PROFILE_DEFAULT: exec_profile},
                  connect_timeout=CONNECT_TIMEOUT)
session = cluster.connect(KEYSPACE)
print("[ok] Connected.")

# ── Prepared / statements ─────────────────────────────────────
SEL_LIVE = SimpleStatement(
    f"SELECT id, symbol, name, market_cap_rank FROM {TABLE_LIVE}",
    fetch_size=FETCH_SIZE
)

SEL_10M_RANGE_DAYS = session.prepare(f"""
  SELECT ts FROM {TEN_MIN_TABLE}
  WHERE id = ? AND ts >= ? AND ts < ?
""")

# ── Core helpers ──────────────────────────────────────────────
def top_assets(limit: int):
    rows = list(session.execute(SEL_LIVE, timeout=REQUEST_TIMEOUT))
    rows = [r for r in rows if isinstance(r.market_cap_rank, int) and r.market_cap_rank > 0]
    rows.sort(key=lambda r: r.market_cap_rank)
    return rows[:limit]

def existing_days_10m(coin_id: str, start_dt: dt.datetime, end_dt: dt.datetime) -> set[dt.date]:
    have = set()
    for row in session.execute(SEL_10M_RANGE_DAYS, [coin_id, start_dt, end_dt], timeout=REQUEST_TIMEOUT):
        if getattr(row, "ts", None) is not None:
            have.add(to_date(row.ts))
    return have

def print_gap_report(window_days: int = WINDOW_D, top_n: int = TOP_N, only_with_gaps: bool = True):
    now = utcnow()
    # Our audit window is the last full UTC day-window (same as your main job)
    end_excl = dt.datetime(now.year, now.month, now.day, tzinfo=timezone.utc) + dt.timedelta(days=1)
    last_inclusive = end_excl.date() - dt.timedelta(days=1)

    want_days = set(date_seq(last_inclusive, window_days))
    start_dt = dt.datetime.combine(min(want_days), dt.time.min, tzinfo=timezone.utc)
    end_dt   = dt.datetime.combine(last_inclusive + dt.timedelta(days=1), dt.time.min, tzinfo=timezone.utc)

    coins = top_assets(top_n)

    print()
    print("────────────────────────────────────────────────────────────────────────────")
    print(f"10m audit window (UTC): {min(want_days)} → {last_inclusive}  (days={window_days})")
    print(f"Coins checked (by rank): {len(coins)}")
    print("────────────────────────────────────────────────────────────────────────────")

    total_with_gaps = 0
    worst = []  # (missing_count, symbol, id, missing_days_sorted)

    t0 = time.perf_counter()
    for i, c in enumerate(coins, 1):
        have = existing_days_10m(c.id, start_dt, end_dt)
        missing = sorted(want_days - have)
        if missing:
            total_with_gaps += 1
            worst.append((len(missing), c.symbol, c.id, missing))
            print(f"[{i:>3}/{len(coins)}] {c.symbol:<12} ({c.id})  rank={c.market_cap_rank:<4}  "
                  f"have={len(have):>2}/{window_days}  MISSING={len(missing)}")
            for d in missing:
                print(f"         · {d.isoformat()} (UTC day)")
        elif not only_with_gaps:
            print(f"[{i:>3}/{len(coins)}] {c.symbol:<12} ({c.id})  rank={c.market_cap_rank:<4}  OK ({len(have)}/{window_days})")

    dt_secs = time.perf_counter() - t0
    print("────────────────────────────────────────────────────────────────────────────")
    print(f"Done in {dt_secs:.2f}s. Coins with gaps: {total_with_gaps}/{len(coins)}")
    if worst:
        worst.sort(reverse=True)  # biggest gaps first
        top5 = worst[:5]
        print("Top gap offenders:")
        for miss_cnt, sym, cid, miss_days in top5:
            first = miss_days[0].isoformat()
            last  = miss_days[-1].isoformat()
            print(f"  · {sym:<12} ({cid})  missing {miss_cnt} day(s) — first:{first}, last:{last}")

# ── Main ──────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        print_gap_report()
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass
