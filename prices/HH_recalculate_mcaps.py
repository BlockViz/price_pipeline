#!/usr/bin/env python3
# backend/prices/onetime_and_config/rebucket_mcap_from_mapping.py
#
# Rebuilds market-cap aggregates from existing price/candle tables using an
# ID->category mapping CSV. Missing IDs are mapped to "Other".
#
# SOURCE -> TARGET
#   gecko_prices_10m_7d        -> gecko_market_cap_10m_7d
#   gecko_candles_hourly_30d   -> gecko_market_cap_hourly_30d
#   gecko_candles_daily_contin -> gecko_market_cap_daily_contin
#
# Notes
# - Rank per bucket: ALL=0; categories ranked 1..N by market_cap DESC.
# - Verbose progress logs so you can see it working while it runs.
# - Targets are TRUNCATEd first, then fully repopulated.

import os, sys, pathlib, csv, time
from datetime import datetime, timezone, timedelta, date
from collections import defaultdict
from typing import Dict

# ───────────────────── Repo root & helpers ─────────────────────
_here = pathlib.Path(__file__).resolve()
_candidates = [_here.parents[2], _here.parents[1]]
for cand in _candidates:
    if (cand / "prices" / "category_mapping.csv").exists():
        _REPO_ROOT = cand
        break
else:
    _REPO_ROOT = _here.parents[2]

if str(_REPO_ROOT) not in sys.path:
    sys.path.append(str(_REPO_ROOT))

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
from cassandra.query import SimpleStatement, BatchStatement, ConsistencyLevel
from dotenv import load_dotenv

load_dotenv(dotenv_path=rel(".env"))

# ───────────────────────── Config ─────────────────────────
BUNDLE      = os.getenv("ASTRA_BUNDLE_PATH") or os.getenv("ASTRA_BUNDLE") or "secure-connect.zip"
ASTRA_TOKEN = os.getenv("ASTRA_TOKEN")
KEYSPACE    = os.getenv("ASTRA_KEYSPACE", "default_keyspace")

REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "60"))
CONNECT_TIMEOUT_SEC = int(os.getenv("CONNECT_TIMEOUT_SEC", "15"))
FETCH_SIZE          = int(os.getenv("FETCH_SIZE", "500"))
BATCH_FLUSH_EVERY   = int(os.getenv("BATCH_FLUSH_EVERY", "50"))

CATEGORY_FILE = os.getenv("CATEGORY_FILE", str(rel("prices", "category_mapping.csv")))

NOW_UTC       = datetime.now(timezone.utc)
WIN_10M_START = NOW_UTC - timedelta(days=7)
WIN_10M_END   = NOW_UTC + timedelta(minutes=10)
WIN_H_START   = NOW_UTC - timedelta(days=30)
WIN_H_END     = NOW_UTC + timedelta(hours=1)
# WIN_D_START = date.fromordinal(max(1, NOW_UTC.date().toordinal() - 365))
WIN_D_START = date(2013, 4, 28)
WIN_D_END     = NOW_UTC.date() + timedelta(days=1)

SRC_10M    = "gecko_prices_10m_7d"
SRC_HOURLY = "gecko_candles_hourly_30d"
SRC_DAILY  = "gecko_candles_daily_contin"

DST_10M    = "gecko_market_cap_10m_7d"
DST_HOURLY = "gecko_market_cap_hourly_30d"
DST_DAILY  = "gecko_market_cap_daily_contin"

def now_str(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def load_id_category_map(path: str) -> Dict[str, str]:
    m: Dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            for delim in [",", ";", "\t", "|"]:
                f.seek(0)
                reader = csv.DictReader(f, delimiter=delim)

                fieldnames = reader.fieldnames
                if not fieldnames:
                    # No header found for this delimiter, try the next one
                    continue

                headers = [h.strip().lower() for h in fieldnames]

                id_col = None
                cat_col = None

                if "id" in headers:
                    id_col = fieldnames[headers.index("id")]
                elif "symbol" in headers:
                    id_col = fieldnames[headers.index("symbol")]

                if "category" in headers:
                    cat_col = fieldnames[headers.index("category")]

                if id_col and cat_col:
                    for row in reader:
                        cid = (row.get(id_col) or "").strip().lower()
                        cat = (row.get(cat_col) or "").strip()
                        if cid:
                            m[cid] = cat or "Other"

                    prev = ", ".join([f"{k}->{m[k]}" for k in list(m.keys())[:10]])
                    print(
                        f"[{now_str()}] [category] loaded {len(m)} id->category rows from {path}\n"
                        f"           preview: {prev}"
                    )
                    break
            else:
                print(
                    f"[{now_str()}] [category] header not found in {path} "
                    f"(need id/category or symbol/category)."
                )
    except FileNotFoundError:
        print(f"[{now_str()}] [category] file not found: {path} -- will map missing IDs to 'Other'")
    except Exception as e:
        print(f"[{now_str()}] [category] failed to read {path}: {e} -- will map missing to 'Other'")
    return m

ID_TO_CAT = load_id_category_map(CATEGORY_FILE)
def cat_for_id(cid: str) -> str:
    return ID_TO_CAT.get((cid or "").lower(), "Other")

# ───────────────────────── Connect ─────────────────────────
if not BUNDLE or not ASTRA_TOKEN or not KEYSPACE:
    raise SystemExit("Missing ASTRA_BUNDLE_PATH / ASTRA_TOKEN / ASTRA_KEYSPACE")

print(f"[{now_str()}] [connect] bundle='{BUNDLE}', keyspace='{KEYSPACE}', fetch_size={FETCH_SIZE}, batch_flush={BATCH_FLUSH_EVERY}")
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

# ───────────────────────── Statements ─────────────────────────
SEL_IDS_LIVE = SimpleStatement(
    "SELECT id FROM gecko_prices_live",
    fetch_size=FETCH_SIZE
)

SEL_10M_RANGE = session.prepare(
    f"SELECT ts, last_updated, market_cap, volume_24h FROM {SRC_10M} WHERE id=? AND ts >= ? AND ts < ?"
)
SEL_H_RANGE = session.prepare(
    f"SELECT ts, last_updated, market_cap, volume_24h FROM {SRC_HOURLY} WHERE id=? AND ts >= ? AND ts < ?"
)
SEL_D_RANGE = session.prepare(
    f"SELECT date, last_updated, market_cap, volume_24h FROM {SRC_DAILY} WHERE id=? AND date >= ? AND date < ?"
)

TRUNC_10M = SimpleStatement(f"TRUNCATE {DST_10M}")
TRUNC_H   = SimpleStatement(f"TRUNCATE {DST_HOURLY}")
TRUNC_D   = SimpleStatement(f"TRUNCATE {DST_DAILY}")

INS_10M = session.prepare(
    f"INSERT INTO {DST_10M} (category, ts, last_updated, market_cap, market_cap_rank, volume_24h) "
    f"VALUES (?,?,?,?,?,?)"
)
INS_H = session.prepare(
    f"INSERT INTO {DST_HOURLY} (category, ts, last_updated, market_cap, market_cap_rank, volume_24h) "
    f"VALUES (?,?,?,?,?,?)"
)
INS_D = session.prepare(
    f"INSERT INTO {DST_DAILY} (category, date, last_updated, market_cap, market_cap_rank, volume_24h) "
    f"VALUES (?,?,?,?,?,?)"
)

# ───────────────────────── Aggregation ─────────────────────────
def agg_add(bucket_map, bucket_key, category, last_updated, market_cap, volume_24h):
    by_cat = bucket_map.setdefault(bucket_key, {})
    lu, mc, vol = by_cat.get(category, (None, 0.0, 0.0))
    new_lu = last_updated if (lu is None or (last_updated and last_updated > lu)) else lu
    by_cat[category] = (new_lu, (mc or 0.0) + (market_cap or 0.0), (vol or 0.0) + (volume_24h or 0.0))
    alu, amc, avol = by_cat.get("ALL", (None, 0.0, 0.0))
    anew_lu = last_updated if (alu is None or (last_updated and last_updated > alu)) else alu
    by_cat["ALL"] = (anew_lu, (amc or 0.0) + (market_cap or 0.0), (avol or 0.0) + (volume_24h or 0.0))

def aggregate_for_ids(ids):
    acc_10m = {}
    acc_hourly = {}
    acc_daily = {}
    n_ids = len(ids)
    t0 = time.time()
    for idx, cid in enumerate(ids, 1):
        if (idx == 1) or (idx % 10 == 0) or (idx == n_ids):
            print(f"[{now_str()}] [read] id {idx}/{n_ids}: {cid}")

        rows = session.execute(SEL_10M_RANGE, [cid, WIN_10M_START, WIN_10M_END], timeout=REQUEST_TIMEOUT_SEC)
        cnt = 0
        for r in rows:
            cnt += 1
            ts  = getattr(r, "ts", None)
            lu  = getattr(r, "last_updated", None)
            m   = getattr(r, "market_cap", None) or 0.0
            v   = getattr(r, "volume_24h", None) or 0.0
            cat = cat_for_id(cid)
            if ts is not None:
                agg_add(acc_10m, ts, cat, lu, m, v)
        if cnt and (idx % 20 == 0):
            print(f"[{now_str()}]   10m rows read so far for id#{idx}: {cnt} (elapsed {time.time()-t0:.1f}s)")

        rows = session.execute(SEL_H_RANGE, [cid, WIN_H_START, WIN_H_END], timeout=REQUEST_TIMEOUT_SEC)
        cnt = 0
        for r in rows:
            cnt += 1
            ts  = getattr(r, "ts", None)
            lu  = getattr(r, "last_updated", None)
            m   = getattr(r, "market_cap", None) or 0.0
            v   = getattr(r, "volume_24h", None) or 0.0
            cat = cat_for_id(cid)
            if ts is not None:
                agg_add(acc_hourly, ts, cat, lu, m, v)
        if cnt and (idx % 20 == 0):
            print(f"[{now_str()}]   hourly rows read so far for id#{idx}: {cnt} (elapsed {time.time()-t0:.1f}s)")

        rows = session.execute(SEL_D_RANGE, [cid, WIN_D_START, WIN_D_END], timeout=REQUEST_TIMEOUT_SEC)
        cnt = 0
        for r in rows:
            cnt += 1
            d   = getattr(r, "date", None)
            lu  = getattr(r, "last_updated", None)
            m   = getattr(r, "market_cap", None) or 0.0
            v   = getattr(r, "volume_24h", None) or 0.0
            cat = cat_for_id(cid)
            if d is not None:
                agg_add(acc_daily, d, cat, lu, m, v)
        if cnt and (idx % 20 == 0):
            print(f"[{now_str()}]   daily rows read so far for id#{idx}: {cnt} (elapsed {time.time()-t0:.1f}s)")

    return acc_10m, acc_hourly, acc_daily

# ───────────────────────── Writers (ranked) ─────────────────────────
def compute_ranks_for_bucket(entries):
    non_all = [(cat, mcap) for (cat, _lu, mcap, _vol) in entries if cat != "ALL"]
    non_all.sort(key=lambda x: (x[1] or 0.0), reverse=True)
    ranks = {cat: idx + 1 for idx, (cat, _m) in enumerate(non_all)}
    if any(cat == "ALL" for (cat, *_rest) in entries):
        ranks["ALL"] = 0
    return ranks

def ensure_all_bucket(catmap):
    """
    Rebuild the ALL bucket from the other categories to avoid stale/partial totals
    (and to keep rankings consistent even if aggregation failed for ALL earlier).
    """
    non_all = [(cat, vals) for cat, vals in catmap.items() if cat != "ALL"]
    if not non_all:
        return catmap

    latest_lu = None
    total_mcap = 0.0
    total_vol = 0.0
    for _cat, (lu, mcap, vol) in non_all:
        if lu is not None and (latest_lu is None or lu > latest_lu):
            latest_lu = lu
        total_mcap += mcap or 0.0
        total_vol += vol or 0.0

    catmap["ALL"] = (latest_lu, total_mcap, total_vol)
    return catmap

def flush_10m(session, acc_10m, INS_10M, batch_every=BATCH_FLUSH_EVERY):
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    total = 0
    t0 = time.time()
    for i, (ts, catmap) in enumerate(sorted(acc_10m.items()), 1):
        catmap = ensure_all_bucket(catmap)
        entries = [(cat, catmap[cat][0], catmap[cat][1], catmap[cat][2]) for cat in catmap]
        ranks = compute_ranks_for_bucket(entries)
        for cat, (lu, mcap, vol) in catmap.items():
            rk = ranks.get(cat)
            batch.add(INS_10M, [cat, ts, lu, mcap, rk, vol])
            total += 1
            if (total % batch_every) == 0:
                session.execute(batch); batch.clear()
                print(f"[{now_str()}] [write] flushed total={total}  (10m={total}, hourly=0, daily=0)  elapsed={time.time()-t0:.1f}s")
    if len(batch):
        session.execute(batch)
    print(f"[{now_str()}] [write] 10m done: rows={total}")
    return total

def flush_hourly(session, acc_hourly, INS_H, batch_every=BATCH_FLUSH_EVERY):
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    total = 0
    t0 = time.time()
    for i, (ts, catmap) in enumerate(sorted(acc_hourly.items()), 1):
        catmap = ensure_all_bucket(catmap)
        entries = [(cat, catmap[cat][0], catmap[cat][1], catmap[cat][2]) for cat in catmap]
        ranks = compute_ranks_for_bucket(entries)
        for cat, (lu, mcap, vol) in catmap.items():
            rk = ranks.get(cat)
            batch.add(INS_H, [cat, ts, lu, mcap, rk, vol])
            total += 1
            if (total % batch_every) == 0:
                session.execute(batch); batch.clear()
                print(f"[{now_str()}] [write] flushed total={total}  (10m=0, hourly={total}, daily=0)  elapsed={time.time()-t0:.1f}s")
    if len(batch):
        session.execute(batch)
    print(f"[{now_str()}] [write] hourly done: rows={total}")
    return total

def flush_daily(session, acc_daily, INS_D, batch_every=BATCH_FLUSH_EVERY):
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    total = 0
    t0 = time.time()
    for i, (d, catmap) in enumerate(sorted(acc_daily.items()), 1):
        catmap = ensure_all_bucket(catmap)
        entries = [(cat, catmap[cat][0], catmap[cat][1], catmap[cat][2]) for cat in catmap]
        ranks = compute_ranks_for_bucket(entries)
        for cat, (lu, mcap, vol) in catmap.items():
            rk = ranks.get(cat)
            batch.add(INS_D, [cat, d, lu, mcap, rk, vol])
            total += 1
            if (total % batch_every) == 0:
                session.execute(batch); batch.clear()
                print(f"[{now_str()}] [write] flushed total={total}  (10m=0, hourly=0, daily={total})  elapsed={time.time()-t0:.1f}s")
    if len(batch):
        session.execute(batch)
    print(f"[{now_str()}] [write] daily done: rows={total}")
    return total

# ───────────────────────── Main ─────────────────────────
def main():
    print(f"[{now_str()}] Windows: 10m[{WIN_10M_START.isoformat()} -> {WIN_10M_END.isoformat()}) "
          f"hourly[{WIN_H_START.isoformat()} -> {WIN_H_END.isoformat()}) "
          f"daily[{WIN_D_START} -> {WIN_D_END})")

    ids_live = [r.id for r in session.execute(SEL_IDS_LIVE, timeout=REQUEST_TIMEOUT_SEC)]
    print(f"[{now_str()}] IDs from gecko_prices_live: {len(ids_live)}")

    t0 = time.time()
    acc_10m, acc_hourly, acc_daily = aggregate_for_ids(ids_live)
    print(f"[{now_str()}] Aggregation complete in {time.time()-t0:.1f}s "
          f"(10m buckets={len(acc_10m)}, hourly buckets={len(acc_hourly)}, daily buckets={len(acc_daily)})")

    print(f"[{now_str()}] TRUNCATING targets ...")
    session.execute(TRUNC_10M)
    session.execute(TRUNC_H)
    session.execute(TRUNC_D)
    print(f"[{now_str()}] Truncated: {DST_10M}, {DST_HOURLY}, {DST_DAILY}")

    w10 = flush_10m(session, acc_10m, INS_10M, batch_every=BATCH_FLUSH_EVERY)
    wh  = flush_hourly(session, acc_hourly, INS_H, batch_every=BATCH_FLUSH_EVERY)
    wd  = flush_daily(session, acc_daily, INS_D, batch_every=BATCH_FLUSH_EVERY)

    print(f"[{now_str()}] DONE. wrote: 10m={w10}, hourly={wh}, daily={wd}")

if __name__ == "__main__":
    try:
        main()
    finally:
        try:
            cluster.shutdown()
        except Exception:
            pass
