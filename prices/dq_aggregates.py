import datetime as dt
from collections import defaultdict
from time import perf_counter

from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement

from dq_config import (
    DRY_RUN,
    RECOMPUTE_MCAP_10M,
    RECOMPUTE_MCAP_HOURLY,
    RECOMPUTE_MCAP_DAILY,
    FULL_DAILY_ALL,
    FULL_MODE,
    TRUNCATE_AGGREGATES_IN_FULL,
    DAYS_10M,
    DAYS_HOURLY,
    DAYS_DAILY,
    phase_enabled,
)
from dq_cassandra import (
    session,
    exec_ps,
    INS_MCAP_10M,
    INS_MCAP_HOURLY,
    INS_MCAP_DAILY,
    SEL_10M_RANGE_FULL,
    SEL_HOURLY_META_RANGE,
    SEL_DAILY_FOR_ID_RANGE_ALL,
    SEL_DAILY_READ_FOR_AGG,
    SEL_DAILY_FIRST,
    SEL_DAILY_LAST,
    TRUNC_10M,
    TRUNC_H,
    TRUNC_D,
)
from dq_utils import _now_str, tdur, to_utc, floor_to_hour_utc, _to_pydate, _maybe_heartbeat

def compute_ranks(entries):
    non_all = [(cat, m) for (cat, m) in entries if cat != "ALL"]
    non_all.sort(key=lambda x: (x[1] or 0.0), reverse=True)
    ranks = {cat: i+1 for i,(cat,_m) in enumerate(non_all)}
    ranks["ALL"] = 0
    return ranks

def write_hourly_aggregates_from_map(hour_totals_map, metrics):
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    total = 0
    started = perf_counter()
    print(f"[{_now_str()}] [mcap_hourly] writing {len(hour_totals_map)} timestamp groups")
    for ts_hr, catmap in sorted(hour_totals_map.items()):
        entries = [(c, catmap[c][1]) for c in catmap]
        ranks = compute_ranks(entries)
        for cat, (lu, mc, vol) in catmap.items():
            rk = ranks.get(cat, None)
            if DRY_RUN: continue
            batch.add(INS_MCAP_HOURLY, [cat, ts_hr, lu or (ts_hr + dt.timedelta(hours=1) - dt.timedelta(seconds=1)), float(mc or 0.0), rk, float(vol or 0.0)])
            total += 1
            if (total % 100) == 0: session.execute(batch); batch.clear()
            if (total % 2000) == 0:
                print(f"[{_now_str()}] [mcap_hourly] wrote {total} rows so far ({tdur(started)})")
    if not DRY_RUN and len(batch): session.execute(batch)
    metrics["agg_rows_hourly"] += total
    print(f"[{_now_str()}] [mcap_hourly] wrote rows={total}")

def write_daily_aggregates_from_map(day_totals_map, metrics):
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    total = 0
    started = perf_counter()
    print(f"[{_now_str()}] [mcap_daily] writing {len(day_totals_map)} date groups")
    for d, catmap in sorted(day_totals_map.items()):
        entries = [(c, catmap[c][1]) for c in catmap]
        ranks = compute_ranks(entries)
        for cat, (lu, mc, vol) in catmap.items():
            rk = ranks.get(cat, None)
            if DRY_RUN: continue
            batch.add(INS_MCAP_DAILY, [cat, d, lu or (dt.datetime(d.year,d.month,d.day,23,59,59,tzinfo=dt.timezone.utc)), float(mc or 0.0), rk, float(vol or 0.0)])
            total += 1
            if (total % 100) == 0: session.execute(batch); batch.clear()
            if (total % 2000) == 0:
                print(f"[{_now_str()}] [mcap_daily] wrote {total} rows so far ({tdur(started)})")
    if not DRY_RUN and len(batch): session.execute(batch)
    metrics["agg_rows_daily"] += total
    print(f"[{_now_str()}] [mcap_daily] wrote rows={total}")

def write_10m_aggregates_from_map(min10_totals_map, metrics):
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    total = 0
    started = perf_counter()
    print(f"[{_now_str()}] [mcap_10m] writing {len(min10_totals_map)} 10m timestamp groups")
    for ts10, catmap in sorted(min10_totals_map.items()):
        entries = [(c, catmap[c][1]) for c in catmap]
        ranks = compute_ranks(entries)
        for cat, (lu, mc, vol) in catmap.items():
            rk = ranks.get(cat, None)
            if DRY_RUN: continue
            batch.add(INS_MCAP_10M, [cat, ts10, lu or ts10, float(mc or 0.0), rk, float(vol or 0.0)])
            total += 1
            if (total % 200) == 0: session.execute(batch); batch.clear()
            if (total % 5000) == 0:
                print(f"[{_now_str()}] [mcap_10m] wrote {total} rows so far ({tdur(started)})")
    if not DRY_RUN and len(batch): session.execute(batch)
    metrics["agg_rows_10m"] += total
    print(f"[{_now_str()}] [mcap_10m] wrote rows={total}")

def recompute_daily_full_all(coins, cat_for_id, metrics):
    print(f"[{_now_str()}] [FULL][daily_all] begin full-table recompute across {len(coins)} coins")
    acc = {}
    def add_point(d, cat, lu, mcap, vol):
        catmap = acc.setdefault(d, {})
        lu0, m0, v0 = catmap.get(cat, (None, 0.0, 0.0))
        lu_new = lu if (lu0 is None or (lu and lu > lu0)) else lu0
        catmap[cat] = (lu_new, m0 + mcap, v0 + vol)
        alu0, am0, av0 = catmap.get("ALL", (None, 0.0, 0.0))
        alu_new = lu if (alu0 is None or (lu and lu > alu0)) else alu0
        catmap["ALL"] = (alu_new, am0 + mcap, av0 + vol)

    for i, c in enumerate(coins, 1):
        if (i == 1) or (i % 25 == 0) or (i == len(coins)):
            print(f"[{_now_str()}] [FULL][daily_all] {i}/{len(coins)}: {c['id']}")
            _maybe_heartbeat(f"FULL daily_all coin {i}/{len(coins)}")
        srow = exec_ps(SEL_DAILY_FIRST, [c["id"]]).one()
        erow = exec_ps(SEL_DAILY_LAST,  [c["id"]]).one()
        if not srow or not erow: continue
        sdate = _to_pydate(srow.date); edate = _to_pydate(erow.date)
        rows = exec_ps(SEL_DAILY_FOR_ID_RANGE_ALL, [c["id"], sdate, edate])
        for r in rows:
            _maybe_heartbeat("FULL daily_all streaming rows")
            d = _to_pydate(r.date)
            lu = to_utc(getattr(r, "last_updated", None)) or dt.datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=dt.timezone.utc)
            mcap = float(getattr(r, "market_cap", 0.0) or 0.0)
            vol  = float(getattr(r, "volume_24h", 0.0) or 0.0)
            cat  = cat_for_id(c["id"])
            add_point(d, cat, lu, mcap, vol)
    write_daily_aggregates_from_map(acc, metrics)

def recompute_hourly_incremental(hours_set, coins, cat_for_id, metrics):
    if not hours_set: return
    total_ts = len(hours_set)
    print(f"[{_now_str()}] [mcap_hourly] incremental timestamps={total_ts} (summing across {len(coins)} coins)")
    hour_map = {h: defaultdict(lambda: (None, 0.0, 0.0)) for h in sorted(hours_set)}
    started = perf_counter()
    hours_sorted = sorted(hours_set)
    win_start = hours_sorted[0]
    win_end   = hours_sorted[-1] + dt.timedelta(hours=1)
    for idx_c, c in enumerate(coins, 1):
        if (idx_c % 50) == 0: _maybe_heartbeat(f"agg_hourly coin {idx_c}/{len(coins)} range")
        rows = exec_ps(SEL_HOURLY_META_RANGE, [c["id"], win_start, win_end])
        for r in rows:
            ts_raw = getattr(r, "ts", None)
            ts = floor_to_hour_utc(ts_raw) if isinstance(ts_raw, dt.datetime) else None
            if ts is None or ts not in hour_map:
                continue
            lu = to_utc(getattr(r, "last_updated", None))
            mcap = float(getattr(r, "market_cap", 0.0) or 0.0)
            vol  = float(getattr(r, "volume_24h", 0.0) or 0.0)
            cat = cat_for_id(c["id"])
            catmap = hour_map[ts]
            old_lu, old_m, old_v = catmap[cat]
            new_lu = lu if (old_lu is None or (lu and lu > old_lu)) else old_lu
            catmap[cat] = (new_lu, old_m + mcap, old_v + vol)
            a_lu, a_m, a_v = catmap["ALL"]
            a_new_lu = lu if (a_lu is None or (lu and lu > a_lu)) else a_lu
            catmap["ALL"] = (a_new_lu, a_m + mcap, a_v + vol)
    for idx_h, h in enumerate(hours_sorted, 1):
        if (idx_h % 50) == 0 or idx_h == total_ts:
            print(f"[{_now_str()}] [mcap_hourly] progress {idx_h}/{total_ts} hours ({tdur(started)})")
    write_hourly_aggregates_from_map(hour_map, metrics)

def recompute_daily_incremental(days_set, coins, cat_for_id, metrics):
    if not days_set: return
    total_days = len(days_set)
    print(f"[{_now_str()}] [mcap_daily] incremental dates={total_days} (summing across {len(coins)} coins)")
    day_map = {d: defaultdict(lambda: (None, 0.0, 0.0)) for d in sorted(days_set)}
    started = perf_counter()
    days_sorted = sorted(days_set)
    win_start = days_sorted[0]; win_end = days_sorted[-1]
    for idx_c, c in enumerate(coins, 1):
        if (idx_c % 50) == 0: _maybe_heartbeat(f"agg_daily coin {idx_c}/{len(coins)} range")
        rows = exec_ps(SEL_DAILY_FOR_ID_RANGE_ALL, [c["id"], win_start, win_end])
        for r in rows:
            d = _to_pydate(getattr(r, "date", None))
            if d is None or d not in day_map:
                continue
            lu = to_utc(getattr(r, "last_updated", None))
            mcap = float(getattr(r, "market_cap", 0.0) or 0.0)
            vol  = float(getattr(r, "volume_24h", 0.0) or 0.0)
            cat = cat_for_id(c["id"])
            catmap = day_map[d]
            old_lu, old_m, old_v = catmap[cat]
            new_lu = lu if (old_lu is None or (lu and lu > old_lu)) else old_lu
            catmap[cat] = (new_lu, old_m + mcap, old_v + vol)
            a_lu, a_m, a_v = catmap["ALL"]
            a_new_lu = lu if (a_lu is None or (lu and lu > a_lu)) else a_lu
            catmap["ALL"] = (a_new_lu, a_m + mcap, a_v + vol)
    for idx_d, d in enumerate(days_sorted, 1):
        if (idx_d % 25) == 0 or idx_d == total_days:
            print(f"[{_now_str()}] [mcap_daily] progress {idx_d}/{total_days} days ({tdur(started)})")
    write_daily_aggregates_from_map(day_map, metrics)

def recompute_10m_incremental(ts_set, coins, cat_for_id, metrics):
    if not ts_set: return
    total_ts = len(ts_set)
    print(f"[{_now_str()}] [mcap_10m] incremental timestamps={total_ts} (summing across {len(coins)} coins)")
    min10_map = {t: defaultdict(lambda: (None, 0.0, 0.0)) for t in sorted(ts_set)}
    started = perf_counter()
    ts_sorted = sorted(ts_set)
    win_start = ts_sorted[0]; win_end = ts_sorted[-1] + dt.timedelta(minutes=10)
    for idx_c, c in enumerate(coins, 1):
        if (idx_c % 50) == 0: _maybe_heartbeat(f"agg_10m coin {idx_c}/{len(coins)} range")
        rows = exec_ps(SEL_10M_RANGE_FULL, [c["id"], win_start, win_end])
        for r in rows:
            ts = to_utc(getattr(r, "ts", None))
            if ts is None or ts not in min10_map:
                continue
            lu = to_utc(getattr(r, "last_updated", None)) or ts
            mcap = float(getattr(r, "market_cap", 0.0) or 0.0)
            vol  = float(getattr(r, "volume_24h", 0.0) or 0.0)
            cat = cat_for_id(c["id"])
            catmap = min10_map[ts]
            old_lu, old_m, old_v = catmap[cat]
            new_lu = lu if (old_lu is None or (lu and lu > old_lu)) else old_lu
            catmap[cat] = (new_lu, old_m + mcap, old_v + vol)
            a_lu, a_m, a_v = catmap["ALL"]
            a_new_lu = lu if (a_lu is None or (lu and lu > a_lu)) else a_lu
            catmap["ALL"] = (a_new_lu, a_m + mcap, a_v + vol)
    for idx_t, t10 in enumerate(ts_sorted, 1):
        if (idx_t % 200) == 0 or idx_t == total_ts:
            print(f"[{_now_str()}] [mcap_10m] progress {idx_t}/{total_ts} stamps ({tdur(started)})")
    write_10m_aggregates_from_map(min10_map, metrics)

def run_aggregate_recompute(coins, cat_for_id, affected_hours, affected_days, affected_10m_ts, last_inclusive, budget_exhausted, metrics):
    print(f"[{_now_str()}] Aggregate recompute: FULL_MODE={FULL_MODE} | recompute 10m={RECOMPUTE_MCAP_10M} hourly={RECOMPUTE_MCAP_HOURLY} daily={RECOMPUTE_MCAP_DAILY}")
    if not FULL_MODE:
        print(f"[{_now_str()}] Incremental sets hours={len(affected_hours)} days={len(affected_days)} t10={len(affected_10m_ts)}")

    if budget_exhausted or not phase_enabled('aggregates'):
        print(f"[{_now_str()}] Aggregates skipped (budget_exhausted={budget_exhausted}, enabled={phase_enabled('aggregates')})")
        return

    if FULL_MODE:
        print(f"[{_now_str()}] [FULL] recomputing aggregates over full windows 10m={DAYS_10M}d hourly={DAYS_HOURLY}d daily={DAYS_DAILY}d")
        if TRUNCATE_AGGREGATES_IN_FULL:
            if RECOMPUTE_MCAP_10M:    session.execute(TRUNC_10M)
            if RECOMPUTE_MCAP_HOURLY: session.execute(TRUNC_H)
            if RECOMPUTE_MCAP_DAILY:  session.execute(TRUNC_D)
            print(f"[{_now_str()}] [FULL] truncated aggregate tables (as enabled)")

        if RECOMPUTE_MCAP_10M:
            win_start = dt.datetime.combine(last_inclusive - dt.timedelta(days=DAYS_10M-1), dt.time.min, tzinfo=dt.timezone.utc)
            win_end   = dt.datetime.combine(last_inclusive + dt.timedelta(days=1),        dt.time.min, tzinfo=dt.timezone.utc)
            print(f"[{_now_str()}] [FULL][10m] window {win_start} - {win_end}")
            acc = {}
            rows_seen = 0
            started = perf_counter()
            for idx,c in enumerate(coins,1):
                if (idx==1) or (idx%25==0) or (idx==len(coins)):
                    print(f"[{_now_str()}] [FULL][10m] id {idx}/{len(coins)}: {c['id']}")
                rows = exec_ps(SEL_10M_RANGE_FULL, [c["id"], win_start, win_end])
                for r in rows:
                    rows_seen += 1
                    ts = to_utc(getattr(r, "ts", None))
                    if ts is None: continue
                    mcap = float(getattr(r, "market_cap", 0.0) or 0.0)
                    vol  = float(getattr(r, "volume_24h", 0.0) or 0.0)
                    lu   = to_utc(getattr(r, "last_updated", None)) or (ts + dt.timedelta(hours=1) - dt.timedelta(seconds=1))
                    cat  = cat_for_id(c["id"])
                    catmap = acc.setdefault(ts, {})
                    lu0, m0, v0 = catmap.get(cat, (None, 0.0, 0.0))
                    lu_new = lu if (lu0 is None or (lu and lu > lu0)) else lu0
                    catmap[cat] = (lu_new, m0 + mcap, v0 + vol)
                    alu0, am0, av0 = catmap.get("ALL", (None, 0.0, 0.0))
                    alu_new = lu if (alu0 is None or (lu and lu > alu0)) else alu0
                    catmap["ALL"] = (alu_new, am0 + mcap, av0 + vol)
                    if (rows_seen % 100000) == 0:
                        print(f"[{_now_str()}] [FULL][10m] streamed {rows_seen} rows ({tdur(started)})")
            write_10m_aggregates_from_map(acc, metrics)

        if RECOMPUTE_MCAP_HOURLY:
            win_start = dt.datetime.combine(last_inclusive - dt.timedelta(days=DAYS_HOURLY-1), dt.time.min, tzinfo=dt.timezone.utc)
            win_end   = dt.datetime.combine(last_inclusive + dt.timedelta(days=1),          dt.time.min, tzinfo=dt.timezone.utc)
            print(f"[{_now_str()}] [FULL][hourly] window {win_start} - {win_end}")
            acc = {}
            rows_seen = 0
            started = perf_counter()
            for idx,c in enumerate(coins,1):
                if (idx==1) or (idx%25==0) or (idx==len(coins)):
                    print(f"[{_now_str()}] [FULL][hourly] id {idx}/{len(coins)}: {c['id']}")
                rows = exec_ps(SEL_HOURLY_META_RANGE, [c["id"], win_start, win_end])
                for r in rows:
                    rows_seen += 1
                    ts_raw = getattr(r, "ts", None)
                    ts = floor_to_hour_utc(ts_raw) if isinstance(ts_raw, dt.datetime) else None
                    if ts is None: continue
                    mcap = float(getattr(r, "market_cap", 0.0) or 0.0)
                    vol  = float(getattr(r, "volume_24h", 0.0) or 0.0)
                    lu   = to_utc(getattr(r, "last_updated", None)) or (ts + dt.timedelta(hours=1) - dt.timedelta(seconds=1))
                    cat  = cat_for_id(c["id"])
                    catmap = acc.setdefault(ts, {})
                    lu0, m0, v0 = catmap.get(cat, (None, 0.0, 0.0))
                    lu_new = lu if (lu0 is None or (lu and lu > lu0)) else lu0
                    catmap[cat] = (lu_new, m0 + mcap, v0 + vol)
                    alu0, am0, av0 = catmap.get("ALL", (None, 0.0, 0.0))
                    alu_new = lu if (alu0 is None or (lu and lu > alu0)) else alu0
                    catmap["ALL"] = (alu_new, am0 + mcap, av0 + vol)
                    if (rows_seen % 100000) == 0:
                        print(f"[{_now_str()}] [FULL][hourly] streamed {rows_seen} rows ({tdur(started)})")
            write_hourly_aggregates_from_map(acc, metrics)

        if RECOMPUTE_MCAP_DAILY:
            if FULL_DAILY_ALL:
                if TRUNCATE_AGGREGATES_IN_FULL:
                    session.execute(TRUNC_D)
                    print(f"[{_now_str()}] [FULL][daily_all] truncated daily aggregate")
                recompute_daily_full_all(coins, cat_for_id, metrics)
            else:
                win_start_d = last_inclusive - dt.timedelta(days=DAYS_DAILY-1)
                win_end_d   = last_inclusive
                print(f"[{_now_str()}] [FULL][daily] window {win_start_d} - {win_end_d}")
                acc = {}
                cur_d = win_start_d; all_days = []
                while cur_d <= win_end_d:
                    all_days.append(cur_d); cur_d += dt.timedelta(days=1)
                rows_seen = 0
                started = perf_counter()
                for idx,c in enumerate(coins,1):
                    if (idx==1) or (idx%25==0) or (idx==len(coins)):
                        print(f"[{_now_str()}] [FULL][daily] id {idx}/{len(coins)}: {c['id']}")
                    for d in all_days:
                        row = exec_ps(SEL_DAILY_READ_FOR_AGG, [c["id"], d]).one()
                        if not row: continue
                        rows_seen += 1
                        mcap = float(getattr(row,"market_cap",0.0) or 0.0)
                        vol  = float(getattr(row,"volume_24h",0.0) or 0.0)
                        lu   = to_utc(getattr(row,"last_updated",None)) or dt.datetime(d.year,d.month,d.day,23,59,59,tzinfo=dt.timezone.utc)
                        cat  = cat_for_id(c["id"])
                        catmap = acc.setdefault(d, {})
                        lu0, m0, v0 = catmap.get(cat, (None, 0.0, 0.0))
                        lu_new = lu if (lu0 is None or (lu and lu > lu0)) else lu0
                        catmap[cat] = (lu_new, m0 + mcap, v0 + vol)
                        alu0, am0, av0 = catmap.get("ALL", (None, 0.0, 0.0))
                        alu_new = lu if (alu0 is None or (lu and lu > alu0)) else lu0
                        catmap["ALL"] = (alu_new, am0 + mcap, av0 + vol)
                        if (rows_seen % 100000) == 0:
                            print(f"[{_now_str()}] [FULL][daily] streamed {rows_seen} rows ({tdur(started)})")
                write_daily_aggregates_from_map(acc, metrics)

    else:
        if RECOMPUTE_MCAP_HOURLY and affected_hours:
            recompute_hourly_incremental(affected_hours, coins, cat_for_id, metrics)
        if RECOMPUTE_MCAP_DAILY and affected_days:
            recompute_daily_incremental(affected_days, coins, cat_for_id, metrics)
        if RECOMPUTE_MCAP_10M and affected_10m_ts:
            recompute_10m_incremental(affected_10m_ts, coins, cat_for_id, metrics)
