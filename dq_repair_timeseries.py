import os, time, requests, datetime as dt
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, BatchStatement
from cassandra import ConsistencyLevel
from dotenv import load_dotenv

load_dotenv()

# ---------- Config ----------
BUNDLE      = os.getenv("ASTRA_BUNDLE_PATH", "secure-connect.zip")
ASTRA_TOKEN = os.getenv("ASTRA_TOKEN")
KEYSPACE    = os.getenv("ASTRA_KEYSPACE", "default_keyspace")

TOP_N_DQ    = int(os.getenv("TOP_N_DQ", "110"))
DQ_MAX_COINS          = int(os.getenv("DQ_MAX_COINS", "110"))            # monitor cap
DQ_MAX_API_COINS_RUN  = int(os.getenv("DQ_MAX_API_COINS_PER_RUN", "20")) # API cap per run

DAYS_15M    = int(os.getenv("DQ_WINDOW_15M_DAYS", "7"))
DAYS_DAILY  = int(os.getenv("DQ_WINDOW_DAILY_DAYS", "365"))

RETRIES     = int(os.getenv("DQ_RETRIES", "3"))
BACKOFF_S   = int(os.getenv("DQ_BACKOFF_SEC", "4"))
PAUSE_S     = float(os.getenv("DQ_PAUSE_PER_COIN", "0.2"))

# Logging controls
LOG_EVERY   = int(os.getenv("DQ_LOG_EVERY", "10"))  # heartbeat every N coins
VERBOSE     = os.getenv("DQ_VERBOSE", "0") == "1"   # per-coin detailed prints
TIME_API    = os.getenv("DQ_TIME_API", "1") == "1"  # show API timings

if not ASTRA_TOKEN:
    raise SystemExit("Missing ASTRA_TOKEN")

# ---------- Small logging helpers ----------
def ts():
    return dt.datetime.now().strftime("%H:%M:%S")

def tdur(start):
    return f"{(time.perf_counter()-start):.2f}s"

# ---------- Connect ----------
auth    = PlainTextAuthProvider("token", ASTRA_TOKEN)
cluster = Cluster(cloud={"secure_connect_bundle": BUNDLE}, auth_provider=auth)
session = cluster.connect(KEYSPACE)

# Inserts
INS_15M = session.prepare("""
  INSERT INTO prices_15m_7d (id, ts, symbol, name, rank, price_usd, market_cap, volume_24h, last_updated)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""")
INS_DAY = session.prepare("""
  INSERT INTO prices_daily (id, date, symbol, name, rank, price_usd, market_cap, volume_24h, last_updated)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# Reads
SEL_15M_RANGE = session.prepare("""
  SELECT ts FROM prices_15m_7d
  WHERE id = ? AND ts >= ? AND ts < ?
""")
SEL_15M_DAY_LAST = session.prepare("""
  SELECT ts, price_usd, market_cap, volume_24h FROM prices_15m_7d
  WHERE id=? AND ts>=? AND ts<? LIMIT 1
""")
SEL_DAILY_RANGE = session.prepare("""
  SELECT date FROM prices_daily
  WHERE id = ? AND date >= ? AND date <= ?
""")
SEL_DAILY_ONE = session.prepare("""
  SELECT date, price_usd, market_cap, volume_24h, last_updated FROM prices_daily
  WHERE id=? AND date=? LIMIT 1
""")

# ---------- Helpers ----------
def utcnow(): return dt.datetime.now(dt.timezone.utc)

def round_up_to_next_day_utc(d: dt.datetime) -> dt.datetime:
    d = d.astimezone(dt.timezone.utc)
    if (d.hour, d.minute, d.second, d.microsecond) == (0,0,0,0):
        return d
    next_day = (d + dt.timedelta(days=1)).date()
    return dt.datetime(next_day.year, next_day.month, next_day.day, tzinfo=dt.timezone.utc)

def date_seq(end_date: dt.date, days: int):
    return [end_date - dt.timedelta(days=i) for i in range(days)][::-1]

def paprika_get(url, params=None):
    last = None
    t0 = time.perf_counter()
    for i in range(RETRIES):
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 200:
            if TIME_API:
                print(f"[{ts()}] API OK {url.split('/')[-2:]}, took {tdur(t0)}")
            return r.json()
        msg = r.text[:200].replace("\n"," ")
        print(f"[{ts()}] API {url} -> {r.status_code}: {msg}")
        if r.status_code in (402,429,500,502,503,504):
            sleep_for = BACKOFF_S*(i+1)
            print(f"[{ts()}]  …backing off {sleep_for}s (attempt {i+1}/{RETRIES})")
            time.sleep(sleep_for); continue
        try: r.raise_for_status()
        except Exception as e: last = e
    raise RuntimeError(f"Paprika failed: {url} :: {last}")

def iso_z(dt_obj: dt.datetime) -> str:
    return dt_obj.astimezone(dt.timezone.utc).isoformat().replace("+00:00","Z")

def to_dt(iso_s: str) -> dt.datetime:
    return dt.datetime.fromisoformat(iso_s.replace("Z","+00:00")).astimezone(dt.timezone.utc)

def top_assets(limit: int):
    t0 = time.perf_counter()
    rows = list(session.execute(SimpleStatement("SELECT id, symbol, name, rank FROM prices_live")))
    rows = [r for r in rows if isinstance(r.rank, int) and r.rank > 0]
    rows.sort(key=lambda r: r.rank)
    rows = rows[:limit]
    print(f"[{ts()}] Loaded {len(rows)} assets from prices_live in {tdur(t0)}")
    return rows

def existing_days_15m(coin_id: str, start_dt: dt.datetime, end_dt: dt.datetime):
    t0 = time.perf_counter()
    have = set()
    rs = session.execute(SEL_15M_RANGE, [coin_id, start_dt, end_dt])
    for row in rs:
        have.add(row.ts.date())
    if VERBOSE:
        print(f"  · fetched 15m days={len(have)} in {tdur(t0)}")
    return have

def existing_days_daily(coin_id: str, start_date: dt.date, end_date: dt.date):
    t0 = time.perf_counter()
    have = set()
    rs = session.execute(SEL_DAILY_RANGE, [coin_id, start_date, end_date])
    for row in rs:
        have.add(row.date)
    if VERBOSE:
        print(f"  · fetched daily days={len(have)} in {tdur(t0)}")
    return have

# ---------- Local repair first ----------
def repair_daily_from_15m(coin, missing_days: set[dt.date]) -> int:
    if not missing_days: return 0
    t0 = time.perf_counter()
    cnt = 0
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for day in sorted(missing_days):
        day_start = dt.datetime(day.year, day.month, day.day, tzinfo=dt.timezone.utc)
        day_end   = day_start + dt.timedelta(days=1)
        last = session.execute(SEL_15M_DAY_LAST, [coin.id, day_start, day_end]).one()
        if not last:
            continue
        ts, price, mcap, vol = last.ts, float(last.price_usd or 0.0), float(last.market_cap or 0.0), float(last.volume_24h or 0.0)
        batch.add(INS_DAY, (coin.id, day, coin.symbol, coin.name, int(coin.rank), price, mcap, vol, ts))
        cnt += 1
        if cnt % 40 == 0:
            session.execute(batch); batch.clear()
    if len(batch) > 0: session.execute(batch)
    if cnt and VERBOSE:
        print(f"  · local daily from 15m: +{cnt} rows ({tdur(t0)})")
    return cnt

def repair_15m_from_daily(coin, missing_days: set[dt.date]) -> int:
    if not missing_days: return 0
    t0 = time.perf_counter()
    cnt = 0
    batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    for day in sorted(missing_days):
        row = session.execute(SEL_DAILY_ONE, [coin.id, day]).one()
        if not row:
            continue
        ts = dt.datetime(day.year, day.month, day.day, 23, 59, 59, tzinfo=dt.timezone.utc)
        price = float(row.price_usd or 0.0)
        mcap  = float(row.market_cap or 0.0)
        vol   = float(row.volume_24h or 0.0)
        last_updated = row.last_updated or ts
        batch.add(INS_15M, (coin.id, ts, coin.symbol, coin.name, int(coin.rank), price, mcap, vol, last_updated))
        cnt += 1
        if cnt % 40 == 0:
            session.execute(batch); batch.clear()
    if len(batch) > 0: session.execute(batch)
    if cnt and VERBOSE:
        print(f"  · local 15m from daily: +{cnt} rows ({tdur(t0)})")
    return cnt

# ---------- API repair ----------
def backfill_from_api(coin, need_15m: set[dt.date], need_daily: set[dt.date]) -> tuple[int,int]:
    if not need_15m and not need_daily:
        return 0, 0
    days_all = sorted(need_15m.union(need_daily))
    start_day = days_all[0]
    end_day   = days_all[-1] + dt.timedelta(days=1)  # exclusive
    start_dt  = dt.datetime(start_day.year, start_day.month, start_day.day, tzinfo=dt.timezone.utc)
    end_dt    = dt.datetime(end_day.year, end_day.month, end_day.day, tzinfo=dt.timezone.utc)

    url = f"https://api.coinpaprika.com/v1/tickers/{coin.id}/historical"
    t0 = time.perf_counter()
    try:
        data = paprika_get(url, params={
            "start": iso_z(start_dt),
            "end":   iso_z(end_dt),
            "interval": "1d",
            "quote": "usd"
        })
    except Exception as e:
        print(f"  · API backfill failed for {coin.id}: {e} ({tdur(t0)})")
        return 0, 0

    by_day = {}
    for d in data:
        ts_dt = to_dt(d["timestamp"])
        day = ts_dt.date()
        by_day[day] = {
            "ts": ts_dt,
            "price": float(d.get("price") or 0.0),
            "mcap":  float(d.get("market_cap") or 0.0),
            "vol":   float(d.get("volume_24h") or 0.0),
        }

    fixed_15m = fixed_daily = 0
    if need_15m:
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        cnt = 0
        for day in sorted(need_15m):
            v = by_day.get(day)
            if not v: continue
            batch.add(INS_15M, (coin.id, v["ts"], coin.symbol, coin.name, int(coin.rank),
                                v["price"], v["mcap"], v["vol"], v["ts"]))
            cnt += 1
            if cnt % 40 == 0: session.execute(batch); batch.clear()
        if len(batch) > 0: session.execute(batch)
        fixed_15m = cnt

    if need_daily:
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        cnt = 0
        for day in sorted(need_daily):
            v = by_day.get(day)
            if not v: continue
            batch.add(INS_DAY, (coin.id, day, coin.symbol, coin.name, int(coin.rank),
                                v["price"], v["mcap"], v["vol"], v["ts"]))
            cnt += 1
            if cnt % 40 == 0: session.execute(batch); batch.clear()
        if len(batch) > 0: session.execute(batch)
        fixed_daily = cnt

    if VERBOSE:
        print(f"  · API fixed 15m={fixed_15m}, daily={fixed_daily} ({tdur(t0)})")
    return fixed_15m, fixed_daily

# ---------- Main ----------
def main():
    now = utcnow()
    end_excl   = round_up_to_next_day_utc(now)  # exclusive end
    start_15m  = round_up_to_next_day_utc(end_excl - dt.timedelta(days=DAYS_15M))
    start_daily= round_up_to_next_day_utc(end_excl - dt.timedelta(days=DAYS_DAILY))

    want_15m_days   = set(date_seq(end_excl.date() - dt.timedelta(days=1), DAYS_15M))
    want_daily_days = set(date_seq(end_excl.date() - dt.timedelta(days=1), DAYS_DAILY))

    assets = top_assets(min(TOP_N_DQ, DQ_MAX_COINS))
    print(f"[{ts()}] DQ windows → 15m: {start_15m.date()}→{(end_excl.date()-dt.timedelta(days=1))} | "
          f"daily: {start_daily.date()}→{(end_excl.date()-dt.timedelta(days=1))}")

    coins_needing_api = []
    fixed15_local = fixedD_local = 0

    t_all = time.perf_counter()
    for i, coin in enumerate(assets, 1):
        t_coin = time.perf_counter()
        if VERBOSE:
            print(f"[{ts()}] [{i}/{len(assets)}] {coin.rank:>3} {coin.symbol} ({coin.id})")

        have_15m   = existing_days_15m(coin.id, start_15m, end_excl)
        have_daily = existing_days_daily(coin.id, start_daily.date(), (end_excl.date()-dt.timedelta(days=1)))

        need_15m   = want_15m_days   - have_15m
        need_daily = want_daily_days - have_daily

        if VERBOSE:
            print(f"  · need_15m={len(need_15m)} need_daily={len(need_daily)}")

        # Local repairs
        if need_daily:
            fixed = repair_daily_from_15m(coin, need_daily)
            fixedD_local += fixed
            if fixed and VERBOSE:
                print(f"  · after local daily fix: +{fixed}")
            # recalc missing
            have_daily = existing_days_daily(coin.id, start_daily.date(), (end_excl.date()-dt.timedelta(days=1)))
            need_daily = want_daily_days - have_daily
            if VERBOSE:
                print(f"  · remaining need_daily={len(need_daily)}")

        if need_15m:
            fixed = repair_15m_from_daily(coin, need_15m)
            fixed15_local += fixed
            if fixed and VERBOSE:
                print(f"  · after local 15m fix: +{fixed}")
            # recalc missing
            have_15m = existing_days_15m(coin.id, start_15m, end_excl)
            need_15m = want_15m_days - have_15m
            if VERBOSE:
                print(f"  · remaining need_15m={len(need_15m)}")

        if need_15m or need_daily:
            coins_needing_api.append((coin, need_15m, need_daily))

        if (i % LOG_EVERY == 0) and not VERBOSE:
            print(f"[{ts()}] Progress {i}/{len(assets)} (this coin {tdur(t_coin)})")

        time.sleep(PAUSE_S)

    # API pass (limited)
    api_targets = coins_needing_api[:DQ_MAX_API_COINS_RUN]
    print(f"[{ts()}] Coins still missing after local repair: {len(coins_needing_api)}; "
          f"API target this run: {len(api_targets)} (cap={DQ_MAX_API_COINS_RUN})")

    fixed15_api = fixedD_api = 0
    for j, (coin, need_15m, need_daily) in enumerate(api_targets, 1):
        if VERBOSE:
            print(f"[{ts()}] API repair {j}/{len(api_targets)} → {coin.symbol} ({coin.id}) "
                  f"need_15m={len(need_15m)} need_daily={len(need_daily)}")
        f15, fD = backfill_from_api(coin, need_15m, need_daily)
        fixed15_api += f15
        fixedD_api  += fD
        time.sleep(PAUSE_S)

    print(f"[{ts()}] DONE in {tdur(t_all)} | Local fixes → 15m:{fixed15_local} daily:{fixedD_local} | "
          f"API fixes → 15m:{fixed15_api} daily:{fixedD_api} | "
          f"Remain for future API runs: {max(0, len(coins_needing_api)-len(api_targets))}")
    

if __name__ == "__main__":
    try:
        main()
    finally:
        cluster.shutdown()
