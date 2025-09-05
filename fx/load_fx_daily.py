# ingest_fx_daily_frankfurter.py
import os, requests, datetime as dt, time
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import RoundRobinPolicy
from cassandra.query import SimpleStatement
from dotenv import load_dotenv

load_dotenv()

BUNDLE   = os.getenv("ASTRA_BUNDLE_PATH", "secure-connect.zip")
TOKEN    = os.getenv("ASTRA_TOKEN")
KEYSPACE = os.getenv("ASTRA_KEYSPACE", "default_keyspace")

PROVIDER = "frankfurter"
BASE     = "USD"
TARGETS  = ["EUR","GBP","JPY","CNY","KRW","INR","AUD","CAD","CHF",
            "HKD","SGD","BRL","MXN","TRY","ZAR","IDR","TWD"]

# Weekend handling: carry-forward only (use last business day's rates)

def log(msg: str) -> None:
    try:
        now = dt.datetime.now().isoformat(timespec="seconds")
    except Exception:
        now = str(dt.datetime.now())
    print(f"[{now}] {msg}", flush=True)

def to_pydate(x):
    """Convert Cassandra driver Date/timestamp or strings to datetime.date."""
    if x is None:
        return None
    if isinstance(x, dt.date) and not isinstance(x, dt.datetime):
        return x
    if isinstance(x, dt.datetime):
        return x.date()
    try:
        # Some drivers return a Date-like with year/month/day
        if hasattr(x, "year") and hasattr(x, "month") and hasattr(x, "day"):
            return dt.date(int(x.year), int(x.month), int(x.day))
    except Exception:
        pass
    try:
        # Some expose .date() -> datetime.date
        if hasattr(x, "date") and callable(getattr(x, "date")):
            d = x.date()
            if isinstance(d, dt.date):
                return d
    except Exception:
        pass
    try:
        # Fallback parse from string 'YYYY-MM-DD'
        s = str(x)
        y, m, d = int(s[0:4]), int(s[5:7]), int(s[8:10])
        return dt.date(y, m, d)
    except Exception:
        return None

def get_latest(base, symbols):
    url = "https://api.frankfurter.app/latest"
    params = {"from": base, "to": ",".join(symbols)}
    for attempt in range(5):
        try:
            log(f"Fetching latest FX rates (attempt {attempt+1}/5) for base={base} ({len(symbols)} symbols)")
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            data = r.json()  # {'amount':1,'base':'USD','date':'YYYY-MM-DD','rates':{...}}
            log(f"Fetched date {data.get('date')} with {len(data.get('rates', {}))} symbol(s)")
            return data
        except Exception as e:
            wait = 2 ** attempt
            if attempt == 4:
                log(f"Failed to fetch latest rates: {e}")
                raise
            log(f"Attempt {attempt+1}/5 failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)

def cassandra_session():
    cloud_config = {"secure_connect_bundle": BUNDLE}
    log(f"Connecting to Cassandra keyspace '{KEYSPACE}' using bundle '{BUNDLE}'")
    auth = PlainTextAuthProvider("token", TOKEN)
    cluster = Cluster(cloud=cloud_config, auth_provider=auth,
                      execution_profiles={EXEC_PROFILE_DEFAULT: ExecutionProfile(load_balancing_policy=RoundRobinPolicy())})
    session = cluster.connect(KEYSPACE)
    log("Cassandra session established")
    return session

def frankfurter_range(start_date: str, end_date: str, base: str, symbols):
    url = f"https://api.frankfurter.app/{start_date}..{end_date}"
    params = {"from": base, "to": ",".join(symbols)}
    log(f"Fetching range {start_date}..{end_date} for base={base} ({len(symbols)} symbols)")
    for attempt in range(5):
        try:
            r = requests.get(url, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
            days = len(data.get("rates", {}))
            log(f"Range {start_date}..{end_date} returned {days} business day(s)")
            return data
        except Exception as e:
            wait = 2 ** attempt
            if attempt == 4:
                log(f"Failed range fetch {start_date}..{end_date}: {e}")
                raise
            log(f"Range attempt {attempt+1}/5 failed: {e}. Retrying in {wait}s...")
            time.sleep(wait)

def is_weekend(d: dt.date) -> bool:
    # Saturday=5, Sunday=6
    return d.weekday() >= 5

def daterange(start: dt.date, end: dt.date):
    cur = start
    while cur <= end:
        yield cur
        cur = cur + dt.timedelta(days=1)

def read_latest_status(s):
    row = s.execute("SELECT latest_date FROM fx_status WHERE provider=%s AND base=%s", (PROVIDER, BASE)).one()
    latest = row.latest_date if row else None
    log(f"Current DB latest_date: {latest}")
    return latest

def write_status(s, asof_date):
    log(f"Writing status latest_date={asof_date}")
    s.execute("""
        INSERT INTO fx_status (provider, base, latest_date, last_fetch_ts)
        VALUES (%s, %s, %s, toTimestamp(now()))
    """, (PROVIDER, BASE, asof_date))

_INS_PREPARED = False
_INS_WITH_SOURCE = False
_INS_DAILY = None
_INS_LIVE = None

def _prepare_inserts(s):
    global _INS_PREPARED, _INS_WITH_SOURCE, _INS_DAILY, _INS_LIVE
    if _INS_PREPARED:
        return
    try:
        _INS_DAILY = s.prepare("""
            INSERT INTO fx_rates_daily (provider, base, asof_date, symbol, rate, source)
            VALUES (?, ?, ?, ?, ?, ?)
        """)
        _INS_LIVE = s.prepare("""
            INSERT INTO fx_rates_live (provider, base, asof_date, symbol, rate, source)
            VALUES (?, ?, ?, ?, ?, ?)
        """)
        _INS_WITH_SOURCE = True
        log("Prepared INSERT statements with 'source' column")
    except Exception as e:
        _INS_DAILY = s.prepare("""
            INSERT INTO fx_rates_daily (provider, base, asof_date, symbol, rate)
            VALUES (?, ?, ?, ?, ?)
        """)
        _INS_LIVE = s.prepare("""
            INSERT INTO fx_rates_live (provider, base, asof_date, symbol, rate)
            VALUES (?, ?, ?, ?, ?)
        """)
        _INS_WITH_SOURCE = False
        log("'source' column not found; using inserts without it")
    finally:
        _INS_PREPARED = True

def publish_daily(s, asof_date, rates, source: str = "observed"):
    # 1) upsert history for this date
    _prepare_inserts(s)
    log(f"Upserting daily rates for {asof_date}: {len(rates)} symbol(s) [source={source}]")
    for sym in TARGETS:
        if sym in rates:
            if _INS_WITH_SOURCE:
                s.execute(_INS_DAILY, (PROVIDER, BASE, asof_date, sym, float(rates[sym]), source))
            else:
                s.execute(_INS_DAILY, (PROVIDER, BASE, asof_date, sym, float(rates[sym])))

    # 2) refresh live partition to this asof_date
    log(f"Refreshing live rates for {asof_date}: {len(rates)} symbol(s) [source={source}]")
    for sym in TARGETS:
        if sym in rates:
            if _INS_WITH_SOURCE:
                s.execute(_INS_LIVE, (PROVIDER, BASE, asof_date, sym, float(rates[sym]), source))
            else:
                s.execute(_INS_LIVE, (PROVIDER, BASE, asof_date, sym, float(rates[sym])))

def main():
    log(f"Starting daily FX load (provider={PROVIDER}, base={BASE})")
    s = cassandra_session()
    try:
        payload = get_latest(BASE, TARGETS)
        asof_str = payload.get("date")         # latest business date
        latest_rates = payload.get("rates", {})
        y,m,d = map(int, asof_str.split("-"))
        asof_date = dt.date(y,m,d)
        log(f"Latest payload date: {asof_date} ({len(latest_rates)} symbol(s))")

        prev_raw = read_latest_status(s)
        prev = to_pydate(prev_raw)
        if prev is None:
            # First run: publish latest business date only
            log("No previous status found; publishing latest only")
            publish_daily(s, asof_date, latest_rates, source="observed")
            write_status(s, asof_date)
            log(f"Published {asof_date} with {len(latest_rates)} symbol(s)")
        elif prev < asof_date:
            # There is a gap: fetch range prev..asof_date to include prev for weekend fill context
            start = prev
            end   = asof_date
            data = frankfurter_range(start.isoformat(), end.isoformat(), BASE, TARGETS)
            biz = {dt.date(*map(int, k.split("-"))): v for k, v in data.get("rates", {}).items()}

            def last_biz_before(day: dt.date):
                cur = day - dt.timedelta(days=1)
                while cur >= start:
                    if cur in biz:
                        return cur
                    cur -= dt.timedelta(days=1)
                return None

            # Walk each day after prev, publish available + synthetic weekends
            for day in daterange(start, end):
                if day == prev:
                    # context day; ensure present for weekend reference
                    continue

                if day in biz:
                    rates = biz[day]
                    log(f"Publishing business day {day}")
                    publish_daily(s, day, rates, source="observed")
                    write_status(s, day)
                    continue

                if is_weekend(day):
                    prev_biz = last_biz_before(day)
                    if prev_biz is None:
                        log(f"Skip {day}: no previous business day available for weekend fill")
                        continue
                    prev_rates = biz.get(prev_biz, {})
                    # carry forward only
                    filled = {sym: float(prev_rates[sym]) for sym in TARGETS if sym in prev_rates}
                    log(f"Publishing weekend {day} by carry-forward from {prev_biz} ({len(filled)} symbol(s))")
                    publish_daily(s, day, filled, source="carry")
                    write_status(s, day)
                    continue

                # Non-weekend missing day (holiday or data gap): leave empty to avoid synthetic values
                log(f"No data for {day} and not weekend; skipping synthetic fill")
        else:
            log(f"Already up-to-date for observed business date {asof_date}")

        # Project a placeholder for tomorrow using the latest observed rates
        tomorrow = dt.date.today() + dt.timedelta(days=1)
        log(f"Projecting placeholder for {tomorrow} from observed {asof_date}")
        publish_daily(s, tomorrow, latest_rates, source="projected")
        # Intentionally DO NOT advance fx_status for projected dates
    finally:
        log("Shutting down Cassandra session")
        s.shutdown()

if __name__ == "__main__":
    main()
