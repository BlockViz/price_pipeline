import os
from datetime import datetime, timedelta, timezone
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from cassandra import OperationTimedOut, ReadTimeout, Unavailable
from dotenv import load_dotenv

load_dotenv()

# ---- Config ----
BUNDLE = os.getenv("ASTRA_BUNDLE_PATH", "secure-connect.zip")
ASTRA_TOKEN = os.getenv("ASTRA_TOKEN")
KEYSPACE = os.getenv("ASTRA_KEYSPACE", "default_keyspace")
TOP_N = int(os.getenv("TOP_N", "110"))  # how many coins to process per run (top N)
REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "30"))

if not ASTRA_TOKEN:
    raise SystemExit("Missing ASTRA_TOKEN")

# ---- Connect ----
auth = PlainTextAuthProvider("token", ASTRA_TOKEN)
cluster = Cluster(cloud={"secure_connect_bundle": BUNDLE}, auth_provider=auth)
session = cluster.connect(KEYSPACE)

# Warm-up (helps on serverless cold start)
try:
    session.execute("SELECT release_version FROM system.local", timeout=REQUEST_TIMEOUT_SEC)
except Exception as e:
    print(f"[warmup] non-fatal: {e}")

# ---- Prepared statements ----
# live list (we sort by rank client-side)
SEL_LIVE = SimpleStatement("""
  SELECT id, symbol, name, rank, price_usd, market_cap, volume_24h, last_updated
  FROM prices_live
""")

# 15m points in UTC day range
SEL_15M_RANGE = session.prepare("""
  SELECT ts, price_usd, market_cap, volume_24h
  FROM prices_15m_7d
  WHERE id = ? AND ts >= ? AND ts < ?
""")

# previous-day row to get prev close if needed
SEL_PREV_DAY = session.prepare("""
  SELECT close, price_usd FROM prices_daily
  WHERE id = ? AND date = ? LIMIT 1
""")

# Upsert into prices_daily (Cassandra UPDATE is an upsert)
# We set OHLC + close + price_usd (aligned) + meta + source every run
UPD_DAILY = session.prepare("""
  UPDATE prices_daily SET
    symbol = ?, name = ?, rank = ?,
    price_usd = ?, market_cap = ?, volume_24h = ?, last_updated = ?,
    open = ?, high = ?, low = ?, close = ?, candle_source = ?
  WHERE id = ? AND date = ?
""")

def utc_today_bounds():
    """Return (start_of_day_utc, end_of_day_exclusive_utc, date_utc)."""
    now_utc = datetime.now(timezone.utc)
    day = now_utc.date()
    start = datetime(day.year, day.month, day.day, tzinfo=timezone.utc)
    end_excl = start + timedelta(days=1)
    return start, end_excl, day

def main():
    start_utc, end_excl_utc, date_utc = utc_today_bounds()

    # 1) load top coins
    rows = list(session.execute(SEL_LIVE, timeout=REQUEST_TIMEOUT_SEC))
    rows = [r for r in rows if isinstance(r.rank, int) and r.rank > 0]
    rows.sort(key=lambda r: r.rank)
    rows = rows[:TOP_N]

    upserts = 0
    src_15m = src_prev = src_flat = 0
    errors = 0

    for r in rows:
        try:
            # Try derive OHLC from today's 15m points first
            pts = session.execute(SEL_15M_RANGE, [r.id, start_utc, end_excl_utc], timeout=REQUEST_TIMEOUT_SEC)

            prices, mcaps, vols = [], [], []
            for p in pts:
                if p.price_usd is not None: prices.append(float(p.price_usd))
                if p.market_cap is not None: mcaps.append(float(p.market_cap))
                if p.volume_24h is not None: vols.append(float(p.volume_24h))

            if prices:
                # True intraday-derived candle for today
                o = prices[0]
                h = max(prices)
                l = min(prices)
                c = prices[-1]
                # choose end-of-period snapshot for market cap / volume if available; else last seen from live
                mcap = mcaps[-1] if mcaps else float(r.market_cap or 0.0)
                vol  = vols[-1] if vols else float(r.volume_24h or 0.0)
                source = "15m_derived"
                src_15m += 1
            else:
                # No 15m points yet today: use prev close as open (if available)
                prev_date = date_utc - timedelta(days=1)
                prev = session.execute(SEL_PREV_DAY, [r.id, prev_date], timeout=REQUEST_TIMEOUT_SEC).one()

                c = float(r.price_usd or 0.0)  # today's "close" snapshot from live
                if prev and (prev.close is not None or prev.price_usd is not None):
                    prev_close = float(prev.close if prev.close is not None else prev.price_usd)
                    o = prev_close
                    h = max(o, c)
                    l = min(o, c)
                    source = "daily_prevclose"
                    src_prev += 1
                else:
                    # first day or missing prev: flat candle
                    o = h = l = c
                    source = "daily_flat"
                    src_flat += 1

                mcap = float(r.market_cap or 0.0)
                vol  = float(r.volume_24h or 0.0)

            # Align price_usd with close for daily usage
            price_usd = c
            last_upd = r.last_updated

            session.execute(UPD_DAILY, (
                r.symbol, r.name, r.rank,
                price_usd, mcap, vol, last_upd,
                o, h, l, c, source,
                r.id, date_utc
            ), timeout=REQUEST_TIMEOUT_SEC)
            upserts += 1

        except (OperationTimedOut, ReadTimeout, Unavailable) as e:
            errors += 1
            print(f"[warn] {r.id} timed out: {e}")
            continue
        except Exception as e:
            errors += 1
            print(f"[warn] {r.id} failed: {e}")
            continue

    print(
        f"Daily OHLC upsert {date_utc} UTC → rows={upserts}, "
        f"15m={src_15m}, prevclose={src_prev}, flat={src_flat}, errors={errors}"
    )

if __name__ == "__main__":
    try:
        main()
    finally:
        cluster.shutdown()
