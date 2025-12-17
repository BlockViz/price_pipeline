from astra_connect.connect import get_session, AstraConfig
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement, BatchStatement

from dq_config import (
    TABLE_LIVE,
    TEN_MIN_TABLE,
    DAILY_TABLE,
    HOURLY_TABLE,
    MONTHLY_TABLE,
    COIN_DAILY_RANGES,
    COIN_INTRADAY_BITS,
    MCAP_10M_TABLE,
    MCAP_HOURLY_TABLE,
    MCAP_DAILY_TABLE,
    FETCH_SIZE,
    REQUEST_TIMEOUT,
)

AstraConfig.from_env()  # required to load creds/secure bundle/keyspace
session, cluster = get_session(return_cluster=True)

def exec_ps(ps, params, timeout=REQUEST_TIMEOUT, fetch_size=FETCH_SIZE):
    """Bind with fetch_size and execute to avoid buffering entire partitions."""
    bound = ps.bind(params)
    bound.fetch_size = fetch_size
    return session.execute(bound, timeout=timeout)

# include category from live
SEL_LIVE = SimpleStatement(
    f"SELECT id, symbol, name, market_cap_rank, category FROM {TABLE_LIVE}",
    fetch_size=FETCH_SIZE
)

SEL_DAILY_DISTINCT_IDS = SimpleStatement(
    f"SELECT DISTINCT id FROM {DAILY_TABLE}",
    fetch_size=FETCH_SIZE
)

SEL_10M_RANGE_FULL = session.prepare(f"""
  SELECT ts, price_usd, market_cap, volume_24h,
         market_cap_rank, circulating_supply, total_supply, last_updated
  FROM {TEN_MIN_TABLE}
  WHERE id = ? AND ts >= ? AND ts < ? ORDER BY ts ASC
""")
SEL_10M_ONE = session.prepare(f"""
  SELECT last_updated, market_cap, volume_24h
  FROM {TEN_MIN_TABLE}
  WHERE id = ? AND ts = ? LIMIT 1
""")

SEL_DAILY_ONE = session.prepare(f"""
  SELECT date, symbol, name,
         price_usd, market_cap, volume_24h, last_updated,
         open, high, low, close, candle_source,
         market_cap_rank, circulating_supply, total_supply
  FROM {DAILY_TABLE}
  WHERE id = ? AND date = ? LIMIT 1
""")
SEL_DAILY_READ_FOR_AGG = session.prepare(f"""
  SELECT last_updated, market_cap, volume_24h
  FROM {DAILY_TABLE}
  WHERE id = ? AND date = ? LIMIT 1
""")

SEL_DAILY_FIRST = session.prepare(f"""
  SELECT date FROM {DAILY_TABLE}
  WHERE id = ? ORDER BY date ASC LIMIT 1
""")
SEL_DAILY_LAST = session.prepare(f"""
  SELECT date FROM {DAILY_TABLE}
  WHERE id = ? ORDER BY date DESC LIMIT 1
""")
SEL_DAILY_FOR_ID_RANGE_ALL = session.prepare(f"""
  SELECT date, last_updated, market_cap, volume_24h
  FROM {DAILY_TABLE}
  WHERE id = ? AND date >= ? AND date <= ?
""")

SEL_HOURLY_META_RANGE = session.prepare(f"""
  SELECT ts, price_usd, market_cap, volume_24h, candle_source, last_updated
  FROM {HOURLY_TABLE}
  WHERE id = ? AND ts >= ? AND ts < ?
""")
SEL_HOURLY_ONE = session.prepare(f"""
  SELECT last_updated, market_cap, volume_24h
  FROM {HOURLY_TABLE}
  WHERE id = ? AND ts = ? LIMIT 1
""")
SEL_PREV_CLOSE = session.prepare(f"""
  SELECT ts, close FROM {HOURLY_TABLE}
  WHERE id = ? AND ts < ? ORDER BY ts DESC LIMIT 1
""")

INS_10M = session.prepare(f"""
  INSERT INTO {TEN_MIN_TABLE}
    (id, ts, symbol, name, price_usd, market_cap, volume_24h,
     market_cap_rank, circulating_supply, total_supply, last_updated)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")
INS_DAY = session.prepare(f"""
  INSERT INTO {DAILY_TABLE}
    (id, date, symbol, name,
     open, high, low, close, price_usd,
     market_cap, volume_24h,
     market_cap_rank, circulating_supply, total_supply,
     candle_source, last_updated)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")
INS_HOURLY = session.prepare(f"""
  INSERT INTO {HOURLY_TABLE}
    (id, ts, symbol, name,
     open, high, low, close, price_usd,
     market_cap, volume_24h,
     market_cap_rank, circulating_supply, total_supply,
     candle_source, last_updated)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

SEL_DAILY_RANGE_FOR_MONTH = session.prepare(f"""
  SELECT date, symbol, name,
         price_usd, market_cap, volume_24h, last_updated,
         open, high, low, close,
         market_cap_rank, circulating_supply, total_supply
  FROM {DAILY_TABLE}
  WHERE id = ? AND date >= ? AND date < ?
""")

SEL_MONTHLY_ONE = session.prepare(f"""
  SELECT open, high, low, close, volume, market_cap, market_cap_rank, candle_source
  FROM {MONTHLY_TABLE}
  WHERE id = ? AND year_month = ? LIMIT 1
""")

INS_MONTHLY = session.prepare(f"""
  INSERT INTO {MONTHLY_TABLE}
    (id, year_month, symbol, name,
     open, high, low, close, volume,
     market_cap, market_cap_rank, circulating_supply, total_supply,
     candle_source, last_updated)
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

# Aggregate inserts
INS_MCAP_10M = session.prepare(f"""
  INSERT INTO {MCAP_10M_TABLE}
    (category, ts, last_updated, market_cap, market_cap_rank, volume_24h)
  VALUES (?, ?, ?, ?, ?, ?)
""")
INS_MCAP_HOURLY = session.prepare(f"""
  INSERT INTO {MCAP_HOURLY_TABLE}
    (category, ts, last_updated, market_cap, market_cap_rank, volume_24h)
  VALUES (?, ?, ?, ?, ?, ?)
""")
INS_MCAP_DAILY = session.prepare(f"""
  INSERT INTO {MCAP_DAILY_TABLE}
    (category, date, last_updated, market_cap, market_cap_rank, volume_24h)
  VALUES (?, ?, ?, ?, ?, ?)
""")

# Optional truncates for FULL mode
TRUNC_10M = SimpleStatement(f"TRUNCATE {MCAP_10M_TABLE}")
TRUNC_H   = SimpleStatement(f"TRUNCATE {MCAP_HOURLY_TABLE}")
TRUNC_D   = SimpleStatement(f"TRUNCATE {MCAP_DAILY_TABLE}")

# ---- Coverage prepared statements ----
PS_RANGES_FOR_ID = session.prepare(f"""
  SELECT start_date, end_date
  FROM {COIN_DAILY_RANGES}
  WHERE id = ?
""")
PS_BITS_RANGE_10M = session.prepare(f"""
  SELECT day, bitmap, set_count
  FROM {COIN_INTRADAY_BITS}
  WHERE id = ? AND granularity = 1 AND day >= ? AND day <= ?
""")
PS_BITS_RANGE_1H = session.prepare(f"""
  SELECT day, bitmap, set_count
  FROM {COIN_INTRADAY_BITS}
  WHERE id = ? AND granularity = 2 AND day >= ? AND day <= ?
""")
