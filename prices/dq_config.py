import os
from collections import deque

# ---------- Config ----------
KEYSPACE     = os.getenv("ASTRA_KEYSPACE", "default_keyspace")

TABLE_LIVE      = os.getenv("TABLE_LIVE", "gecko_prices_live")
TEN_MIN_TABLE   = os.getenv("TEN_MIN_TABLE", "gecko_prices_10m_7d")
DAILY_TABLE     = os.getenv("DAILY_TABLE", "gecko_candles_daily_contin")
HOURLY_TABLE    = os.getenv("HOURLY_TABLE", "gecko_candles_hourly_30d")
MONTHLY_TABLE   = os.getenv("MONTHLY_TABLE", "gecko_candles_monthly")

# Coverage tables
COIN_DAILY_RANGES   = os.getenv("COIN_DAILY_RANGES",   "coin_daily_coverage_ranges")
COIN_INTRADAY_BITS  = os.getenv("COIN_INTRADAY_BITS",  "coin_intraday_coverage")

# Aggregate targets
MCAP_10M_TABLE    = os.getenv("MCAP_10M_TABLE",   "gecko_market_cap_10m_7d")
MCAP_HOURLY_TABLE = os.getenv("MCAP_HOURLY_TABLE","gecko_market_cap_hourly_30d")
MCAP_DAILY_TABLE  = os.getenv("MCAP_DAILY_TABLE", "gecko_market_cap_daily_contin")

# Repair windows
DAYS_10M    = int(os.getenv("DQ_WINDOW_10M_DAYS", "7"))
DAYS_DAILY  = int(os.getenv("DQ_WINDOW_DAILY_DAYS", "365"))
DAYS_HOURLY = int(os.getenv("DQ_WINDOW_HOURLY_DAYS", "30"))

# Nightly vs Intraday (convenience)
DQ_MODE = (os.getenv("DQ_MODE", "NIGHTLY").strip().upper())  # "NIGHTLY" | "INTRADAY"

# What to run
FIX_DAILY_FROM_10M      = os.getenv("FIX_DAILY_FROM_10M", "1") == "1"
SEED_10M_FROM_DAILY     = os.getenv("SEED_10M_FROM_DAILY", "0") == "1"
FILL_HOURLY             = os.getenv("FILL_HOURLY", "1") == "1"
FILL_HOURLY_FROM_API    = os.getenv("FILL_HOURLY_FROM_API", "1") == "1"
INTERPOLATE_IF_API_MISS = os.getenv("INTERPOLATE_IF_API_MISS", "1") == "1"
BACKFILL_DAILY_FROM_API = os.getenv("BACKFILL_DAILY_FROM_API", "1") == "1"
FULL_DAILY_ALL          = os.getenv("FULL_DAILY_ALL", "1") == "1"
FILL_HOURLY_BADSCAN     = os.getenv("FILL_HOURLY_BADSCAN", "1") == "1"

# Aggregate recompute toggles
FULL_MODE = os.getenv("FULL_MODE", "1") == "1"
TRUNCATE_AGGREGATES_IN_FULL = os.getenv("TRUNCATE_AGGREGATES_IN_FULL", "1") == "1"
RECOMPUTE_MONTHLY = os.getenv("RECOMPUTE_MONTHLY", "1") == "1"
RECOMPUTE_MCAP_10M    = os.getenv("RECOMPUTE_MCAP_10M", "1") == "1"
RECOMPUTE_MCAP_HOURLY = os.getenv("RECOMPUTE_MCAP_HOURLY", "1") == "1"
RECOMPUTE_MCAP_DAILY  = os.getenv("RECOMPUTE_MCAP_DAILY", "1") == "1"

# Universe selection
TOP_N_DQ              = int(os.getenv("TOP_N_DQ", "210"))
DQ_MAX_COINS          = int(os.getenv("DQ_MAX_COINS", "100000"))
INCLUDE_ALL_DAILY_IDS = os.getenv("INCLUDE_ALL_DAILY_IDS", "1") == "1"
INTRADAY_TOP_N            = int(os.getenv("INTRADAY_TOP_N", str(TOP_N_DQ)))
INCLUDE_UNRANKED_INTRADAY = os.getenv("INCLUDE_UNRANKED_INTRADAY", "0") == "1"
SKIP_UNCOVERED_COINS = os.getenv("SKIP_UNCOVERED_COINS", "1") == "1"

# Performance / logging
REQUEST_TIMEOUT = int(os.getenv("DQ_REQUEST_TIMEOUT_SEC", "30"))
FETCH_SIZE      = int(os.getenv("DQ_FETCH_SIZE", "500"))
RETRIES         = int(os.getenv("DQ_RETRIES", "3"))
BACKOFF_S       = int(os.getenv("DQ_BACKOFF_SEC", "4"))
PAUSE_S         = float(os.getenv("DQ_PAUSE_PER_COIN", "0.04"))
LOG_HEARTBEAT_SEC       = float(os.getenv("DQ_LOG_HEARTBEAT_SEC", "15"))
LOG_EVERY               = int(os.getenv("DQ_LOG_EVERY", "10"))
VERBOSE                 = os.getenv("DQ_VERBOSE", "0") == "1"
TIME_API                = os.getenv("DQ_TIME_API", "1") == "1"
DRY_RUN                 = os.getenv("DQ_DRY_RUN", "0") == "1"
PHASES = set(p.strip() for p in os.getenv(
    "PHASES",
    "coverage,daily_local,daily_api,seed_10m,hourly,aggregates,monthly"
).split(",") if p.strip())

# Soft run budget
SOFT_BUDGET_SEC = int(os.getenv("DQ_SOFT_BUDGET_SEC", str(24*60)))

# CoinGecko API
API_TIER = (os.getenv("COINGECKO_API_TIER") or "pro").strip().lower()
API_KEY  = (os.getenv("COINGECKO_API_KEY") or "").strip()
if API_KEY.lower().startswith("api key:"):
    API_KEY = API_KEY.split(":", 1)[1].strip()
BASE = os.getenv(
    "COINGECKO_BASE_URL",
    "https://api.coingecko.com/api/v3" if API_TIER == "demo" else "https://pro-api.coingecko.com/api/v3"
)

# Daily API backfill request budget
DAILY_API_REQ_BUDGET = int(os.getenv("DAILY_API_REQ_BUDGET", "150"))
PAD_DAYS             = int(os.getenv("DAILY_API_PAD_DAYS", "1"))
MAX_RANGE_DAYS       = int(os.getenv("DAILY_API_MAX_RANGE_DAYS", "90"))

# Interpolation knobs
INTERPOLATE_MCAP_VOL_DAILY  = os.getenv("INTERPOLATE_MCAP_VOL_DAILY", "1") == "1"
INTERPOLATE_MCAP_VOL_HOURLY = os.getenv("INTERPOLATE_MCAP_VOL_HOURLY", "1") == "1"
INTERP_MAX_DAYS  = int(os.getenv("INTERP_MAX_DAYS", "60"))
INTERP_MAX_HOURS = int(os.getenv("INTERP_MAX_HOURS", "168"))

# Rate limiting
CG_REQ_INTERVAL_S = float(os.getenv("CG_REQUEST_INTERVAL_S", "1.0" if API_TIER == "demo" else "0.25"))
CG_MAX_RPM        = int(os.getenv("CG_MAX_RPM", "50" if API_TIER == "demo" else "120"))
REQ_TIMES = deque()

def phase_enabled(name: str) -> bool:
    return name in PHASES
