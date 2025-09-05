CREATE KEYSPACE default_keyspace WITH replication = {'class': 'NetworkTopologyStrategy', 'us-east-2': '3'}  AND durable_writes = true;

CREATE TABLE default_keyspace.asset_categories (
    id text PRIMARY KEY,
    category text,
    source text,
    symbol text,
    updated_at timestamp
) WITH additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.UnifiedCompactionStrategy'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair = 'BLOCKING'
    AND speculative_retry = '99p';

CREATE TABLE default_keyspace.candles_daily_contin (
    id text,
    date date,
    candle_source text,
    close double,
    high double,
    last_updated timestamp,
    low double,
    market_cap double,
    name text,
    open double,
    price_usd double,
    rank int,
    symbol text,
    volume_24h double,
    PRIMARY KEY (id, date)
) WITH CLUSTERING ORDER BY (date DESC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.UnifiedCompactionStrategy'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair = 'BLOCKING'
    AND speculative_retry = '99p';

CREATE TABLE default_keyspace.candles_hourly_30d (
    id text,
    ts timestamp,
    close double,
    high double,
    low double,
    market_cap double,
    name text,
    open double,
    source text,
    symbol text,
    volume_24h double,
    PRIMARY KEY (id, ts)
) WITH CLUSTERING ORDER BY (ts DESC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.UnifiedCompactionStrategy'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 2592000
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair = 'BLOCKING'
    AND speculative_retry = '99p';

CREATE TABLE default_keyspace.fx_rates_daily (
    provider text,
    base text,
    asof_date date,
    symbol text,
    rate double,
    source text,
    PRIMARY KEY ((provider, base, asof_date), symbol)
) WITH CLUSTERING ORDER BY (symbol ASC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.UnifiedCompactionStrategy'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair = 'BLOCKING'
    AND speculative_retry = '99p';

CREATE TABLE default_keyspace.fx_rates_live (
    provider text,
    base text,
    symbol text,
    asof_date date,
    rate double,
    source text,
    PRIMARY KEY ((provider, base), symbol)
) WITH CLUSTERING ORDER BY (symbol ASC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.UnifiedCompactionStrategy'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair = 'BLOCKING'
    AND speculative_retry = '99p';

CREATE TABLE default_keyspace.fx_status (
    provider text,
    base text,
    last_fetch_ts timestamp,
    latest_date date,
    PRIMARY KEY ((provider, base))
) WITH additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.UnifiedCompactionStrategy'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair = 'BLOCKING'
    AND speculative_retry = '99p';

CREATE TABLE default_keyspace.prices_10m_7d (
    id text,
    ts timestamp,
    last_updated timestamp,
    market_cap double,
    name text,
    price_usd double,
    rank int,
    symbol text,
    volume_24h double,
    PRIMARY KEY (id, ts)
) WITH CLUSTERING ORDER BY (ts DESC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.UnifiedCompactionStrategy'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 604800
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair = 'BLOCKING'
    AND speculative_retry = '99p';

CREATE TABLE default_keyspace.prices_live (
    id text PRIMARY KEY,
    ath_date timestamp,
    ath_price double,
    category text,
    chg_12h double,
    chg_1h double,
    chg_1y double,
    chg_24h double,
    chg_30d double,
    chg_6h double,
    chg_7d double,
    last_fetched timestamp,
    last_updated timestamp,
    market_cap double,
    max_supply double,
    name text,
    price_12h_ago double,
    price_1h_ago double,
    price_1y_ago double,
    price_24h_ago double,
    price_30d_ago double,
    price_6h_ago double,
    price_7d_ago double,
    price_usd double,
    rank int,
    symbol text,
    total_supply double,
    volume_24h double
) WITH additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.UnifiedCompactionStrategy'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair = 'BLOCKING'
    AND speculative_retry = '99p';

CREATE TABLE default_keyspace.prices_live_rolling (
    id text,
    last_updated timestamp,
    ath_date timestamp,
    ath_price double,
    category text,
    chg_12h double,
    chg_1h double,
    chg_1y double,
    chg_24h double,
    chg_30d double,
    chg_6h double,
    chg_7d double,
    last_fetched timestamp,
    market_cap double,
    max_supply double,
    name text,
    price_12h_ago double,
    price_1h_ago double,
    price_1y_ago double,
    price_24h_ago double,
    price_30d_ago double,
    price_6h_ago double,
    price_7d_ago double,
    price_usd double,
    rank int,
    symbol text,
    total_supply double,
    volume_24h double,
    PRIMARY KEY (id, last_updated)
) WITH CLUSTERING ORDER BY (last_updated DESC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.UnifiedCompactionStrategy'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 259200
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair = 'BLOCKING'
    AND speculative_retry = '99p';