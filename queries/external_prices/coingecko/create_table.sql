CREATE TABLE IF NOT EXISTS {{EXTERNAL_PRICES_DATABASE}}.coingecko_prices (
  block_date Date,
  coingecko_id LowCardinality(String),
  symbol LowCardinality(String),
  price Float64,
  ingested_at DateTime
)
ENGINE = MergeTree
PARTITION BY toStartOfMonth(block_date)
ORDER BY (symbol, block_date, coingecko_id);
