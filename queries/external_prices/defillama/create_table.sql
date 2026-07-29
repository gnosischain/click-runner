CREATE TABLE IF NOT EXISTS {{EXTERNAL_PRICES_DATABASE}}.defillama_prices (
  block_date Date,
  chain LowCardinality(String),
  token_address String,
  symbol LowCardinality(String),
  price Float64,
  confidence Nullable(Float64),
  ingested_at DateTime
)
ENGINE = MergeTree
PARTITION BY toStartOfMonth(block_date)
ORDER BY (symbol, block_date, token_address);
