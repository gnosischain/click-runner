CREATE TABLE IF NOT EXISTS crawlers_data.dune_prices (
  block_date Date, symbol LowCardinality(String), price Float64
)
ENGINE = MergeTree
PARTITION BY toStartOfMonth(block_date)
ORDER BY (symbol, block_date);