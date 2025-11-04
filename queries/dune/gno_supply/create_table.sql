CREATE TABLE IF NOT EXISTS crawlers_data.dune_gno_supply (
  label LowCardinality(String),
  block_date Date,
  supply Float64
)
ENGINE = MergeTree
ORDER BY (label, block_date);