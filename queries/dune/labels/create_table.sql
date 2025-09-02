CREATE TABLE IF NOT EXISTS playground_max.dune_labels (
  address String, label String, introduced_at DateTime, source LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toStartOfMonth(introduced_at)
ORDER BY (address, label, introduced_at);