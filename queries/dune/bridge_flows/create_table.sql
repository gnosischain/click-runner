CREATE TABLE IF NOT EXISTS crawlers_data.dune_bridge_flows (
  `timestamp`    DateTime,
  `bridge`       LowCardinality(String),
  `source_chain` LowCardinality(String),
  `dest_chain`   LowCardinality(String),
  `token`        LowCardinality(String),
  `amount_token` Float64,
  `amount_usd`   Float64,
  `net_usd`      Float64
)
ENGINE = MergeTree
PARTITION BY toStartOfMonth(timestamp)
ORDER BY (bridge, source_chain, dest_chain, token, timestamp);