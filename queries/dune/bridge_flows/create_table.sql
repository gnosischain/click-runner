CREATE TABLE IF NOT EXISTS crawlers_data.dune_bridge_flows (
  `date`         Date,
  `bridge`       LowCardinality(String),
  `source_chain` LowCardinality(String),
  `dest_chain`   LowCardinality(String),
  `token`        LowCardinality(String),
  `amount_token` Float64,
  `amount_usd`   Float64,
  `net_usd`      Float64,
  `txs`          UInt64
)
ENGINE = MergeTree
PARTITION BY toStartOfMonth(date)
ORDER BY (date, bridge, source_chain, dest_chain, token);