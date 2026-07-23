CREATE TABLE IF NOT EXISTS {{GOVERNANCE_DATABASE}}.snapshot_delegations (
    tx_hash      String,
    block_number UInt64,
    log_index    UInt32,
    block_time   DateTime,
    action       LowCardinality(String),
    delegator    String,
    delegate     String,
    ingested_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (tx_hash, log_index)
SETTINGS index_granularity = 8192
