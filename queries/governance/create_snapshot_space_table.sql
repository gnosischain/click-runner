CREATE TABLE IF NOT EXISTS {{GOVERNANCE_DATABASE}}.snapshot_space (
    space_id         String,
    name             String,
    network          LowCardinality(String),
    symbol           LowCardinality(String),
    proposals_count  UInt32,
    followers_count  UInt32,
    votes_count      UInt64,
    raw_json         String,
    ingested_at      DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY space_id
SETTINGS index_granularity = 8192
