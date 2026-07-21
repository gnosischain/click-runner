CREATE TABLE IF NOT EXISTS {{GOVERNANCE_DATABASE}}.snapshot_proposals (
    id             String,
    space_id       String,
    title          String,
    state          LowCardinality(String),
    type           LowCardinality(String),
    author         String,
    created_at     DateTime,
    start_at       DateTime,
    end_at         DateTime,
    snapshot_block UInt64,
    scores_total   Float64,
    quorum         Float64,
    votes_count    UInt32,
    scores_state   LowCardinality(String),
    raw_json       String,
    ingested_at    DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY id
SETTINGS index_granularity = 8192
