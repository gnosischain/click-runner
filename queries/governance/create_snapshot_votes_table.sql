CREATE TABLE IF NOT EXISTS {{GOVERNANCE_DATABASE}}.snapshot_votes (
    id           String,
    proposal_id  String,
    space_id     String,
    voter        String,
    created_at   DateTime,
    vp           Float64,
    vp_state     LowCardinality(String),
    raw_json     String,
    ingested_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (proposal_id, id)
SETTINGS index_granularity = 8192
