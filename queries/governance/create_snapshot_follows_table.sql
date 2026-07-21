CREATE TABLE IF NOT EXISTS {{GOVERNANCE_DATABASE}}.snapshot_follows (
    id           String,
    follower     String,
    space_id     String,
    created_at   DateTime,
    raw_json     String,
    ingested_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY id
SETTINGS index_granularity = 8192
