CREATE TABLE IF NOT EXISTS {{GOVERNANCE_DATABASE}}.forum_users (
    id              UInt32,
    username        String,
    name            String,
    trust_level     Int8,
    likes_received  UInt32,
    likes_given     UInt32,
    post_count      UInt32,
    topic_count     UInt32,
    days_visited    UInt32,
    raw_json        String,
    ingested_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY id
SETTINGS index_granularity = 8192
