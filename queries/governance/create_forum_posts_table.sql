CREATE TABLE IF NOT EXISTS {{GOVERNANCE_DATABASE}}.forum_posts (
    id                   UInt64,
    topic_id             UInt32,
    post_number          UInt32,
    user_id              Int32,
    username             String,
    created_at           DateTime,
    updated_at           DateTime,
    reply_to_post_number Int32,
    reply_count          UInt32,
    reads                UInt32,
    like_count           UInt32,
    raw                  String,
    cooked               String,
    raw_json             String,
    ingested_at          DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY (topic_id, post_number)
SETTINGS index_granularity = 8192
