CREATE TABLE IF NOT EXISTS {{GOVERNANCE_DATABASE}}.forum_topics (
    id                UInt32,
    title             String,
    slug              String,
    category_id       Int32,
    posts_count       UInt32,
    reply_count       UInt32,
    views             UInt32,
    like_count        UInt32,
    participant_count UInt32,
    tags              String,
    created_at        DateTime,
    last_posted_at    DateTime,
    bumped_at         DateTime,
    closed            UInt8,
    archived          UInt8,
    pinned            UInt8,
    raw_json          String,
    ingested_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY id
SETTINGS index_granularity = 8192
