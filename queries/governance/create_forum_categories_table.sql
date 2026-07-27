CREATE TABLE IF NOT EXISTS {{GOVERNANCE_DATABASE}}.forum_categories (
    id           UInt32,
    parent_id    Int32,
    name         String,
    slug         String,
    topic_count  UInt32,
    post_count   UInt32,
    description  String,
    raw_json     String,
    ingested_at  DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY id
SETTINGS index_granularity = 8192
