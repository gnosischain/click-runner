CREATE TABLE IF NOT EXISTS {{MIXPANEL_DATABASE}}.mixpanel_raw_events (
    event_name    LowCardinality(String),
    distinct_id   String,
    event_time    DateTime,
    insert_id     String,
    project_id    LowCardinality(String),
    properties    String
)
ENGINE = ReplacingMergeTree()
PARTITION BY toStartOfMonth(event_time)
ORDER BY (project_id, event_name, event_time, insert_id)
SETTINGS index_granularity = 8192
