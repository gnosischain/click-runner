CREATE TABLE IF NOT EXISTS {{MIXPANEL_DATABASE}}.mixpanel_raw_profiles (
    distinct_id   String,
    project_id    LowCardinality(String),
    properties    String,
    synced_at     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(synced_at)
ORDER BY (project_id, distinct_id)
SETTINGS index_granularity = 8192
