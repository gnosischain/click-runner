CREATE TABLE IF NOT EXISTS {{MIXPANEL_DATABASE}}.mixpanel_ingestion_state (
    project_id      LowCardinality(String),
    completed_date  Date,
    rows_indexed    UInt64 DEFAULT 0,
    completed_at    DateTime DEFAULT now(),
    status          LowCardinality(String) DEFAULT 'complete'
)
ENGINE = ReplacingMergeTree(completed_at)
ORDER BY (project_id, completed_date)
