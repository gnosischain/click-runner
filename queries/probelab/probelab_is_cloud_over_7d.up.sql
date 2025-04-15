CREATE TABLE IF NOT EXISTS crawlers_data.probelab_is_cloud_over_7d
(
    `agent_version_type`                Nullable(String),
    `min_crawl_created_at`              Nullable(DateTime64(6)),
    `max_crawl_created_at`              Nullable(DateTime64(6)),
    `crawl_created_at`                  Nullable(DateTime64(6)),
    `is_cloud`                          Nullable(String),
    `__count`                           Nullable(UInt32),
    `__pct`                             Nullable(Float64),
    `__total`                           Nullable(UInt32),
)
ENGINE = ReplacingMergeTree()
ORDER BY (min_crawl_created_at, crawl_created_at, agent_version_type, is_cloud)
SETTINGS allow_nullable_key = 1;