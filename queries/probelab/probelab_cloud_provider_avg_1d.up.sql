CREATE TABLE IF NOT EXISTS crawlers_data.probelab_cloud_provider_avg_1d
(
    `agent_version_type`                Nullable(String),
    `min_crawl_created_at`              Nullable(DateTime64(6)),
    `max_crawl_created_at`              Nullable(DateTime64(6)),
    `cloud_provider`                    Nullable(String),
    `__count`                           Nullable(Float64),
    `__samples`                         Nullable(UInt32),
    `__pct`                             Nullable(Float64),
    `__total`                           Nullable(Float64),
)
ENGINE = MergeTree
ORDER BY (min_crawl_created_at, agent_version_type, cloud_provider)
SETTINGS allow_nullable_key = 1;