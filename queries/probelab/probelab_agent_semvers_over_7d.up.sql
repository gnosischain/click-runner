CREATE TABLE IF NOT EXISTS crawlers_data.probelab_agent_semvers_over_7d
(
    `agent_version_type`                Nullable(String),
    `min_crawl_created_at`              Nullable(DateTime64(6)),
    `max_crawl_created_at`              Nullable(DateTime64(6)),
    `crawl_created_at`                  Nullable(DateTime64(6)),        
    `agent_version_semver`              Array(Nullable(Int64)),
    `agent_version_semver_str`          Nullable(String),
    `__count`                           Nullable(UInt32),
    `__pct`                             Nullable(Float64),
    `__total`                           Nullable(UInt32),
)
ENGINE = MergeTree
ORDER BY (min_crawl_created_at, agent_version_type, crawl_created_at)
SETTINGS allow_nullable_key = 1;