CREATE TABLE IF NOT EXISTS crawlers_data.probelab_quic_support_over_7d
(
    `agent_version_type`                Nullable(String),
    `min_crawl_created_at`              Nullable(DateTime64(6)),
    `max_crawl_created_at`              Nullable(DateTime64(6)),
    `crawl_created_at`                  Nullable(DateTime64(6)),
    `quic_support`                        Nullable(String),
    `__count`                           Nullable(UInt32),
    `__pct`                             Nullable(Float64),
    `__total`                           Nullable(UInt32),
)
ENGINE = ReplacingMergeTree()
ORDER BY (min_crawl_created_at, agent_version_type, quic_support)
SETTINGS allow_nullable_key = 1;