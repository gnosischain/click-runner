CREATE TABLE IF NOT EXISTS crawlers_data.probelab_discv5_stale_records
(
    `crawl_id`                          Nullable(String),
    `crawl_created_at`                  Nullable(DateTime64(6)),
    `total`                             Nullable(Int64),
    `reachability`                      Nullable(String),
    `count`                             Nullable(Int64),
    `percentage`                        Nullable(Float64)
)
ENGINE = ReplacingMergeTree()
ORDER BY (crawl_id, crawl_created_at)
SETTINGS allow_nullable_key = 1;