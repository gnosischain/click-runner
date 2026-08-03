-- Hourly count of online HOPR nodes, from the dashboard's own history
-- (starts 2023-09-02). The API returns the full series every call, so this is
-- populated by `--mode backfill` and does not need a daily run.
CREATE TABLE IF NOT EXISTS {{HOPR_DATABASE}}.hopr_network_online_hourly
(
    network_id    UInt16,
    observed_at   DateTime,
    nodes_online  UInt32,
    ingested_at   DateTime
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toStartOfYear(observed_at)
ORDER BY (network_id, observed_at)
