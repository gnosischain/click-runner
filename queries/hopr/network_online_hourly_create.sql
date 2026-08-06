-- Hourly count of online HOPR nodes, from the dashboard's own history
-- (starts 2023-09-02). The API returns the full series every call, so this needs no
-- incremental logic -- but it DOES need re-running: new hours accrue continuously, so
-- a single backfill leaves the series frozen from that day on. The ingestor therefore
-- fetches it on every run regardless of --mode, and the ReplacingMergeTree collapses
-- the repeats. That also makes the table self-healing, which matters because the
-- upstream history is not gap-free.
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
