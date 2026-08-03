-- Daily node quality snapshot from HOPR's network dashboard prober (dufour only;
-- the prober was never ported to jura/v4). node_address is the on-chain address,
-- so this joins directly to decoded HoprChannels source/destination.
-- Availability columns are fractions in [0,1]; availability_1y is NULL for nodes
-- younger than a year.
CREATE TABLE IF NOT EXISTS crawlers_data.hopr_network_nodes
(
    snapshot_date      Date,
    network_id         UInt16,
    node_address       String,
    latency_ms         Nullable(UInt32),
    availability_24h   Nullable(Float64),
    availability_7d    Nullable(Float64),
    availability_30d   Nullable(Float64),
    availability_6m    Nullable(Float64),
    availability_1y    Nullable(Float64),
    first_seen         Nullable(DateTime),
    last_seen          Nullable(DateTime),
    prober_last_run    Nullable(DateTime),
    ingested_at        DateTime
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toStartOfYear(snapshot_date)
ORDER BY (network_id, snapshot_date, node_address)
