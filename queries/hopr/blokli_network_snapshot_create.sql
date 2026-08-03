-- Daily HOPR network snapshot from Blokli (HOPR's own v4 indexer).
-- ReplacingMergeTree(ingested_at): re-running a day supersedes that day's row.
CREATE TABLE IF NOT EXISTS crawlers_data.hopr_blokli_network_snapshot
(
    snapshot_date                          Date,
    network                                LowCardinality(String),
    chain_id                               UInt32,
    block_number                           UInt64,
    api_version                            String,
    ticket_price_wxhopr                    Nullable(Float64),
    min_ticket_winning_probability         Float64,
    key_binding_fee_wxhopr                 Nullable(Float64),
    channel_closure_grace_period_s         UInt32,
    account_count                          UInt32,
    safes_count                            UInt32,
    safes_balance_wxhopr                   Nullable(Float64),
    channels_total                          UInt32,
    channels_balance_wxhopr                Nullable(Float64),
    channels_open                           UInt32,
    channels_open_balance_wxhopr            Nullable(Float64),
    channels_pendingtoclose                 UInt32,
    channels_pendingtoclose_balance_wxhopr  Nullable(Float64),
    channels_closed                         UInt32,
    channels_closed_balance_wxhopr          Nullable(Float64),
    ingested_at                            DateTime
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toStartOfYear(snapshot_date)
ORDER BY (network, snapshot_date)
