-- Per-node identity snapshot from Blokli. `multiaddress` is the node's announced
-- transport address and carries a raw IP, which is the input for geo/ASN
-- enrichment (see crawlers_data.ipinfo). chain_key joins to the source/destination
-- addresses in the decoded HoprChannels events.
CREATE TABLE IF NOT EXISTS crawlers_data.hopr_blokli_nodes
(
    snapshot_date       Date,
    network             LowCardinality(String),
    keyid               UInt32,
    chain_key           String,
    packet_key          String,
    safe_address        String,
    multiaddress        String,
    multiaddress_count  UInt8,
    ingested_at         DateTime
)
ENGINE = ReplacingMergeTree(ingested_at)
PARTITION BY toStartOfYear(snapshot_date)
ORDER BY (network, snapshot_date, keyid)
