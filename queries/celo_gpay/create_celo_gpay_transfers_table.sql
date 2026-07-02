-- Raw, unclassified transfer rows (sender/receiver, not safe_address/action/
-- counterparty). Classification lives in dbt-cerebro's
-- int_celo_gpay_activity.sql, mirroring how Gnosis Chain classifies from
-- raw execution.logs rather than pre-classifying upstream.
CREATE TABLE IF NOT EXISTS {{CELO_GPAY_DB}}.celo_gpay_transfers
(
    `block_date`     Date,
    `block_time`     DateTime,
    `tx_hash`        String,
    `sender`         String,
    `receiver`       String,
    `token_symbol`   LowCardinality(String),
    `token_address`  LowCardinality(String),
    `amount`         Float64,
    `amount_usd`     Nullable(Float64)
)
ENGINE = ReplacingMergeTree
PARTITION BY toStartOfMonth(block_date)
ORDER BY (sender, receiver, block_time, tx_hash, token_address);
