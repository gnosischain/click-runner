CREATE TABLE IF NOT EXISTS crawlers_data.celo_gpay_transfers
(
    `block_date`     Date,
    `block_time`     DateTime,
    `tx_hash`        String,
    `safe_address`   String,
    `token_symbol`   LowCardinality(String),
    `token_address`  LowCardinality(String),
    `action`         LowCardinality(String),
    `amount`         Float64,
    `amount_usd`     Float64,
    `counterparty`   String
)
ENGINE = ReplacingMergeTree
PARTITION BY toStartOfMonth(block_date)
ORDER BY (safe_address, block_time, tx_hash, token_address, counterparty, action);
