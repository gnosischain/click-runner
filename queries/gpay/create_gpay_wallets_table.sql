CREATE TABLE IF NOT EXISTS crawlers_data.gpay_wallets
(
    `SAFE_address`        String,
    `SAFE_createdAt`      DateTime
)
ENGINE = MergeTree
ORDER BY (SAFE_address, SAFE_createdAt);