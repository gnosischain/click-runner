CREATE TABLE IF NOT EXISTS crawlers_data.celo_gpay_wallets
(
    `safe_address`    String,
    `owner_address`   String,
    `issued_at`       Date,
    `first_spend_at`  Nullable(Date),
    `is_activated`    UInt8,
    `ingested_at`     DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(ingested_at)
ORDER BY safe_address;
